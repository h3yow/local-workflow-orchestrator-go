package store

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/local-workflow-orchestrator-go/pkg/model"
)

func TestActionCommitDedupAndReplay(t *testing.T) {
	for name, st := range storeCases(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			if err := st.SaveStatus(ctx, mkStatus("act1", 1)); err != nil {
				t.Fatal(err)
			}
			rec := &model.ActionRecord{RunID: "act1", NodeID: "n", Key: "k1",
				Value: "v1", Attempt: 0, CommittedAt: time.Now()}
			got, committed, err := st.CommitAction(ctx, rec)
			if err != nil || !committed || got.Seq != 1 {
				t.Fatalf("first commit: %+v committed=%v err=%v", got, committed, err)
			}
			// Same (node, key) again: first record wins unchanged.
			dup := &model.ActionRecord{RunID: "act1", NodeID: "n", Key: "k1",
				Value: "v2-loser", Attempt: 2, CommittedAt: time.Now()}
			got2, committed, err := st.CommitAction(ctx, dup)
			if err != nil || committed {
				t.Fatalf("dup commit: committed=%v err=%v", committed, err)
			}
			if got2.Value != "v1" || got2.Seq != 1 || got2.Attempt != 0 {
				t.Fatalf("dup changed winner: %+v", got2)
			}
			// Same key from a DIFFERENT node in the same run is the same
			// business action: it must not take effect a second time and must
			// return the first node's winning record.
			got3, committed, err := st.CommitAction(ctx, &model.ActionRecord{
				RunID: "act1", NodeID: "other", Key: "k1", Value: "v3-loser",
				Attempt: 1, CommittedAt: time.Now()})
			if err != nil || committed {
				t.Fatalf("other-node same-key commit: %+v committed=%v err=%v", got3, committed, err)
			}
			if got3.NodeID != "n" || got3.Value != "v1" || got3.Seq != 1 || got3.Attempt != 0 {
				t.Fatalf("other-node commit did not return winner: %+v", got3)
			}
			log, err := st.LoadActions(ctx, "act1")
			if err != nil {
				t.Fatal(err)
			}
			if len(log.Actions) != 1 {
				t.Fatalf("records=%d want 1", len(log.Actions))
			}
			// Deterministic order across reads.
			log2, _ := st.LoadActions(ctx, "act1")
			for i := range log.Actions {
				if log.Actions[i] != log2.Actions[i] {
					t.Fatal("action replay order/content unstable")
				}
			}
			if _, err := st.LoadActions(ctx, "missing"); err != model.ErrNotFound {
				t.Fatalf("unknown run err=%v want ErrNotFound", err)
			}

			// File store: reopen the directory and dedup must still hold,
			// both for the same node and for a different node.
			if fs, ok := st.(*FileStore); ok {
				reopened, err := NewFileStore(fs.dir)
				if err != nil {
					t.Fatal(err)
				}
				got4, committed, err := reopened.CommitAction(ctx, dup)
				if err != nil || committed || got4.Value != "v1" {
					t.Fatalf("post-restart dedup: %+v committed=%v err=%v", got4, committed, err)
				}
				got5, committed, err := reopened.CommitAction(ctx, &model.ActionRecord{
					RunID: "act1", NodeID: "replayed-node", Key: "k1",
					Value: "v5-loser", CommittedAt: time.Now()})
				if err != nil || committed || got5.Value != "v1" || got5.NodeID != "n" {
					t.Fatalf("post-restart cross-node dedup: %+v committed=%v err=%v", got5, committed, err)
				}
				log3, _ := reopened.LoadActions(ctx, "act1")
				if len(log3.Actions) != 1 {
					t.Fatalf("post-restart records=%d want 1", len(log3.Actions))
				}
			}
		})
	}
}

func TestActionConcurrentCommitOneWinner(t *testing.T) {
	for name, st := range storeCases(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			if err := st.SaveStatus(ctx, mkStatus("actc", 1)); err != nil {
				t.Fatal(err)
			}
			const g = 50
			var wg sync.WaitGroup
			wg.Add(g)
			start := make(chan struct{})
			var winners int32
			var mu sync.Mutex
			seqs := map[int64]bool{}
			for i := 0; i < g; i++ {
				go func() {
					defer wg.Done()
					<-start
					got, committed, err := st.CommitAction(ctx, &model.ActionRecord{
						RunID: "actc", NodeID: "n", Key: "race-key", CommittedAt: time.Now()})
					if err != nil {
						t.Error(err)
						return
					}
					mu.Lock()
					seqs[got.Seq] = true
					mu.Unlock()
					if committed {
						winners++
					}
				}()
			}
			close(start)
			wg.Wait()
			if winners != 1 {
				t.Fatalf("winners=%d want 1", winners)
			}
			if len(seqs) != 1 {
				t.Fatalf("observed seqs=%v want single winner seq", seqs)
			}
			log, _ := st.LoadActions(ctx, "actc")
			if len(log.Actions) != 1 {
				t.Fatalf("records=%d want 1", len(log.Actions))
			}
		})
	}
}

// TestActionConcurrentSameKeyDifferentNodes: goroutines acting on behalf of
// DIFFERENT nodes race to commit one business key in one run. Exactly one
// record survives, exactly one committer reports committed=true, and every
// loser receives the same winning record (same seq, same originating node).
func TestActionConcurrentSameKeyDifferentNodes(t *testing.T) {
	for name, st := range storeCases(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			if err := st.SaveStatus(ctx, mkStatus("actn", 1)); err != nil {
				t.Fatal(err)
			}
			const g = 40
			var wg sync.WaitGroup
			wg.Add(g)
			start := make(chan struct{})
			var winners int32
			var winnerNode string
			var winnerSeq int64
			var mu sync.Mutex
			for i := 0; i < g; i++ {
				nodeID := fmt.Sprintf("node-%02d", i)
				go func() {
					defer wg.Done()
					<-start
					got, committed, err := st.CommitAction(ctx, &model.ActionRecord{
						RunID: "actn", NodeID: nodeID, Key: "shared-biz-key",
						Attempt: 0, CommittedAt: time.Now()})
					if err != nil {
						t.Error(err)
						return
					}
					mu.Lock()
					defer mu.Unlock()
					if committed {
						atomic.AddInt32(&winners, 1)
						winnerNode = got.NodeID
						winnerSeq = got.Seq
					} else if winnerSeq != 0 && got.Seq != winnerSeq {
						t.Errorf("loser saw seq %d, want winner seq %d", got.Seq, winnerSeq)
					}
				}()
			}
			close(start)
			wg.Wait()
			if winners != 1 {
				t.Fatalf("winners=%d want 1", winners)
			}
			log, _ := st.LoadActions(ctx, "actn")
			if len(log.Actions) != 1 {
				t.Fatalf("records=%d want 1", len(log.Actions))
			}
			r := log.Actions[0]
			if r.Key != "shared-biz-key" || r.NodeID != winnerNode || r.Seq != winnerSeq {
				t.Fatalf("surviving record=%+v winnerNode=%q winnerSeq=%d", r, winnerNode, winnerSeq)
			}
			// A different business key from the runner-up node still commits.
			other, committed, err := st.CommitAction(ctx, &model.ActionRecord{
				RunID: "actn", NodeID: "node-01", Key: "other-key", CommittedAt: time.Now()})
			if err != nil || !committed || other.Seq != 2 {
				t.Fatalf("distinct key commit: %+v committed=%v err=%v", other, committed, err)
			}
		})
	}
}

func TestActionIsolatedPerRun(t *testing.T) {
	for name, st := range storeCases(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			_ = st.SaveStatus(ctx, mkStatus("r1", 1))
			_ = st.SaveStatus(ctx, mkStatus("r2", 1))
			for _, rid := range []string{"r1", "r2"} {
				if _, committed, err := st.CommitAction(ctx, &model.ActionRecord{
					RunID: rid, NodeID: "n", Key: "k", CommittedAt: time.Now()}); err != nil || !committed {
					t.Fatalf("run %s first commit committed=%v err=%v", rid, committed, err)
				}
			}
			l1, _ := st.LoadActions(ctx, "r1")
			l2, _ := st.LoadActions(ctx, "r2")
			if len(l1.Actions) != 1 || l1.Actions[0].Seq != 1 ||
				len(l2.Actions) != 1 || l2.Actions[0].Seq != 1 {
				t.Fatalf("run action seqs leaked: %+v %+v", l1.Actions, l2.Actions)
			}
		})
	}
}
