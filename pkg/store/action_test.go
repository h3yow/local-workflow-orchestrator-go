package store

import (
	"context"
	"sync"
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
			// Same key reused by a DIFFERENT node in the same run is still the
			// same business action: the run-scoped key, not the node, is the
			// dedup domain, so the first winner is returned unchanged.
			got3, committed, err := st.CommitAction(ctx, &model.ActionRecord{
				RunID: "act1", NodeID: "other", Key: "k1",
				Value: "v-other-node", Attempt: 1, CommittedAt: time.Now()})
			if err != nil || committed {
				t.Fatalf("other-node same-key commit: committed=%v err=%v", committed, err)
			}
			if got3.Seq != 1 || got3.NodeID != "n" || got3.Value != "v1" || got3.Attempt != 0 {
				t.Fatalf("other-node same-key changed winner: %+v", got3)
			}
			// A genuinely different key (even from that other node) is new and
			// continues the run-scoped sequence.
			got4, committed, err := st.CommitAction(ctx, &model.ActionRecord{
				RunID: "act1", NodeID: "other", Key: "k2", CommittedAt: time.Now()})
			if err != nil || !committed || got4.Seq != 2 {
				t.Fatalf("other-node new-key commit: %+v committed=%v err=%v", got4, committed, err)
			}
			log, err := st.LoadActions(ctx, "act1")
			if err != nil {
				t.Fatal(err)
			}
			if len(log.Actions) != 2 {
				t.Fatalf("records=%d want 2", len(log.Actions))
			}
			if log.Actions[0].Key != "k1" || log.Actions[0].NodeID != "n" ||
				log.Actions[1].Key != "k2" || log.Actions[1].NodeID != "other" {
				t.Fatalf("unexpected surviving records: %+v", log.Actions)
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

			// File store: reopen the directory and dedup must still hold.
			if fs, ok := st.(*FileStore); ok {
				reopened, err := NewFileStore(fs.dir)
				if err != nil {
					t.Fatal(err)
				}
				got4, committed, err := reopened.CommitAction(ctx, dup)
				if err != nil || committed || got4.Value != "v1" {
					t.Fatalf("post-restart dedup: %+v committed=%v err=%v", got4, committed, err)
				}
				log3, _ := reopened.LoadActions(ctx, "act1")
				if len(log3.Actions) != 2 {
					t.Fatalf("post-restart records=%d want 2", len(log3.Actions))
				}
			}
		})
	}
}

// TestActionDedupAcrossNodesSameRun reproduces the cross-node reuse bug:
// within ONE run the business key alone is the dedup domain, so two different
// nodes registering the same key must yield exactly one record; the second
// registrant gets the first winner's record and committed=false and performs
// no side effect of its own.
func TestActionDedupAcrossNodesSameRun(t *testing.T) {
	for name, st := range storeCases(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			if err := st.SaveStatus(ctx, mkStatus("cross", 1)); err != nil {
				t.Fatal(err)
			}
			first, committed, err := st.CommitAction(ctx, &model.ActionRecord{
				RunID: "cross", NodeID: "node-a", Key: "order-7",
				Value: "from-a", Attempt: 0, CommittedAt: time.Now()})
			if err != nil || !committed || first.Seq != 1 {
				t.Fatalf("first commit: rec=%+v committed=%v err=%v", first, committed, err)
			}
			// A different node reuses the same business key in the same run.
			second, committed, err := st.CommitAction(ctx, &model.ActionRecord{
				RunID: "cross", NodeID: "node-b", Key: "order-7",
				Value: "from-b", Attempt: 1, CommittedAt: time.Now()})
			if err != nil || committed {
				t.Fatalf("cross-node dup: committed=%v err=%v", committed, err)
			}
			if second.Seq != 1 || second.NodeID != "node-a" || second.Value != "from-a" || second.Attempt != 0 {
				t.Fatalf("loser did not receive the winning record: %+v", second)
			}
			log, err := st.LoadActions(ctx, "cross")
			if err != nil {
				t.Fatal(err)
			}
			if len(log.Actions) != 1 || log.Actions[0].Key != "order-7" || log.Actions[0].NodeID != "node-a" {
				t.Fatalf("records=%+v, want exactly the single winner", log.Actions)
			}

			// File store: reopen (simulated restart) and a third node replaying
			// the same key after recovery must still see the one winner.
			if fs, ok := st.(*FileStore); ok {
				reopened, err := NewFileStore(fs.dir)
				if err != nil {
					t.Fatal(err)
				}
				third, committed, err := reopened.CommitAction(ctx, &model.ActionRecord{
					RunID: "cross", NodeID: "node-c", Key: "order-7",
					Value: "from-c", CommittedAt: time.Now()})
				if err != nil || committed || third.Seq != 1 || third.NodeID != "node-a" {
					t.Fatalf("post-restart cross-node dup: rec=%+v committed=%v err=%v", third, committed, err)
				}
				log2, _ := reopened.LoadActions(ctx, "cross")
				if len(log2.Actions) != 1 {
					t.Fatalf("post-restart records=%d want 1", len(log2.Actions))
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
