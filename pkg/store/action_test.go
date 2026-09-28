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
			// Same key on a different node is a distinct action.
			got3, committed, err := st.CommitAction(ctx, &model.ActionRecord{
				RunID: "act1", NodeID: "other", Key: "k1", CommittedAt: time.Now()})
			if err != nil || !committed || got3.Seq != 2 {
				t.Fatalf("other-node commit: %+v committed=%v err=%v", got3, committed, err)
			}
			log, err := st.LoadActions(ctx, "act1")
			if err != nil {
				t.Fatal(err)
			}
			if len(log.Actions) != 2 {
				t.Fatalf("records=%d want 2", len(log.Actions))
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
