package sink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/cursor"
	"github.com/co-codin/USDT-Tracker/internal/model"
)

// Recorder receives delivery telemetry (implemented by the metrics package).
type Recorder interface {
	ObserveSink(sink, status string, n int, d time.Duration)
}

// Entry is a sink registered with the dispatcher.
type Entry struct {
	Sink Sink
	// BestEffort sinks never block progress: failures are logged and counted
	// but the cursor still advances. Required sinks (the default) cause the
	// block to be retried until they succeed, so nothing is lost.
	BestEffort bool
}

type entry struct {
	Entry
	delivered *cursor.Dedupe
}

// Dispatcher fans batches out to sinks concurrently and remembers, per sink,
// which transfers were already delivered (dedupe by tx id + log index). When a
// block is retried because one sink failed, sinks that already succeeded are
// not called again for the same transfers.
type Dispatcher struct {
	entries []*entry
	rec     Recorder
	log     *slog.Logger
}

// NewDispatcher creates a dispatcher. dedupeSize bounds the per-sink memory.
func NewDispatcher(entries []Entry, dedupeSize int, rec Recorder, log *slog.Logger) *Dispatcher {
	d := &Dispatcher{rec: rec, log: log}
	if d.log == nil {
		d.log = slog.New(slog.DiscardHandler)
	}
	for _, e := range entries {
		d.entries = append(d.entries, &entry{Entry: e, delivered: cursor.NewDedupe(dedupeSize)})
	}
	return d
}

// Names returns the registered sink names.
func (d *Dispatcher) Names() []string {
	out := make([]string, 0, len(d.entries))
	for _, e := range d.entries {
		out = append(out, e.Sink.Name())
	}
	return out
}

// Dispatch delivers b to every sink. It returns an error if any required sink
// failed; the caller should retry the same batch later.
func (d *Dispatcher) Dispatch(ctx context.Context, b Batch) error {
	if len(b.Transfers) == 0 {
		return nil
	}
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for _, e := range d.entries {
		pending := make([]model.Transfer, 0, len(b.Transfers))
		for _, t := range b.Transfers {
			if !e.delivered.Seen(t.Key()) {
				pending = append(pending, t)
			}
		}
		if len(pending) == 0 {
			continue
		}
		wg.Add(1)
		go func(e *entry, pending []model.Transfer) {
			defer wg.Done()
			start := time.Now()
			err := e.Sink.Send(ctx, Batch{BlockNumber: b.BlockNumber, BlockTime: b.BlockTime, Transfers: pending})
			status := "ok"
			switch {
			case err == nil:
				for _, t := range pending {
					e.delivered.Add(t.Key())
				}
			case e.BestEffort:
				status = "dropped"
				d.log.Error("best-effort sink failed, skipping", "sink", e.Sink.Name(), "block", b.BlockNumber, "transfers", len(pending), "err", err)
				for _, t := range pending { // do not retry best-effort deliveries
					e.delivered.Add(t.Key())
				}
			default:
				status = "error"
				mu.Lock()
				errs = append(errs, fmt.Errorf("sink %s: %w", e.Sink.Name(), err))
				mu.Unlock()
			}
			if d.rec != nil {
				d.rec.ObserveSink(e.Sink.Name(), status, len(pending), time.Since(start))
			}
		}(e, pending)
	}
	wg.Wait()
	return errors.Join(errs...)
}

// Close closes all sinks.
func (d *Dispatcher) Close() error {
	var errs []error
	for _, e := range d.entries {
		if err := e.Sink.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close %s: %w", e.Sink.Name(), err))
		}
	}
	return errors.Join(errs...)
}
