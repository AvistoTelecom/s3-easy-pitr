package pitr

import (
	"sync"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

// progress wraps mpb to handle safe progress updates
type progress struct {
	*mpb.Progress
	bar *mpb.Bar
	mu  sync.Mutex
}

// newProgress creates a new progress wrapper
func newProgress(total int64) *progress {
	// Use a single shared progress instance
	p := mpb.New(
		mpb.WithWidth(60),
		// Higher refresh rate but with mutex protection
		mpb.WithAutoRefresh(),
	)

	bar := p.AddBar(total,
		mpb.PrependDecorators(
			decor.Name("Restoring: "),
			decor.CountersNoUnit("%d/%d"),
		),
		mpb.AppendDecorators(
			decor.Percentage(),
		),
	)

	return &progress{Progress: p, bar: bar}
}

// increment safely increments the progress bar
func (p *progress) increment() {
	p.mu.Lock()
	p.bar.Increment()
	p.mu.Unlock()
}
