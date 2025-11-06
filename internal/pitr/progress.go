package pitr

import (
	"io"
	"os"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

// progress wraps mpb to handle safe progress updates
type progress struct {
	*mpb.Progress
	bar    *mpb.Bar
	output io.Writer
}

// newProgress creates a new progress wrapper
func newProgress(total int64) *progress {
	// Create a container that writes to stdout
	// This allows us to redirect logger output through mpb
	p := mpb.New(
		mpb.WithWidth(60),
		mpb.WithOutput(os.Stdout),
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

	return &progress{Progress: p, bar: bar, output: p}
}

// increment safely increments the progress bar (mpb is already thread-safe)
func (p *progress) increment() {
	p.bar.Increment()
}

// Output returns the writer that logs should use to stay above the progress bar
func (p *progress) Output() io.Writer {
	return p.output
}
