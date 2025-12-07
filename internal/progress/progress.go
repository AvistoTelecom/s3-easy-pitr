package progress

import (
	"fmt"
	"io"
	"os"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"go.uber.org/zap"

	"github.com/AvistoTelecom/s3-easy-pitr/internal"
)

// Bar wraps mpb to provide safe progress updates with integrated logging
type Bar struct {
	*mpb.Progress
	bar    *mpb.Bar
	output io.Writer
	logger *zap.SugaredLogger
}

// Options holds configuration for creating a progress bar
type Options struct {
	Total         int64
	OperationName string // e.g., "Restoring", "Deleting", "Uploading"
}

// New creates a new progress bar with integrated logging
func New(opts Options) (*Bar, error) {
	// Create a container that writes to stdout
	p := mpb.New(
		mpb.WithWidth(60),
		mpb.WithOutput(os.Stdout),
		mpb.WithAutoRefresh(),
	)

	bar := p.AddBar(opts.Total,
		mpb.PrependDecorators(
			decor.Name(fmt.Sprintf("%s: ", opts.OperationName)),
			decor.CountersNoUnit("%d/%d"),
		),
		mpb.AppendDecorators(
			decor.Percentage(),
			decor.Name(" "),
			decor.Elapsed(decor.ET_STYLE_GO),
			decor.Name(" ETA: "),
			decor.AverageETA(decor.ET_STYLE_GO),
		),
	)

	// Create a logger that writes through mpb to avoid cropping
	logger, err := internal.CreateLoggerWithOutput(p, zap.L().Level())
	if err != nil {
		return nil, fmt.Errorf("create progress-aware logger: %w", err)
	}

	return &Bar{
		Progress: p,
		bar:      bar,
		output:   p,
		logger:   logger.Sugar(),
	}, nil
}

// Increment safely increments the progress bar (mpb is thread-safe)
func (p *Bar) Increment() {
	p.bar.Increment()
}

// Output returns the writer that logs should use to stay above the progress bar
func (p *Bar) Output() io.Writer {
	return p.output
}

// Logger returns a logger that writes output above the progress bar
// Use this logger instead of the original logger to prevent output cropping
func (p *Bar) Logger() *zap.SugaredLogger {
	return p.logger
}
