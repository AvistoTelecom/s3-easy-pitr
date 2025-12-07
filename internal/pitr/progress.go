package pitr

import (
	"io"

	progresspkg "github.com/AvistoTelecom/s3-easy-pitr/internal/progress"
)

// progress wraps the shared progress.Bar for PITR-specific operations
type progress struct {
	*progresspkg.Bar
}

// newProgress creates a new progress wrapper for restore operations
func newProgress(total int64) *progress {
	bar, err := progresspkg.New(progresspkg.Options{
		Total:         total,
		OperationName: "Restoring",
	})
	if err != nil {
		// This shouldn't happen in practice, but handle it gracefully
		panic(err)
	}

	return &progress{Bar: bar}
}

// increment safely increments the progress bar (mpb is already thread-safe)
func (p *progress) increment() {
	p.Increment()
}

// Output returns the writer that logs should use to stay above the progress bar
func (p *progress) Output() io.Writer {
	return p.Bar.Output()
}
