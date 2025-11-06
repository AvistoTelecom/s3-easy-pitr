package internal

import (
	"io"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// createLoggerWithOutput creates a zap logger that writes to the given writer
func CreateLoggerWithOutput(output io.Writer, level zapcore.Level) (*zap.Logger, error) {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	encoderConfig.EncodeDuration = zapcore.StringDurationEncoder
	encoderConfig.StacktraceKey = ""
	encoderConfig.ConsoleSeparator = " "

	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderConfig),
		zapcore.AddSync(output),
		level,
	)

	return zap.New(core), nil
}
