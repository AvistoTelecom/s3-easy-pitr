package main

import (
	"github.com/AvistoTelecom/s3-easy-pitr/cmd"
	"go.uber.org/zap"
)

func main() {
	cmd.Execute()
	// flush any buffered logs from the root logger
	_ = zap.L().Sync()
}
