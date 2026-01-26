package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	version = "dev"
)

// versionCmd represents the version command
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show the version of s3-easy-pitr",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("s3-easy-pitr %s\n", version)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
