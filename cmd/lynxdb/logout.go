package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/pkg/auth"
)

var flagLogoutAll bool

var logoutCmd = &cobra.Command{
	Use:   "logout",
	Short: "Remove saved credentials for a server",
	Example: `  lynxdb logout
  lynxdb logout --server https://lynxdb.company.com
  lynxdb logout --all`,
	RunE: runLogout,
}

func init() {
	logoutCmd.Flags().BoolVar(&flagLogoutAll, "all", false, "Remove ALL saved credentials")
	rootCmd.AddCommand(logoutCmd)
}

func runLogout(_ *cobra.Command, _ []string) error {
	if flagLogoutAll {
		count, err := auth.RemoveAll()
		if err != nil {
			return fmt.Errorf("remove credentials: %w", err)
		}

		if count == 0 {
			printMeta("No saved credentials to remove.")

			return nil
		}

		printSuccess("Removed all saved credentials (%d servers)", count)

		return nil
	}

	removed, err := auth.RemoveToken(globalServer)
	if err != nil {
		return fmt.Errorf("remove credentials: %w", err)
	}

	if !removed {
		printMeta("No saved credentials for %s", globalServer)

		return nil
	}

	printSuccess("Removed credentials for %s", globalServer)

	return nil
}
