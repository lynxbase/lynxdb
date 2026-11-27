package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/output"
	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/client"
)

func init() {
	rootCmd.AddCommand(newPartitionsCmd())
}

func newPartitionsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "partitions",
		Short: "List, drop, detach or attach index partitions",
	}
	timeout := cmd.PersistentFlags().Duration("timeout", 0, "Maximum time to wait for the server (0 = no timeout)")
	partitionClient := func() *client.Client { return newAPIClient(client.WithTimeout(*timeout)) }
	list := &cobra.Command{
		Use:     "list <index>",
		Short:   "Show partition boundaries, events, bytes and parts",
		Example: "  lynxdb partitions list web",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			partitions, err := partitionClient().Partitions(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			return printPartitions(partitions)
		},
	}
	var yes bool
	drop := &cobra.Command{
		Use:     "drop <index> <start>",
		Short:   "Permanently remove one partition",
		Example: "  lynxdb partitions drop web 2026-10-01",
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !yes && isStdinTTY() && !confirmAction(fmt.Sprintf("Permanently drop partition %s of index %s?", args[1], args[0])) {
				fmt.Fprintln(os.Stderr, "  Aborted.")
				return nil
			}
			return printPartitionResult(partitionClient().DropPartition(cmd.Context(), args[0], args[1]))
		},
	}
	drop.Flags().BoolVarP(&yes, "yes", "y", false, "Skip the confirmation prompt")

	var to string
	detach := &cobra.Command{
		Use:     "detach <index> <start> --to <directory>",
		Short:   "Archive one partition and remove it from the index",
		Example: "  lynxdb partitions detach web 2026-10-01 --to /mnt/archive/web-20261001",
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return printPartitionResult(partitionClient().DetachPartition(cmd.Context(), args[0], args[1], to))
		},
	}
	detach.Flags().StringVar(&to, "to", "", "New directory on the server for the detached partition")
	_ = detach.MarkFlagRequired("to")

	attach := &cobra.Command{
		Use:     "attach <index> <directory>",
		Short:   "Import a detached partition from a directory on the server",
		Example: "  lynxdb partitions attach web /mnt/archive/web-20261001",
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return printPartitionResult(partitionClient().AttachPartition(cmd.Context(), args[0], args[1]))
		},
	}
	cmd.AddCommand(list, drop, detach, attach)
	return cmd
}

func printPartitionResult(p *client.PartitionInfo, err error) error {
	if err != nil {
		return err
	}
	return printPartitions([]client.PartitionInfo{*p})
}

func printPartitions(partitions []client.PartitionInfo) error {
	cols := []output.Column{
		{Key: "start", Header: "START"},
		{Key: "end", Header: "END"},
		{Key: "events", Header: "EVENTS"},
		{Key: "bytes", Header: "BYTES"},
		{Key: "parts", Header: "PARTS"},
	}
	rows := make([][]any, 0, len(partitions))
	for _, p := range partitions {
		rows = append(rows, []any{p.Start, p.End, p.Events, p.Bytes, p.Parts})
	}
	return renderTabular(os.Stdout, cols, rows, ui.Stdout)
}
