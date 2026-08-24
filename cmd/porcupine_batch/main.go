// Fast Porcupine batch check without HTML visualization.
package main

import (
	"fmt"
	"os"

	"github.com/anishathalye/porcupine"
	"github.com/benaepli/turnpike-porcupine/checker"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: porcupine_batch <output_dir>")
		os.Exit(1)
	}
	path := os.Args[1]
	model := checker.KVModel()

	var total, violations int
	var violatingIDs []int

	err := checker.ProcessAllRunsFromDuckDB(path, func(runID int, events []*checker.EventRow) error {
		total++
		ops, _ := checker.BuildOperationsWithAnnotations(events)
		res, _ := porcupine.CheckOperationsVerbose(model, ops, 0)
		if res != porcupine.Ok {
			violations++
			violatingIDs = append(violatingIDs, runID)
			if len(violatingIDs) <= 20 {
				fmt.Printf("run %d: NOT linearizable\n", runID)
			}
		}
		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\n=== %s ===\n", path)
	fmt.Printf("total=%d violations=%d\n", total, violations)
	if len(violatingIDs) > 20 {
		fmt.Printf("first 20 violating run_ids: %v\n", violatingIDs[:20])
	} else if len(violatingIDs) > 0 {
		fmt.Printf("violating run_ids: %v\n", violatingIDs)
	}

	if violations > 0 {
		os.Exit(2)
	}
}
