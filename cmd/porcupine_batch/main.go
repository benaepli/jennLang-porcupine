// Fast Porcupine batch checker: no HTML visualization, machine-readable JSON
// output, per-run timeouts, and Unknown-vs-Illegal separation.
//
// JSON goes to stdout (and optionally to -json <path>); human-readable
// progress goes to stderr.
//
// Exit codes:
//
//	0 - all runs linearizable
//	1 - usage / IO / query error
//	2 - at least one run is NOT linearizable
//	3 - no runs found in the input
//	4 - no violations, but at least one run was Unknown (timeout / check failure)
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/benaepli/turnpike-porcupine/checker"
)

// Result is the machine-readable summary consumed by the research harness.
type Result struct {
	Input           string `json:"input"`
	Model           string `json:"model"`
	TotalRuns       int    `json:"total_runs"`
	Ok              int    `json:"ok"`
	Violations      int    `json:"violations"`
	Unknown         int    `json:"unknown"`
	SkippedOps      int    `json:"skipped_ops"`
	ViolatingRunIDs []int  `json:"violating_run_ids"`
	UnknownRunIDs   []int  `json:"unknown_run_ids"`
	WallMs          int64  `json:"wall_ms"`
}

func knownAction(a checker.ActionType) bool {
	switch a {
	case checker.Read, checker.Write, checker.Rmw, checker.Delete,
		checker.Crash, checker.Recover, checker.Timeout:
		return true
	}
	return false
}

func main() {
	inputPath := flag.String("input", "", "Path to DuckDB file or Parquet output directory (required)")
	modelName := flag.String("model", "", "Model to check: kv|kv_rmw|queue (required)")
	timeoutMs := flag.Int("timeout", 10000, "Per-run check timeout in milliseconds (0 = no timeout)")
	jsonPath := flag.String("json", "", "Also write the JSON result to this file (optional)")
	flag.Parse()

	// Back-compat: `porcupine_batch <dir>` positional form (model defaults to kv).
	if *inputPath == "" && flag.NArg() == 1 {
		*inputPath = flag.Arg(0)
	}
	if *inputPath == "" || flag.NArg() > 1 {
		flag.Usage()
		log.Fatalln("Error: -input is required.")
	}
	if *modelName == "" {
		*modelName = "kv"
	}

	var model porcupine.Model
	switch *modelName {
	case "kv":
		model = checker.KVModel()
	case "kv_rmw":
		model = checker.KVRMWModel()
	case "queue":
		model = checker.QueueModel()
	default:
		log.Fatalf("unknown model %q (use kv|kv_rmw|queue)", *modelName)
	}

	res := Result{
		Input:           *inputPath,
		Model:           *modelName,
		ViolatingRunIDs: []int{},
		UnknownRunIDs:   []int{},
	}
	start := time.Now()

	err := checker.ProcessAllRunsFromDuckDB(*inputPath, func(runID int, events []*checker.EventRow) error {
		res.TotalRuns++
		kept := events[:0:0]
		for _, row := range events {
			if knownAction(row.Action) {
				kept = append(kept, row)
			} else {
				res.SkippedOps++
			}
		}
		ops, _ := checker.BuildOperationsWithAnnotations(kept)
		verdict, _ := porcupine.CheckOperationsVerbose(model, ops, time.Duration(*timeoutMs)*time.Millisecond)
		switch verdict {
		case porcupine.Ok:
			res.Ok++
		case porcupine.Illegal:
			res.Violations++
			res.ViolatingRunIDs = append(res.ViolatingRunIDs, runID)
			if res.Violations <= 20 {
				fmt.Fprintf(os.Stderr, "run %d: NOT linearizable\n", runID)
			}
		default: // porcupine.Unknown: timeout or aborted check
			res.Unknown++
			res.UnknownRunIDs = append(res.UnknownRunIDs, runID)
			fmt.Fprintf(os.Stderr, "run %d: UNKNOWN (timeout or check failure)\n", runID)
		}
		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	res.WallMs = time.Since(start).Milliseconds()

	fmt.Fprintf(os.Stderr, "\n=== %s (model=%s) ===\n", *inputPath, *modelName)
	fmt.Fprintf(os.Stderr, "total=%d ok=%d violations=%d unknown=%d skipped_ops=%d wall_ms=%d\n",
		res.TotalRuns, res.Ok, res.Violations, res.Unknown, res.SkippedOps, res.WallMs)

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(&res); err != nil {
		log.Fatalf("failed to encode JSON: %v", err)
	}
	if *jsonPath != "" {
		f, err := os.Create(*jsonPath)
		if err != nil {
			log.Fatalf("failed to create %s: %v", *jsonPath, err)
		}
		fenc := json.NewEncoder(f)
		fenc.SetIndent("", "  ")
		if err := fenc.Encode(&res); err != nil {
			log.Fatalf("failed to write %s: %v", *jsonPath, err)
		}
		_ = f.Close()
	}

	switch {
	case res.TotalRuns == 0:
		os.Exit(3)
	case res.Violations > 0:
		os.Exit(2)
	case res.Unknown > 0:
		os.Exit(4)
	}
}
