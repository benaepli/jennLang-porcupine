package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/anishathalye/porcupine"
	"github.com/gocarina/gocsv"

	"github.com/benaepli/jennlang-porcupine/checker"
)

func main() {
	inputFile := flag.String("input", "", "Path to the input history CSV file (required)")
	outputFile := flag.String("output", "", "Path for the output visualization HTML file (required)")
	modelName := flag.String("model", "", "Model to check (e.g., 'kv', 'queue') (required)")
	flag.Parse()

	// Validate required flags
	if *inputFile == "" || *outputFile == "" || *modelName == "" {
		flag.Usage()
		log.Fatalln("Error: -input, -output, and -model flags are all required.")
	}

	f, err := os.Open(*inputFile)
	if err != nil {
		log.Fatalf("failed to open input file %s: %v", *inputFile, err)
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(f)

	var eventRows []*checker.EventRow
	if err := gocsv.UnmarshalFile(f, &eventRows); err != nil {
		log.Fatalf("failed to unmarshal CSV: %v", err)
	}

	ops := checker.BuildOperations(eventRows)

	var model porcupine.Model
	switch *modelName {
	case "kv":
		model = checker.KVModel()
	case "queue":
		model = checker.QueueModel()
	default:
		log.Fatalf("unknown model %q (use kv|queue)", *modelName)
	}

	res, info := porcupine.CheckOperationsVerbose(model, ops, 0)
	if res == porcupine.Ok {
		fmt.Println("Linearizable? true")
	} else if res == porcupine.Illegal {
		fmt.Println("Linearizable? false")
	} else {
		fmt.Println("Linearizable? Unknown (Check failed)")
	}

	if err := porcupine.VisualizePath(model, info, *outputFile); err != nil {
		log.Fatalf("failed to write visualization: %v", err)
	} else {
		fmt.Printf("Visualization written to %s\n", *outputFile)
	}

	if res != porcupine.Ok {
		log.Println("History is NOT linearizable.")
		os.Exit(2)
	}
}
