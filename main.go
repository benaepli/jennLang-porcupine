package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/anishathalye/porcupine"
)

func mustAtoi(s string) int {
	v, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		log.Fatalf("bad int %q: %v", s, err)
	}
	return v
}

func mustAtoi64(s string) int64 {
	v, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		log.Fatalf("bad int64 %q: %v", s, err)
	}
	return v
}

func main() {
	inputFile := flag.String("input", "", "Path to the input history CSV file (required)")
	outputFile := flag.String("output", "", "Path for the output visualization HTML file (required)")
	modelName := flag.String("model", "", "Model to check (e.g., 'kv', 'queue') (required)")
	flag.Parse()

	// Validate required flags
	if *inputFile == "" || *outputFile == "" || *modelName == "" {
		flag.Usage() // Print default usage message
		log.Fatalln("Error: -input, -output, and -model flags are all required.")
	}

	f, err := os.Open(*inputFile)
	if err != nil {
		log.Fatalf("failed to open input file %s: %v", *inputFile, err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1 // allow variable; we’ll pick by header

	// Read header
	header, err := r.Read()
	if err != nil {
		log.Fatalf("read header: %v", err)
	}
	index := func(name string) int {
		name = strings.ToLower(name)
		for i, h := range header {
			if strings.ToLower(strings.TrimSpace(h)) == name {
				return i
			}
		}
		log.Fatalf("missing required column %q in header: %v", name, header)
		return -1
	}
	iCall := index("call_ns")
	iRet := index("return_ns")
	iClient := index("client_id")
	iOp := index("op")
	iVal := index("value")
	iOut := index("output")
	// Conditionally get "key" index only if model is "kv"
	var iKey int
	if *modelName == "kv" {
		iKey = index("key")
	}

	var ops []porcupine.Operation
	rowNum := 1
	for {
		row, err := r.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			log.Fatalf("read row %d: %v", rowNum, err)
		}
		rowNum++

		call := mustAtoi64(row[iCall])
		ret := mustAtoi64(row[iRet])
		client := mustAtoi(row[iClient])
		op := strings.TrimSpace(row[iOp])
		val := strings.TrimSpace(row[iVal])
		out := strings.TrimSpace(row[iOut])

		// Normalize empty output to nil
		var outAny interface{}
		if out != "" {
			outAny = out
		}

		// Handle different applications based on the model flag
		switch *modelName {
		case "queue":
			ops = append(ops, porcupine.Operation{
				Input:    qInput{Op: op, Val: val}, // Assumes qInput type exists
				Output:   outAny,
				Call:     call,
				Return:   ret,
				ClientId: client,
			})
		case "kv":
			ops = append(ops, porcupine.Operation{
				Input:    kvInput{Op: op, Key: strings.TrimSpace(row[iKey]), Val: val}, // Assumes kvInput type exists
				Output:   outAny,
				Call:     call,
				Return:   ret,
				ClientId: client,
			})
		default:
			log.Fatalf("unknown model %q (use kv|queue)", *modelName)
		}
	}

	// Check linearizability + visualize
	var model porcupine.Model
	switch *modelName {
	case "kv":
		model = kvModel() // Assumes kvModel function exists
	case "queue":
		model = queueModel() // Assumes queueModel function exists
	default:
		log.Fatalf("unknown model %q (use kv|queue)", *modelName)
	}

	res, info := porcupine.CheckOperationsVerbose(model, ops, 0) // 0 = no timeout
	fmt.Println("Linearizable?", res == porcupine.Ok)

	// Always produce a visualization so you can inspect even passing runs
	if err := porcupine.VisualizePath(model, info, *outputFile); err != nil {
		log.Fatalf("failed to write visualization to %s: %v", *outputFile, err)
	}
	fmt.Printf("Wrote visualization to %s (open it in a browser)\n", *outputFile)

	// Exit with a non-zero status for CI if not linearizable
	if res != porcupine.Ok {
		log.Println("History is NOT linearizable.")
		os.Exit(2)
	}
}
