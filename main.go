package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/anishathalye/porcupine"
	"github.com/gocarina/gocsv"
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

type pendingInvocation struct {
	invRow   *EventRow
	callTime int64
	clientID int
}

type ActionType string

const (
	Read   ActionType = "read"
	Write  ActionType = "write"
	Delete ActionType = "delete"
)

func (e *ActionType) UnmarshalCSV(value string) error {
	switch {
	case strings.HasSuffix(value, "_read"):
		*e = Read
	case strings.HasSuffix(value, "_write"):
		*e = Write
	case strings.HasSuffix(value, "_delete"):
		*e = Delete
	default:
		*e = "Unknown operation."
	}
	return nil
}

type EventRow struct {
	UniqueID string     `csv:"UniqueID"`
	ClientID string     `csv:"ClientID"`
	Kind     string     `csv:"Kind"`
	Action   ActionType `csv:"Action"`
	Payload1 string     `csv:"Payload1"`
	Payload2 string     `csv:"Payload2"`
	Payload3 string     `csv:"Payload3"`
}

const (
	opWrite = "write"
	opRead  = "read"
)

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
	defer func(f *os.File) {
		_ = f.Close()
	}(f)

	var eventRows []*EventRow
	if err := gocsv.UnmarshalFile(f, &eventRows); err != nil {
		log.Fatalf("failed to unmarshal CSV: %v", err)
	}

	var ops []porcupine.Operation
	pendingInvocations := make(map[string]pendingInvocation)

	for i, row := range eventRows {
		syntheticTime := int64(i + 1)

		if row.Kind == "Invocation" {
			if _, exists := pendingInvocations[row.UniqueID]; exists {
				log.Printf("Warning: Found duplicate invocation for UniqueID %s. Overwriting.", row.UniqueID)
			}
			clientID := mustAtoi(row.ClientID)
			pendingInvocations[row.UniqueID] = pendingInvocation{
				invRow:   row,
				callTime: syntheticTime, // Set call time
				clientID: clientID,
			}
		} else if row.Kind == "Response" {
			inv, ok := pendingInvocations[row.UniqueID]
			if !ok {
				log.Printf("Warning: Found response for UniqueID %s without matching invocation. Skipping.", row.UniqueID)
				continue
			}
			delete(pendingInvocations, row.UniqueID)

			retTime := syntheticTime
			invRow := inv.invRow
			respRow := row

			if invRow.Action != Read && invRow.Action != Write && invRow.Action != Delete {
				continue
			}

			var opInput interface{}
			var opOutput interface{}

			switch invRow.Action {
			case Write:
				opInput = kvInput{
					Op:  "PUT",
					Key: invRow.Payload2, // Key from Invocation
					Val: invRow.Payload3, // Val from Invocation
				}
				opOutput = respRow.Payload1 // Output from Response
			case Read:
				opInput = kvInput{
					Op:  "GET",
					Key: invRow.Payload2, // Key from Invocation
					Val: "",              // Val is not used for GET
				}
				opOutput = respRow.Payload1 // Output from Response
			case Delete:
				opInput = kvInput{
					Op:  "DELETE",
					Key: invRow.Payload2,
					Val: "", // Val is not used for DELETE
				}
				opOutput = respRow.Payload1
			}
			ops = append(ops, porcupine.Operation{
				Input:    opInput,
				Output:   opOutput,
				Call:     inv.callTime, // The synthetic call time
				Return:   retTime,      // The synthetic return time
				ClientId: inv.clientID, // ClientID from Invocation
			})
		}
	}

	var model porcupine.Model
	switch *modelName {
	case "kv":
		model = kvModel()
	case "queue":
		model = queueModel()
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
