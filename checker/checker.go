package checker

import (
	"log"
	"strconv"
	"strings"

	"github.com/anishathalye/porcupine"
)

// ActionType represents the type of action performed on the data structure
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

// EventRow represents a single row in the history CSV file
type EventRow struct {
	UniqueID string     `csv:"UniqueID"`
	ClientID string     `csv:"ClientID"`
	Kind     string     `csv:"Kind"`
	Action   ActionType `csv:"Action"`
	Payload1 string     `csv:"Payload1"`
	Payload2 string     `csv:"Payload2"`
	Payload3 string     `csv:"Payload3"`
}

type pendingInvocation struct {
	invRow   *EventRow
	callTime int64
	clientID int
}

func mustAtoi(s string) int {
	v, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		log.Fatalf("bad int %q: %v", s, err)
	}
	return v
}

// BuildOperations converts a slice of EventRows into porcupine Operations
func BuildOperations(eventRows []*EventRow) []porcupine.Operation {
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
				callTime: syntheticTime,
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
				opInput = KVInput{
					Op:  "PUT",
					Key: invRow.Payload2,
					Val: invRow.Payload3,
				}
				opOutput = respRow.Payload1
			case Read:
				opInput = KVInput{
					Op:  "GET",
					Key: invRow.Payload2,
					Val: "",
				}
				opOutput = respRow.Payload1
			case Delete:
				opInput = KVInput{
					Op:  "DELETE",
					Key: invRow.Payload2,
					Val: "",
				}
				opOutput = respRow.Payload1
			}
			ops = append(ops, porcupine.Operation{
				Input:    opInput,
				Output:   opOutput,
				Call:     inv.callTime,
				Return:   retTime,
				ClientId: inv.clientID,
			})
		}
	}

	return ops
}
