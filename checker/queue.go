package checker

import (
	"fmt"
	"strings"

	"github.com/anishathalye/porcupine"
)

// QueueInput represents an input to a queue operation
type QueueInput struct {
	Op  string // "ENQ" or "DEQ"
	Val string // for ENQ: the string to enqueue; for DEQ: unused
}

// QueueModel returns a porcupine.Model for a FIFO queue
func QueueModel() porcupine.Model {
	return porcupine.Model{
		// State is a FIFO queue of strings
		Init: func() interface{} { return []string{} },

		Step: func(state, input, output interface{}) (bool, interface{}) {
			q := append([]string{}, state.([]string)...)
			in := input.(QueueInput)
			out := ""
			out, _ = output.(string)

			switch strings.ToUpper(in.Op) {
			case "ENQ":
				// Enqueue: accept any output (often empty)
				q = append(q, in.Val)
				return true, q

			case "DEQ":
				if len(q) == 0 {
					// Must have recorded "<empty>" for empty dequeue
					return out == "<empty>", q
				}
				hd := q[0]
				if out != hd {
					return false, state
				}
				return true, q[1:]

			default:
				// Unknown operation this should not happen
				fmt.Println("Debug: Unknown Ops")
				return false, state
			}
		},

		Equal: func(a, b interface{}) bool {
			x := a.([]string)
			y := b.([]string)
			if len(x) != len(y) {
				return false
			}
			for i := range x {
				if x[i] != y[i] {
					return false
				}
			}
			return true
		},

		DescribeOperation: func(input, output interface{}) string {
			in := input.(QueueInput)
			switch strings.ToUpper(in.Op) {
			case "ENQ":
				return fmt.Sprintf("ENQ(%q)", in.Val)
			case "DEQ":
				if output == nil {
					return "DEQ(?)"
				}
				return fmt.Sprintf("DEQ→%q", output.(string))
			default:
				return "??"
			}
		},

		DescribeState: func(state interface{}) string {
			return fmt.Sprintf("%v", state.([]string))
		},
	}
}
