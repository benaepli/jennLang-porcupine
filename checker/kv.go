package checker

import (
	"fmt"
	"maps"
	"sort"
	"strings"

	"github.com/anishathalye/porcupine"
)

// KVInput represents an input to a key-value store operation
type KVInput struct {
	Op  string // "GET" / "PUT" / "DELETE"
	Key string // the key
	Val string // for "PUT" ops
}

const (
	// The expected output of a successful write
	writeOutput = "{\"type\":\"VTuple\",\"value\":[]}"
	// The expected output of a read on a non-existent key
	readNil = "{\"type\":\"VOption\",\"value\":null}"
)

// KVModel returns a porcupine.Model for a key-value store
func KVModel() porcupine.Model {
	return porcupine.Model{
		Init: func() interface{} { return map[string]string{} },

		Step: func(state, input, output interface{}) (bool, interface{}) {
			q := maps.Clone(state.(map[string]string))
			in := input.(KVInput)
			out := ""
			out, _ = output.(string)

			switch strings.ToUpper(in.Op) {
			case "PUT":
				wrappedVal := fmt.Sprintf("{\"type\":\"VOption\",\"value\":%s}", in.Val)
				q[in.Key] = wrappedVal
				return true, q

			case "GET":
				v, ok := q[in.Key]

				if !ok {
					return out == readNil, q
				}
				return out == v, q

			case "DELETE":
				delete(q, in.Key)
				return true, q
			default:
				// Unknown operation this should not happen
				fmt.Println("Debug: Unknown Ops")
				return false, state
			}
		},

		Equal: func(a, b interface{}) bool {
			return maps.Equal(a.(map[string]string), b.(map[string]string))
		},

		DescribeOperation: func(input, output interface{}) string {
			in := input.(KVInput)
			switch strings.ToUpper(in.Op) {
			case "PUT":
				return fmt.Sprintf("PUT <(%q), (%q)>", in.Key, in.Val)
			case "GET":
				if output == nil {
					return "DEQ(?)"
				}
				return fmt.Sprintf("GET (%q)→%q", in.Key, output.(string))
			case "DELETE":
				return fmt.Sprintf("DELETE <(%q), (%q)>", in.Key, in.Val)
			default:
				return "??"
			}
		},

		DescribeState: func(state interface{}) string {
			// pretty print map with sorted keys (stable viz)
			m := state.(map[string]string)
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			var b strings.Builder
			b.WriteString("{")
			for i, k := range keys {
				if i > 0 {
					b.WriteString(", ")
				}
				_, _ = fmt.Fprintf(&b, "%s:%s", k, m[k])
			}
			b.WriteString("}")
			return b.String()
		},
	}
}
