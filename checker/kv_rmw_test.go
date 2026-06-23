package checker

import (
	"encoding/json"
	"testing"

	"github.com/anishathalye/porcupine"
)

// makeUidListValue serializes a []int as the JSON shape that
// ClientInterface.Read or ClientInterface.RMW would emit: a VList of VInt.
func makeUidListValue(uids []int) string {
	type rawValue struct {
		Type string          `json:"type"`
		Raw  json.RawMessage `json:"value"`
	}
	items := make([]rawValue, len(uids))
	for i, u := range uids {
		items[i] = rawValue{Type: "VInt", Raw: json.RawMessage(intToJSON(u))}
	}
	listRaw, _ := json.Marshal(items)
	out, _ := json.Marshal(rawValue{Type: "VList", Raw: listRaw})
	return string(out)
}

func intToJSON(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}

func runRMW(t *testing.T, ops []porcupine.Operation) bool {
	t.Helper()
	return porcupine.CheckOperations(KVRMWModel(), ops)
}

func op(call, ret int64, in KVInput, out interface{}) porcupine.Operation {
	return porcupine.Operation{
		Input:    in,
		Output:   out,
		Call:     call,
		Return:   ret,
		ClientId: int(call),
	}
}

func TestKVRMW_PutThenGet(t *testing.T) {
	// Blind PUT overwrites the key with [uid].
	ops := []porcupine.Operation{
		op(1, 2, KVInput{Op: "PUT", Key: "k", Uid: 7}, nil),
		op(3, 4, KVInput{Op: "GET", Key: "k"}, makeUidListValue([]int{7})),
	}
	if !runRMW(t, ops) {
		t.Fatalf("expected linearizable: blind PUT then GET")
	}
}

func TestKVRMW_PutOverwritesPriorState(t *testing.T) {
	// Two blind PUTs: the second overwrites the first; GET sees only the second.
	ops := []porcupine.Operation{
		op(1, 2, KVInput{Op: "PUT", Key: "k", Uid: 1}, nil),
		op(3, 4, KVInput{Op: "PUT", Key: "k", Uid: 2}, nil),
		op(5, 6, KVInput{Op: "GET", Key: "k"}, makeUidListValue([]int{2})),
	}
	if !runRMW(t, ops) {
		t.Fatalf("expected linearizable: PUT 2 overwrites PUT 1, GET sees [2]")
	}
}

func TestKVRMW_PutOverwriteRejectsAppendObservation(t *testing.T) {
	// If GET observes [1, 2] after two blind PUTs, the model must reject —
	// PUT is overwrite, not append.
	ops := []porcupine.Operation{
		op(1, 2, KVInput{Op: "PUT", Key: "k", Uid: 1}, nil),
		op(3, 4, KVInput{Op: "PUT", Key: "k", Uid: 2}, nil),
		op(5, 6, KVInput{Op: "GET", Key: "k"}, makeUidListValue([]int{1, 2})),
	}
	if runRMW(t, ops) {
		t.Fatalf("expected non-linearizable: GET cannot see appended log under blind PUT semantics")
	}
}

func TestKVRMW_RmwOnEmptyKeyReturnsEmpty(t *testing.T) {
	// RMW on an empty key returns [], appends uid; GET sees [uid].
	ops := []porcupine.Operation{
		op(1, 2, KVInput{Op: "RMW", Key: "k", Uid: 9}, makeUidListValue(nil)),
		op(3, 4, KVInput{Op: "GET", Key: "k"}, makeUidListValue([]int{9})),
	}
	if !runRMW(t, ops) {
		t.Fatalf("expected linearizable: RMW on empty key returns []")
	}
}

func TestKVRMW_RmwWrongOldOutput(t *testing.T) {
	// RMW claims to have observed [99] but the key was empty.
	ops := []porcupine.Operation{
		op(1, 2, KVInput{Op: "RMW", Key: "k", Uid: 9}, makeUidListValue([]int{99})),
	}
	if runRMW(t, ops) {
		t.Fatalf("expected non-linearizable: RMW returned wrong old value")
	}
}

func TestKVRMW_PutThenRmwThenGet(t *testing.T) {
	// PUT 1, RMW 2 (must return [1]), GET sees [1, 2].
	ops := []porcupine.Operation{
		op(1, 2, KVInput{Op: "PUT", Key: "k", Uid: 1}, nil),
		op(3, 4, KVInput{Op: "RMW", Key: "k", Uid: 2}, makeUidListValue([]int{1})),
		op(5, 6, KVInput{Op: "GET", Key: "k"}, makeUidListValue([]int{1, 2})),
	}
	if !runRMW(t, ops) {
		t.Fatalf("expected linearizable: PUT 1, RMW returning [1], GET returning [1, 2]")
	}
}

func TestKVRMW_RmwAfterPutWithWrongOld(t *testing.T) {
	// RMW after PUT 1 must return [1], not [].
	ops := []porcupine.Operation{
		op(1, 2, KVInput{Op: "PUT", Key: "k", Uid: 1}, nil),
		op(3, 4, KVInput{Op: "RMW", Key: "k", Uid: 2}, makeUidListValue(nil)),
	}
	if runRMW(t, ops) {
		t.Fatalf("expected non-linearizable: RMW after PUT must return [1]")
	}
}

func TestKVRMW_ConcurrentRmws(t *testing.T) {
	// Two concurrent RMWs on an empty key. Valid ordering: 10 first (returns
	// []), then 20 (returns [10]); GET sees [10, 20].
	ops := []porcupine.Operation{
		op(1, 4, KVInput{Op: "RMW", Key: "k", Uid: 10}, makeUidListValue(nil)),
		op(2, 5, KVInput{Op: "RMW", Key: "k", Uid: 20}, makeUidListValue([]int{10})),
		op(6, 7, KVInput{Op: "GET", Key: "k"}, makeUidListValue([]int{10, 20})),
	}
	if !runRMW(t, ops) {
		t.Fatalf("expected linearizable: ordering 10 then 20")
	}
}

func TestKVRMW_GetEmptyKey(t *testing.T) {
	ops := []porcupine.Operation{
		op(1, 2, KVInput{Op: "GET", Key: "k"}, makeUidListValue(nil)),
	}
	if !runRMW(t, ops) {
		t.Fatalf("expected linearizable: GET on empty key returns []")
	}
}

func TestKVRMW_PendingRmwSyntheticResponse(t *testing.T) {
	// A pending (no-response) RMW gets synthesized with Output=nil. The model
	// must accept it (skip the output check) so the run can still linearize.
	rows := []*EventRow{
		{UniqueID: "1", ClientID: "0", Kind: "Invocation", Action: Rmw,
			Payload: `["{\"type\":\"VNode\",\"value\":{\"role\":0,\"index\":0}}","{\"type\":\"VString\",\"value\":\"k\"}","{\"type\":\"VInt\",\"value\":42}"]`},
		{UniqueID: "2", ClientID: "0", Kind: "Invocation", Action: Read,
			Payload: `["{\"type\":\"VNode\",\"value\":{\"role\":0,\"index\":0}}","{\"type\":\"VString\",\"value\":\"k\"}"]`},
		{UniqueID: "2", ClientID: "0", Kind: "Response", Action: Read,
			Payload: `["` + escapeJSON(makeUidListValue([]int{42})) + `"]`},
	}
	ops, _ := BuildOperationsWithAnnotations(rows)
	if len(ops) != 2 {
		t.Fatalf("expected 2 ops (synthetic RMW + GET), got %d", len(ops))
	}
	foundRMW := false
	for _, o := range ops {
		if in, ok := o.Input.(KVInput); ok && in.Op == "RMW" && in.Uid == 42 {
			foundRMW = true
		}
	}
	if !foundRMW {
		t.Fatalf("expected synthetic RMW op with Uid=42, got ops: %+v", ops)
	}
	if !runRMW(t, ops) {
		t.Fatalf("expected linearizable: pending RMW accepted, GET observes [42]")
	}
}

// escapeJSON escapes a string for embedding inside a JSON string literal.
func escapeJSON(s string) string {
	b, _ := json.Marshal(s)
	return string(b[1 : len(b)-1])
}
