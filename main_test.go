package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	modbusprotocol "github.com/tamzrod/modbus/protocol"
)

type mockRequester struct{}

func (m *mockRequester) Do(cfg normalizedConfig, function uint8, payload []byte) (*modbusprotocol.Response, error) {
	resp := &modbusprotocol.Response{Function: function}
	switch function {
	case 1, 2:
		qty := int(binary.BigEndian.Uint16(payload[2:4]))
		byteCount := (qty + 7) / 8
		body := make([]byte, byteCount+1)
		body[0] = uint8(byteCount)
		for i := 0; i < qty; i++ {
			if i%2 == 0 {
				body[1+i/8] |= 1 << uint(i%8)
			}
		}
		resp.Payload = body
	case 3, 4:
		start := int(binary.BigEndian.Uint16(payload[0:2])) + 1
		qty := int(binary.BigEndian.Uint16(payload[2:4]))
		body := bytes.NewBuffer([]byte{uint8(qty * 2)})
		for i := 0; i < qty; i++ {
			_ = binary.Write(body, binary.BigEndian, uint16(start+i))
		}
		resp.Payload = body.Bytes()
	case 5, 6:
		resp.Payload = payload
	default:
		ex := modbusprotocol.ExIllegalFunction
		resp.Exception = &ex
	}
	return resp, nil
}

func newTestServer() (*httptest.Server, *appState) {
	st := newApp(&mockRequester{})
	return httptest.NewServer(st.routes()), st
}

type recordingRequester struct {
	functions []uint8
	payloads  [][]byte
}

func (r *recordingRequester) Do(cfg normalizedConfig, function uint8, payload []byte) (*modbusprotocol.Response, error) {
	r.functions = append(r.functions, function)
	cp := make([]byte, len(payload))
	copy(cp, payload)
	r.payloads = append(r.payloads, cp)

	resp := &modbusprotocol.Response{Function: function}
	switch function {
	case 1:
		resp.Payload = []byte{1, 0x01}
	case 3:
		resp.Payload = []byte{2, 0x00, 0x2A}
	default:
		resp.Payload = payload
	}
	return resp, nil
}

func TestBulkSingleReadWriteAndStatus(t *testing.T) {
	ts, _ := newTestServer()
	defer ts.Close()

	bulkReq := map[string]any{"config": map[string]any{"function_code": 3, "start_address": 40001, "quantity": 3, "unit_id": 1, "target": "127.0.0.1:502", "timeout_ms": 500}}
	res := postJSON(t, ts.URL+"/api/read/bulk", bulkReq)
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("unexpected bulk status: %d", res.StatusCode)
	}

	var bulkResp map[string][]rowResult
	if err := json.NewDecoder(res.Body).Decode(&bulkResp); err != nil {
		t.Fatal(err)
	}
	if len(bulkResp["rows"]) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(bulkResp["rows"]))
	}
	if bulkResp["rows"][0].ValueDec != 40001 {
		t.Fatalf("unexpected first row dec value: %d", bulkResp["rows"][0].ValueDec)
	}

	singleReq := map[string]any{"config": map[string]any{"function_code": 3, "start_address": 40001, "quantity": 3, "unit_id": 1, "target": "127.0.0.1:502", "timeout_ms": 500}, "address": 40002}
	res = postJSON(t, ts.URL+"/api/read/single", singleReq)
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("unexpected single read status: %d", res.StatusCode)
	}

	writeReq := map[string]any{"config": map[string]any{"function_code": 3, "start_address": 40001, "quantity": 3, "unit_id": 1, "target": "127.0.0.1:502", "timeout_ms": 500}, "address": 40001, "value": 123}
	res = postJSON(t, ts.URL+"/api/write/single", writeReq)
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("unexpected write status: %d", res.StatusCode)
	}

	statusRes, err := http.Get(ts.URL + "/api/status")
	if err != nil {
		t.Fatal(err)
	}
	defer statusRes.Body.Close()
	if statusRes.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status endpoint code: %d", statusRes.StatusCode)
	}
	var status map[string]any
	if err := json.NewDecoder(statusRes.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	if status["connected"] != true {
		t.Fatalf("expected connected true, got %v", status["connected"])
	}
	if status["last_read_timestamp"] == "" {
		t.Fatal("expected last_read_timestamp")
	}
}

func TestPollingDisablesWriteButAllowsSingleRead(t *testing.T) {
	ts, _ := newTestServer()
	defer ts.Close()

	startPollingReq := map[string]any{"config": map[string]any{"function_code": 3, "start_address": 40001, "quantity": 2, "unit_id": 1, "target": "127.0.0.1:502", "timeout_ms": 500}, "interval_ms": 20}
	res := postJSON(t, ts.URL+"/api/polling/start", startPollingReq)
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("unexpected polling start status: %d", res.StatusCode)
	}

	time.Sleep(50 * time.Millisecond)

	readReq := map[string]any{"config": map[string]any{"function_code": 3, "start_address": 40001, "quantity": 2, "unit_id": 1, "target": "127.0.0.1:502", "timeout_ms": 500}, "address": 40001}
	res = postJSON(t, ts.URL+"/api/read/single", readReq)
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("single read should work during polling, got %d", res.StatusCode)
	}

	writeReq := map[string]any{"config": map[string]any{"function_code": 3, "start_address": 40001, "quantity": 2, "unit_id": 1, "target": "127.0.0.1:502", "timeout_ms": 500}, "address": 40001, "value": 15}
	res = postJSON(t, ts.URL+"/api/write/single", writeReq)
	defer res.Body.Close()
	if res.StatusCode != http.StatusConflict {
		t.Fatalf("write should be blocked during polling, got %d", res.StatusCode)
	}

	stopRes := postJSON(t, ts.URL+"/api/polling/stop", map[string]any{})
	defer stopRes.Body.Close()
	if stopRes.StatusCode != http.StatusOK {
		t.Fatalf("unexpected polling stop status: %d", stopRes.StatusCode)
	}
}

func TestWriteBlockedForReadOnlyFunctions(t *testing.T) {
	ts, _ := newTestServer()
	defer ts.Close()

	writeReq := map[string]any{"config": map[string]any{"function_code": 4, "start_address": 40001, "quantity": 1, "unit_id": 1, "target": "127.0.0.1:502", "timeout_ms": 500}, "address": 40001, "value": 1}
	res := postJSON(t, ts.URL+"/api/write/single", writeReq)
	defer res.Body.Close()
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("write should fail for function 04, got %d", res.StatusCode)
	}
}

func TestBulkReadSupportedFunctionCodes(t *testing.T) {
	ts, _ := newTestServer()
	defer ts.Close()

	cases := []struct {
		name      string
		function  int
		startAddr int
		quantity  int
		wantFirst int
		wantLast  int
	}{
		{name: "fc01 coils", function: 1, startAddr: 1, quantity: 4, wantFirst: 1, wantLast: 0},
		{name: "fc02 inputs", function: 2, startAddr: 1, quantity: 4, wantFirst: 1, wantLast: 0},
		{name: "fc03 holding", function: 3, startAddr: 40001, quantity: 3, wantFirst: 40001, wantLast: 40003},
		{name: "fc04 input registers", function: 4, startAddr: 30001, quantity: 3, wantFirst: 30001, wantLast: 30003},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := map[string]any{
				"config": map[string]any{
					"function_code": tc.function,
					"start_address": tc.startAddr,
					"quantity":      tc.quantity,
					"unit_id":       1,
					"target":        "127.0.0.1:502",
					"timeout_ms":    500,
				},
			}
			res := postJSON(t, ts.URL+"/api/read/bulk", req)
			defer res.Body.Close()
			if res.StatusCode != http.StatusOK {
				t.Fatalf("unexpected status: %d", res.StatusCode)
			}

			var out map[string][]rowResult
			if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
				t.Fatal(err)
			}
			rows := out["rows"]
			if len(rows) != tc.quantity {
				t.Fatalf("expected %d rows, got %d", tc.quantity, len(rows))
			}
			if rows[0].ValueDec != tc.wantFirst {
				t.Fatalf("unexpected first value: %d", rows[0].ValueDec)
			}
			if rows[len(rows)-1].ValueDec != tc.wantLast {
				t.Fatalf("unexpected last value: %d", rows[len(rows)-1].ValueDec)
			}
		})
	}
}

func TestWriteMapsToModbusWriteFunctions(t *testing.T) {
	req := &recordingRequester{}
	st := newApp(req)

	row, err := st.writeSingle(normalizedConfig{FunctionCode: 1, StartAddress: 1, Quantity: 1}, 1, 1)
	if err != nil {
		t.Fatalf("write with fc01 failed: %v", err)
	}
	if row.ValueDec != 1 {
		t.Fatalf("unexpected readback for fc01 write: %d", row.ValueDec)
	}
	if len(req.functions) < 2 {
		t.Fatalf("expected at least two modbus calls for fc01 write, got %d", len(req.functions))
	}
	if req.functions[0] != 5 || req.functions[1] != 1 {
		t.Fatalf("unexpected function sequence for fc01 write: %v", req.functions[:2])
	}
	if got := binary.BigEndian.Uint16(req.payloads[0][2:4]); got != 0xFF00 {
		t.Fatalf("expected coil ON payload 0xFF00, got 0x%04X", got)
	}

	req.functions = nil
	req.payloads = nil

	row, err = st.writeSingle(normalizedConfig{FunctionCode: 3, StartAddress: 40001, Quantity: 1}, 40001, 42)
	if err != nil {
		t.Fatalf("write with fc03 failed: %v", err)
	}
	if row.ValueDec != 42 {
		t.Fatalf("unexpected readback for fc03 write: %d", row.ValueDec)
	}
	if len(req.functions) < 2 {
		t.Fatalf("expected at least two modbus calls for fc03 write, got %d", len(req.functions))
	}
	if req.functions[0] != 6 || req.functions[1] != 3 {
		t.Fatalf("unexpected function sequence for fc03 write: %v", req.functions[:2])
	}
	if got := binary.BigEndian.Uint16(req.payloads[0][2:4]); got != 42 {
		t.Fatalf("expected register write payload 42, got %d", got)
	}
}

func postJSON(t *testing.T, url string, payload any) *http.Response {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return res
}
