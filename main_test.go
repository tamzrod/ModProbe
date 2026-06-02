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
