package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type mockClient struct{}

func (m *mockClient) ReadHoldingRegisters(address, quantity uint16) ([]byte, error) {
	out := make([]byte, int(quantity)*2)
	for i := 0; i < int(quantity); i++ {
		v := address + uint16(i)
		if address == 40020 {
			v = 2
		}
		binary.BigEndian.PutUint16(out[i*2:i*2+2], v)
	}
	return out, nil
}

func (m *mockClient) WriteSingleRegister(address, value uint16) ([]byte, error) {
	return []byte{0, 0}, nil
}

func (m *mockClient) WriteMultipleRegisters(address, quantity uint16, value []byte) ([]byte, error) {
	return []byte{0, 0}, nil
}

func TestBasicReadWriteStatus(t *testing.T) {
	st := newState(&mockClient{})
	ts := httptest.NewServer(st.routes())
	defer ts.Close()

	res, err := http.Get(ts.URL + "/api/read/100?quantity=2")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("unexpected read status: %d", res.StatusCode)
	}

	payload := strings.NewReader(`{"values":[10,11]}`)
	res, err = http.Post(ts.URL+"/api/write/100", "application/json", payload)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("unexpected write status: %d", res.StatusCode)
	}

	res, err = http.Get(ts.URL + "/api/status")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status code: %d", res.StatusCode)
	}
	var status map[string]any
	if err := json.NewDecoder(res.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	if status["connected"] != true {
		t.Fatalf("expected connected true, got %v", status["connected"])
	}
	if _, ok := status["last_poll_time"]; !ok {
		t.Fatal("expected last_poll_time in status")
	}
}

func TestProfileEndpointsAndScan(t *testing.T) {
	st := newState(&mockClient{})
	ts := httptest.NewServer(st.routes())
	defer ts.Close()

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, err := mw.CreateFormFile("file", "profile.yaml")
	if err != nil {
		t.Fatal(err)
	}
	profileYAML := `device:
  name: "SEL-751A"
  connection: "192.168.1.100:502"
  timeout: "500ms"
  protocol: "tcp"
registers:
  - address: 40020
    name: "Fault Code"
    type: enum
    mapping:
      0: "No Fault"
      1: "Overcurrent"
      2: "Overvoltage"
`
	if _, err := io.WriteString(fw, profileYAML); err != nil {
		t.Fatal(err)
	}
	_ = mw.Close()

	res, err := http.Post(ts.URL+"/api/profile/import", mw.FormDataContentType(), &body)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("unexpected import status: %d", res.StatusCode)
	}

	res, err = http.Get(ts.URL + "/api/read/40020?quantity=1")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("unexpected read status: %d", res.StatusCode)
	}

	res, err = http.Get(ts.URL + "/api/parse/40020")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("unexpected parse status: %d", res.StatusCode)
	}
	var parsed map[string]any
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		t.Fatal(err)
	}
	if parsed["value"] != "Overvoltage" {
		t.Fatalf("unexpected parsed value: %v", parsed["value"])
	}

	res, err = http.Get(ts.URL + "/api/scan/40020-40020")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("unexpected scan status: %d", res.StatusCode)
	}

	res, err = http.Get(ts.URL + "/api/profile")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("unexpected profile status: %d", res.StatusCode)
	}

	res, err = http.Get(ts.URL + "/api/profile/export")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("unexpected export status: %d", res.StatusCode)
	}
}

func TestParseByType(t *testing.T) {
	v, err := parseByType(registerDef{Type: "float32", ByteOrder: "big", Scale: 0.001}, []uint16{0x447A, 0x0000})
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(v.(float64)-1) > 1e-9 {
		t.Fatalf("unexpected float32 value: %v", v)
	}

	v, err = parseByType(registerDef{Type: "bits", Bits: map[int]string{0: "Running", 2: "Fault"}}, []uint16{0b0101})
	if err != nil {
		t.Fatal(err)
	}
	bits, ok := v.(map[string]bool)
	if !ok || !bits["Running"] || !bits["Fault"] {
		t.Fatalf("unexpected bits parse: %#v", v)
	}

	v, err = parseByType(registerDef{Type: "enum", Mapping: map[int]string{2: "Overvoltage"}}, []uint16{2})
	if err != nil {
		t.Fatal(err)
	}
	if v.(string) != "Overvoltage" {
		t.Fatalf("unexpected enum parse: %v", v)
	}
}
