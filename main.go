package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	modbusprotocol "github.com/tamzrod/modbus/protocol"
	tcptransport "github.com/tamzrod/modbus/transport/tcp"
)

const (
	defaultBindAddr      = "localhost:8080"
	defaultTarget        = "127.0.0.1:502"
	defaultTargetPort    = "502"
	defaultUnitID        = 1
	defaultTimeoutMS     = 500
	defaultFunctionCode  = 3
	defaultStartAddress  = 0
	defaultQuantity      = 10
	defaultPollingMS     = 1000
	maxTimeoutMS         = 60000
	maxPollIntervalMS    = 60000
	maxRegisterReadCount = 125
	maxBitReadCount      = 2000
	defaultTestTimeoutMS = 2500
	maxRequestBodySize   = 1 << 20
	maxStoredErrorLength = 1000
)

var errPollingActive = errors.New("writes are disabled while polling is active")

type connectionConfig struct {
	Target       string `json:"target"`
	UnitID       int    `json:"unit_id"`
	TimeoutMS    int    `json:"timeout_ms"`
	FunctionCode int    `json:"function_code"`
	StartAddress int    `json:"start_address"`
	Quantity     int    `json:"quantity"`
}

type rowResult struct {
	Address   int    `json:"address"`
	ValueHex  string `json:"value_hex"`
	ValueDec  int    `json:"value_dec"`
	Exception string `json:"exception"`
	Timestamp string `json:"timestamp"`
}

type bulkReadRequest struct {
	Config connectionConfig `json:"config"`
}

type singleReadRequest struct {
	Config  connectionConfig `json:"config"`
	Address int              `json:"address"`
}

type writeRequest struct {
	Config  connectionConfig `json:"config"`
	Address int              `json:"address"`
	Value   int              `json:"value"`
}

type pollingStartRequest struct {
	Config     connectionConfig `json:"config"`
	IntervalMS int              `json:"interval_ms"`
}

type testRequest struct {
	Type      TestType `json:"type"`
	Target    string   `json:"target"`
	TimeoutMS int      `json:"timeout_ms"`
}

type TestType string

const (
	TestTCP  TestType = "tcp"
	TestICMP TestType = "icmp"
)

type TestResult struct {
	Type    string        `json:"type"`
	Target  string        `json:"target"`
	Success bool          `json:"success"`
	Latency time.Duration `json:"-"`
	Error   string        `json:"error"`
}

type modbusRequester interface {
	Do(cfg normalizedConfig, function uint8, payload []byte) (*modbusprotocol.Response, error)
}

type tcpRequester struct {
	tid atomic.Uint32
}

type normalizedConfig struct {
	Target       string
	UnitID       uint8
	Timeout      time.Duration
	FunctionCode uint8
	StartAddress int
	Quantity     int
}

type appState struct {
	mu           sync.RWMutex
	requester    modbusRequester
	lastReadTime *time.Time
	lastError    string
	rows         map[int]rowResult
	poller       *poller
}

type poller struct {
	mu         sync.RWMutex
	active     bool
	intervalMS int
	cfg        normalizedConfig
	stopCh     chan struct{}
	doneCh     chan struct{}
}

func newApp(requester modbusRequester) *appState {
	a := &appState{
		requester: requester,
		rows:      map[int]rowResult{},
	}
	a.poller = &poller{}
	return a
}

func normalizeConfig(cfg connectionConfig) (normalizedConfig, error) {
	if cfg.Target == "" {
		cfg.Target = defaultTarget
	}
	if cfg.UnitID == 0 {
		cfg.UnitID = defaultUnitID
	}
	if cfg.TimeoutMS == 0 {
		cfg.TimeoutMS = defaultTimeoutMS
	}
	if cfg.FunctionCode == 0 {
		cfg.FunctionCode = defaultFunctionCode
	}
	if cfg.StartAddress == 0 {
		cfg.StartAddress = defaultStartAddress
	}
	if cfg.Quantity == 0 {
		cfg.Quantity = defaultQuantity
	}
	if cfg.UnitID < 0 || cfg.UnitID > math.MaxUint8 {
		return normalizedConfig{}, errors.New("unit_id out of range")
	}
	if cfg.TimeoutMS < 1 || cfg.TimeoutMS > maxTimeoutMS {
		return normalizedConfig{}, errors.New("timeout_ms out of range")
	}
	if cfg.StartAddress < 0 || cfg.StartAddress > math.MaxUint16 {
		return normalizedConfig{}, errors.New("start_address out of range")
	}
	if cfg.Quantity < 1 {
		return normalizedConfig{}, errors.New("quantity must be at least 1")
	}
	if cfg.FunctionCode != 1 && cfg.FunctionCode != 2 && cfg.FunctionCode != 3 && cfg.FunctionCode != 4 {
		return normalizedConfig{}, errors.New("unsupported function_code")
	}
	if (cfg.FunctionCode == 1 || cfg.FunctionCode == 2) && cfg.Quantity > maxBitReadCount {
		return normalizedConfig{}, fmt.Errorf("quantity exceeds max %d for bit reads", maxBitReadCount)
	}
	if (cfg.FunctionCode == 3 || cfg.FunctionCode == 4) && cfg.Quantity > maxRegisterReadCount {
		return normalizedConfig{}, fmt.Errorf("quantity exceeds max %d for register reads", maxRegisterReadCount)
	}
	if _, err := humanAddressToDevice(cfg.StartAddress); err != nil {
		return normalizedConfig{}, err
	}
	if cfg.StartAddress+cfg.Quantity-1 > math.MaxUint16 {
		return normalizedConfig{}, errors.New("address range out of bounds")
	}

	return normalizedConfig{
		Target:       cfg.Target,
		UnitID:       uint8(cfg.UnitID),
		Timeout:      time.Duration(cfg.TimeoutMS) * time.Millisecond,
		FunctionCode: uint8(cfg.FunctionCode),
		StartAddress: cfg.StartAddress,
		Quantity:     cfg.Quantity,
	}, nil
}

func humanAddressToDevice(address int) (uint16, error) {
	if address < 0 || address > math.MaxUint16 {
		return 0, errors.New("address out of range")
	}
	return uint16(address), nil
}

func buildReadPayload(startAddr, qty uint16) []byte {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint16(payload[0:2], startAddr)
	binary.BigEndian.PutUint16(payload[2:4], qty)
	return payload
}

func (r *tcpRequester) Do(cfg normalizedConfig, function uint8, payload []byte) (*modbusprotocol.Response, error) {
	conn, err := net.DialTimeout("tcp", cfg.Target, cfg.Timeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	tcpClient := &tcptransport.Client{Conn: conn, Timeout: cfg.Timeout}
	req := &modbusprotocol.Request{
		TransactionID: uint16(r.tid.Add(1)),
		ProtocolID:    0,
		UnitID:        cfg.UnitID,
		Function:      modbusprotocol.FunctionCode(function),
		Payload:       payload,
	}
	rawReq := req.EncodeTCP()
	rawResp, err := tcpClient.Send(rawReq)
	if err != nil {
		return nil, err
	}
	return modbusprotocol.DecodeTCP(rawResp)
}

func (a *appState) setReadOutcome(err error) {
	now := time.Now().UTC()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.lastReadTime = &now
	if err != nil {
		msg := err.Error()
		if len(msg) > maxStoredErrorLength {
			msg = msg[:maxStoredErrorLength]
		}
		a.lastError = msg
		return
	}
	a.lastError = ""
}

func (a *appState) mergeRows(rows []rowResult) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, row := range rows {
		a.rows[row.Address] = row
	}
}

func (a *appState) replaceRows(rows []rowResult) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.rows = make(map[int]rowResult, len(rows))
	for _, row := range rows {
		a.rows[row.Address] = row
	}
}

func (a *appState) snapshotRows() []rowResult {
	a.mu.RLock()
	defer a.mu.RUnlock()
	rows := make([]rowResult, 0, len(a.rows))
	for _, row := range a.rows {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Address < rows[j].Address })
	return rows
}

func (a *appState) readRange(cfg normalizedConfig, startAddress, quantity int) ([]rowResult, error) {
	if quantity < 1 || quantity > maxQuantityForFunction(cfg.FunctionCode) {
		return nil, errors.New("invalid quantity for function code")
	}
	startDevice, err := humanAddressToDevice(startAddress)
	if err != nil {
		return nil, err
	}
	resp, err := a.requester.Do(cfg, cfg.FunctionCode, buildReadPayload(startDevice, uint16(quantity)))
	if err != nil {
		a.setReadOutcome(err)
		return nil, err
	}

	timestamp := time.Now().UTC().Format(time.RFC3339Nano)
	rows := make([]rowResult, 0, quantity)
	if resp.Exception != nil {
		exCode := fmt.Sprintf("0x%02X", uint8(*resp.Exception))
		for i := 0; i < quantity; i++ {
			rows = append(rows, rowResult{
				Address:   startAddress + i,
				Exception: exCode,
				Timestamp: timestamp,
			})
		}
		a.setReadOutcome(nil)
		return rows, nil
	}

	decoded, err := decodeReadPayload(cfg.FunctionCode, resp.Payload, quantity)
	if err != nil {
		a.setReadOutcome(err)
		return nil, err
	}
	for i, value := range decoded {
		rows = append(rows, rowResult{
			Address:   startAddress + i,
			ValueHex:  fmt.Sprintf("0x%04X", value),
			ValueDec:  int(value),
			Timestamp: timestamp,
		})
	}
	a.setReadOutcome(nil)
	return rows, nil
}

func decodeReadPayload(functionCode uint8, payload []byte, quantity int) ([]uint16, error) {
	if quantity < 1 || quantity > maxQuantityForFunction(functionCode) {
		return nil, errors.New("invalid quantity for function code")
	}
	if len(payload) < 1 {
		return nil, errors.New("malformed payload")
	}
	byteCount := int(payload[0])
	if len(payload[1:]) < byteCount {
		return nil, errors.New("short payload")
	}

	data := payload[1 : 1+byteCount]
	values := make([]uint16, 0, quantity)

	switch functionCode {
	case 1, 2:
		for i := 0; i < quantity; i++ {
			byteIdx := i / 8
			bitIdx := uint(i % 8)
			if byteIdx >= len(data) {
				return nil, errors.New("bit payload too short")
			}
			if data[byteIdx]&(1<<bitIdx) != 0 {
				values = append(values, 1)
			} else {
				values = append(values, 0)
			}
		}
	case 3, 4:
		expectedBytes := quantity * 2
		if len(data) < expectedBytes {
			return nil, errors.New("register payload too short")
		}
		for i := 0; i < quantity; i++ {
			start := i * 2
			values = append(values, binary.BigEndian.Uint16(data[start:start+2]))
		}
	default:
		return nil, errors.New("unsupported function code")
	}
	return values, nil
}

func maxQuantityForFunction(functionCode uint8) int {
	if functionCode == 1 || functionCode == 2 {
		return maxBitReadCount
	}
	return maxRegisterReadCount
}

func (a *appState) readSingle(cfg normalizedConfig, address int) (rowResult, error) {
	rows, err := a.readRange(cfg, address, 1)
	if err != nil {
		return rowResult{}, err
	}
	if len(rows) != 1 {
		return rowResult{}, errors.New("single read returned unexpected row count")
	}
	a.mergeRows(rows)
	return rows[0], nil
}

func (a *appState) writeSingle(cfg normalizedConfig, address int, value int) (rowResult, error) {
	if a.poller.IsActive() {
		return rowResult{}, errPollingActive
	}
	if cfg.FunctionCode != 1 && cfg.FunctionCode != 3 {
		return rowResult{}, errors.New("write is only allowed for function codes 01 and 03")
	}
	if _, err := humanAddressToDevice(address); err != nil {
		return rowResult{}, err
	}
	if value < 0 || value > math.MaxUint16 {
		return rowResult{}, errors.New("write value out of range")
	}

	deviceAddr, _ := humanAddressToDevice(address)
	var payload []byte
	var function uint8
	if cfg.FunctionCode == 1 {
		function = 5
		payload = make([]byte, 4)
		binary.BigEndian.PutUint16(payload[0:2], deviceAddr)
		if value != 0 {
			binary.BigEndian.PutUint16(payload[2:4], 0xFF00)
		} else {
			binary.BigEndian.PutUint16(payload[2:4], 0x0000)
		}
	} else {
		function = 6
		payload = make([]byte, 4)
		binary.BigEndian.PutUint16(payload[0:2], deviceAddr)
		binary.BigEndian.PutUint16(payload[2:4], uint16(value))
	}

	resp, err := a.requester.Do(cfg, function, payload)
	a.setReadOutcome(err)
	if err != nil {
		return rowResult{}, err
	}
	if resp.Exception != nil {
		return rowResult{}, fmt.Errorf("modbus exception 0x%02X", uint8(*resp.Exception))
	}
	row, err := a.readSingle(cfg, address)
	if err != nil {
		return rowResult{}, err
	}
	return row, nil
}

func (p *poller) IsActive() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.active
}

func (p *poller) Snapshot() (bool, int) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.active, p.intervalMS
}

func (p *poller) Start(owner *appState, cfg normalizedConfig, intervalMS int) error {
	if intervalMS < 1 || intervalMS > maxPollIntervalMS {
		return fmt.Errorf("interval_ms must be between 1 and %d", maxPollIntervalMS)
	}
	p.Stop()

	stop := make(chan struct{})
	done := make(chan struct{})

	p.mu.Lock()
	p.active = true
	p.intervalMS = intervalMS
	p.cfg = cfg
	p.stopCh = stop
	p.doneCh = done
	p.mu.Unlock()

	go func() {
		defer close(done)
		runOnce := func() {
			rows, err := owner.readRange(cfg, cfg.StartAddress, cfg.Quantity)
			if err != nil {
				return
			}
			owner.replaceRows(rows)
		}
		runOnce()
		ticker := time.NewTicker(time.Duration(intervalMS) * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				runOnce()
			case <-stop:
				return
			}
		}
	}()
	return nil
}

func (p *poller) Stop() {
	p.mu.Lock()
	if !p.active {
		p.mu.Unlock()
		return
	}
	stop := p.stopCh
	done := p.doneCh
	p.active = false
	p.intervalMS = 0
	p.stopCh = nil
	p.doneCh = nil
	p.mu.Unlock()

	close(stop)
	<-done
}

func jsonWrite(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	defer r.Body.Close()
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBodySize))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		jsonWrite(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON payload"})
		return false
	}
	if err := dec.Decode(&struct{}{}); err == nil {
		jsonWrite(w, http.StatusBadRequest, map[string]string{"error": "payload must contain a single JSON object"})
		return false
	}
	return true
}

func methodGuard(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method == method {
		return true
	}
	jsonWrite(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	return false
}

func (a *appState) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/read/bulk", a.handleBulkRead)
	mux.HandleFunc("/api/read/single", a.handleSingleRead)
	mux.HandleFunc("/api/write/single", a.handleWriteSingle)
	mux.HandleFunc("/api/test", a.handleTest)
	mux.HandleFunc("/api/polling/start", a.handlePollingStart)
	mux.HandleFunc("/api/polling/stop", a.handlePollingStop)
	mux.HandleFunc("/api/status", a.handleStatus)
	mux.Handle("/", http.FileServer(http.Dir("web")))
	return mux
}

func normalizeTCPTestTarget(target string) (string, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		target = defaultTarget
	}
	if host, port, err := net.SplitHostPort(target); err == nil {
		if strings.TrimSpace(host) == "" {
			return "", errors.New("invalid target: host cannot be empty")
		}
		portNum, convErr := strconv.Atoi(port)
		if convErr != nil || portNum < 1 || portNum > 65535 {
			return "", errors.New("invalid target: port must be between 1 and 65535")
		}
		return target, nil
	}
	defaultPortNum, convErr := strconv.Atoi(defaultTargetPort)
	if convErr != nil || defaultPortNum < 1 || defaultPortNum > 65535 {
		return "", fmt.Errorf("default target port %q is misconfigured", defaultTargetPort)
	}
	if strings.Contains(target, ":") {
		// Allow raw IPv6 address without port; bracket + append default port.
		if ip := net.ParseIP(target); ip != nil && ip.To4() == nil {
			return net.JoinHostPort(target, defaultTargetPort), nil
		}
		return "", errors.New("invalid target: expected host:port or valid IPv6 address")
	}
	// Hostname or IPv4 without port; append default Modbus port.
	return net.JoinHostPort(target, defaultTargetPort), nil
}

func normalizeICMPHost(target string) (string, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		target = defaultTarget
	}
	if host, port, err := net.SplitHostPort(target); err == nil {
		if strings.TrimSpace(host) == "" {
			return "", errors.New("invalid target: host cannot be empty")
		}
		if port != "" {
			portNum, convErr := strconv.Atoi(port)
			if convErr != nil || portNum < 1 || portNum > 65535 {
				return "", errors.New("invalid target: port must be between 1 and 65535")
			}
		}
		return host, nil
	}
	if strings.HasPrefix(target, "[") && strings.HasSuffix(target, "]") {
		target = strings.TrimPrefix(strings.TrimSuffix(target, "]"), "[")
	}
	if strings.Contains(target, ":") {
		if ip := net.ParseIP(target); ip != nil && ip.To4() == nil {
			return target, nil
		}
		return "", errors.New("invalid target: expected host, host:port, or valid IPv6 address")
	}
	return target, nil
}

func TCPConnectTest(target string, timeout time.Duration) (duration time.Duration, err error) {
	start := time.Now()
	conn, err := net.DialTimeout("tcp", target, timeout)
	if err != nil {
		return 0, err
	}
	_ = conn.Close()
	return time.Since(start), nil
}

var icmpLatencyRe = regexp.MustCompile(`time[=<]\s*([0-9]*\.?[0-9]+)\s*ms`)
var icmpLatencyWindowsRe = regexp.MustCompile(`Average\s*=\s*([0-9]*\.?[0-9]+)ms`)

func ICMPPing(host string, timeout time.Duration) (duration time.Duration, err error) {
	seconds := int(timeout / time.Second)
	if timeout%time.Second != 0 {
		seconds++
	}
	if seconds < 1 {
		seconds = 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout+500*time.Millisecond)
	defer cancel()
	start := time.Now()
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		timeoutMS := int(timeout / time.Millisecond)
		if timeoutMS < 1 {
			timeoutMS = 1
		}
		cmd = exec.CommandContext(ctx, "ping", "-n", "1", "-w", strconv.Itoa(timeoutMS), host)
	} else {
		cmd = exec.CommandContext(ctx, "ping", "-c", "1", "-W", strconv.Itoa(seconds), host)
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		msg := strings.TrimSpace(string(output))
		if msg == "" {
			msg = err.Error()
		}
		return 0, errors.New(msg)
	}
	match := icmpLatencyRe.FindStringSubmatch(string(output))
	if len(match) > 1 {
		ms, parseErr := strconv.ParseFloat(match[1], 64)
		if parseErr == nil {
			return time.Duration(ms * float64(time.Millisecond)), nil
		}
	}
	windowsMatch := icmpLatencyWindowsRe.FindStringSubmatch(string(output))
	if len(windowsMatch) > 1 {
		ms, parseErr := strconv.ParseFloat(windowsMatch[1], 64)
		if parseErr == nil {
			return time.Duration(ms * float64(time.Millisecond)), nil
		}
	}
	return time.Since(start), nil
}

var tcpConnectTestFn = TCPConnectTest
var icmpPingFn = ICMPPing

func RunTest(testType TestType, target string) (result TestResult) {
	return runTestWithTimeout(testType, target, time.Duration(defaultTestTimeoutMS)*time.Millisecond)
}

func runTestWithTimeout(testType TestType, target string, timeout time.Duration) (result TestResult) {
	result.Type = string(testType)
	switch testType {
	case TestTCP:
		normalizedTarget, err := normalizeTCPTestTarget(target)
		if err != nil {
			result.Error = err.Error()
			return result
		}
		result.Target = normalizedTarget
		latency, testErr := tcpConnectTestFn(normalizedTarget, timeout)
		result.Success = testErr == nil
		result.Latency = latency
		if testErr != nil {
			result.Error = testErr.Error()
		}
	case TestICMP:
		host, err := normalizeICMPHost(target)
		if err != nil {
			result.Error = err.Error()
			return result
		}
		result.Target = host
		latency, testErr := icmpPingFn(host, timeout)
		result.Success = testErr == nil
		result.Latency = latency
		if testErr != nil {
			result.Error = testErr.Error()
		}
	default:
		result.Error = "unsupported test type"
	}
	return result
}

func (a *appState) handleBulkRead(w http.ResponseWriter, r *http.Request) {
	if !methodGuard(w, r, http.MethodPost) {
		return
	}
	var req bulkReadRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	cfg, err := normalizeConfig(req.Config)
	if err != nil {
		jsonWrite(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	rows, err := a.readRange(cfg, cfg.StartAddress, cfg.Quantity)
	if err != nil {
		jsonWrite(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	a.replaceRows(rows)
	jsonWrite(w, http.StatusOK, map[string]any{"rows": rows})
}

func (a *appState) handleSingleRead(w http.ResponseWriter, r *http.Request) {
	if !methodGuard(w, r, http.MethodPost) {
		return
	}
	var req singleReadRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	cfg, err := normalizeConfig(req.Config)
	if err != nil {
		jsonWrite(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	row, err := a.readSingle(cfg, req.Address)
	if err != nil {
		jsonWrite(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	jsonWrite(w, http.StatusOK, map[string]any{"row": row})
}

func (a *appState) handleWriteSingle(w http.ResponseWriter, r *http.Request) {
	if !methodGuard(w, r, http.MethodPost) {
		return
	}
	var req writeRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	cfg, err := normalizeConfig(req.Config)
	if err != nil {
		jsonWrite(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	row, err := a.writeSingle(cfg, req.Address, req.Value)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, errPollingActive) {
			status = http.StatusConflict
		}
		jsonWrite(w, status, map[string]string{"error": err.Error()})
		return
	}
	jsonWrite(w, http.StatusOK, map[string]any{"row": row})
}

func (a *appState) handleTest(w http.ResponseWriter, r *http.Request) {
	if !methodGuard(w, r, http.MethodPost) {
		return
	}
	var req testRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	if req.Type != TestTCP && req.Type != TestICMP {
		jsonWrite(w, http.StatusBadRequest, map[string]string{"error": "unsupported test type"})
		return
	}
	if req.TimeoutMS == 0 {
		req.TimeoutMS = defaultTestTimeoutMS
	}
	if req.TimeoutMS < 1 || req.TimeoutMS > maxTimeoutMS {
		jsonWrite(w, http.StatusBadRequest, map[string]string{"error": "timeout_ms out of range"})
		return
	}
	result := runTestWithTimeout(req.Type, req.Target, time.Duration(req.TimeoutMS)*time.Millisecond)
	if result.Error != "" {
		if strings.HasPrefix(result.Error, "invalid target:") {
			jsonWrite(w, http.StatusBadRequest, map[string]string{"error": result.Error})
			return
		}
	}
	jsonWrite(w, http.StatusOK, map[string]any{
		"type":       result.Type,
		"target":     result.Target,
		"success":    result.Success,
		"latency_ms": result.Latency.Milliseconds(),
		"error":      result.Error,
	})
}

func (a *appState) handlePollingStart(w http.ResponseWriter, r *http.Request) {
	if !methodGuard(w, r, http.MethodPost) {
		return
	}
	var req pollingStartRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	cfg, err := normalizeConfig(req.Config)
	if err != nil {
		jsonWrite(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if req.IntervalMS == 0 {
		req.IntervalMS = defaultPollingMS
	}
	if err := a.poller.Start(a, cfg, req.IntervalMS); err != nil {
		jsonWrite(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	jsonWrite(w, http.StatusOK, map[string]any{"polling_active": true, "interval_ms": req.IntervalMS})
}

func (a *appState) handlePollingStop(w http.ResponseWriter, r *http.Request) {
	if !methodGuard(w, r, http.MethodPost) {
		return
	}
	a.poller.Stop()
	jsonWrite(w, http.StatusOK, map[string]any{"polling_active": false})
}

func (a *appState) handleStatus(w http.ResponseWriter, r *http.Request) {
	if !methodGuard(w, r, http.MethodGet) {
		return
	}
	a.mu.RLock()
	lastErr := a.lastError
	var lastRead string
	if a.lastReadTime != nil {
		lastRead = a.lastReadTime.Format(time.RFC3339Nano)
	}
	a.mu.RUnlock()

	pollingActive, intervalMS := a.poller.Snapshot()
	jsonWrite(w, http.StatusOK, map[string]any{
		"connected":           lastErr == "",
		"last_error":          lastErr,
		"last_read_timestamp": lastRead,
		"polling_active":      pollingActive,
		"polling_interval_ms": intervalMS,
		"rows":                a.snapshotRows(),
	})
}

func main() {
	bindAddr := os.Getenv("BIND_ADDR")
	if bindAddr == "" {
		bindAddr = defaultBindAddr
	}

	app := newApp(&tcpRequester{})
	if cwd, err := os.Getwd(); err == nil {
		log.Printf("serving web assets from %s", filepath.Join(cwd, "web"))
	}
	log.Printf("ModProbe listening on http://%s", bindAddr)
	if err := http.ListenAndServe(bindAddr, app.routes()); err != nil {
		log.Fatal(err)
	}
}
