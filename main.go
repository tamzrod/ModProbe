package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/goburrow/modbus"
	"gopkg.in/yaml.v3"
)

type registerDef struct {
	Address   int            `yaml:"address" json:"address"`
	Name      string         `yaml:"name" json:"name"`
	Type      string         `yaml:"type" json:"type"`
	ByteOrder string         `yaml:"byte_order" json:"byte_order,omitempty"`
	Scale     float64        `yaml:"scale" json:"scale,omitempty"`
	Unit      string         `yaml:"unit" json:"unit,omitempty"`
	Bits      map[int]string `yaml:"bits" json:"bits,omitempty"`
	Mapping   map[int]string `yaml:"mapping" json:"mapping,omitempty"`
}

type profile struct {
	Device struct {
		Name       string `yaml:"name" json:"name"`
		Connection string `yaml:"connection" json:"connection"`
		Timeout    string `yaml:"timeout" json:"timeout"`
		Protocol   string `yaml:"protocol" json:"protocol"`
	} `yaml:"device" json:"device"`
	Registers []registerDef `yaml:"registers" json:"registers"`
}

type mbClient interface {
	ReadHoldingRegisters(address, quantity uint16) ([]byte, error)
	WriteSingleRegister(address, value uint16) ([]byte, error)
	WriteMultipleRegisters(address, quantity uint16, value []byte) ([]byte, error)
}

type goburrowClient struct {
	client modbus.Client
}

func (g *goburrowClient) ReadHoldingRegisters(address, quantity uint16) ([]byte, error) {
	return g.client.ReadHoldingRegisters(address, quantity)
}

func (g *goburrowClient) WriteSingleRegister(address, value uint16) ([]byte, error) {
	return g.client.WriteSingleRegister(address, value)
}

func (g *goburrowClient) WriteMultipleRegisters(address, quantity uint16, value []byte) ([]byte, error) {
	return g.client.WriteMultipleRegisters(address, quantity, value)
}

type state struct {
	mu           sync.RWMutex
	client       mbClient
	lastPollTime *time.Time
	lastErr      string
	cache        map[int][]uint16
	prof         *profile
	profRaw      []byte
}

// maxProfileSize bounds profile uploads to prevent excessive memory use.
const maxProfileSize int64 = 2 * 1024 * 1024
const defaultModbusAddr = "127.0.0.1:502"
const defaultBindAddr = "localhost:8080"
const defaultModbusTimeout = 500 * time.Millisecond

func newState(client mbClient) *state {
	return &state{client: client, cache: map[int][]uint16{}}
}

func (s *state) setPoll(err error) {
	n := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastPollTime = &n
	if err != nil {
		s.lastErr = err.Error()
		return
	}
	s.lastErr = ""
}

func (s *state) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/read/", s.handleRead)
	mux.HandleFunc("/api/write/", s.handleWrite)
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/parse/", s.handleParse)
	mux.HandleFunc("/api/profile", s.handleProfile)
	mux.HandleFunc("/api/profile/import", s.handleImportProfile)
	mux.HandleFunc("/api/profile/export", s.handleExportProfile)
	mux.HandleFunc("/api/scan/", s.handleScan)
	mux.Handle("/", http.FileServer(http.Dir("web")))
	return mux
}

func parseAddress(path, prefix string) (int, error) {
	raw := strings.TrimPrefix(path, prefix)
	if raw == "" || strings.Contains(raw, "/") {
		return 0, errors.New("invalid address")
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, errors.New("address must be integer")
	}
	if v < 0 || v > math.MaxUint16 {
		return 0, errors.New("address out of range")
	}
	return v, nil
}

func toUint16(v int) (uint16, error) {
	if v < 0 || v > math.MaxUint16 {
		return 0, errors.New("value out of range")
	}
	return uint16(v), nil
}

func decodeRegisters(data []byte) []uint16 {
	regs := make([]uint16, 0, len(data)/2)
	for i := 0; i+1 < len(data); i += 2 {
		regs = append(regs, binary.BigEndian.Uint16(data[i:i+2]))
	}
	return regs
}

func jsonWrite(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *state) handleRead(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonWrite(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	addr, err := parseAddress(r.URL.Path, "/api/read/")
	if err != nil {
		jsonWrite(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	qty := 1
	if q := r.URL.Query().Get("quantity"); q != "" {
		qty, err = strconv.Atoi(q)
		if err != nil || qty < 1 || qty > math.MaxUint16 {
			jsonWrite(w, http.StatusBadRequest, map[string]string{"error": "invalid quantity"})
			return
		}
	}

	addr16, err := toUint16(addr)
	if err != nil {
		jsonWrite(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	qty16, err := toUint16(qty)
	if err != nil {
		jsonWrite(w, http.StatusBadRequest, map[string]string{"error": "invalid quantity"})
		return
	}
	b, err := s.client.ReadHoldingRegisters(addr16, qty16)
	s.setPoll(err)
	if err != nil {
		jsonWrite(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	regs := decodeRegisters(b)

	s.mu.Lock()
	s.cache[addr] = regs
	s.mu.Unlock()

	jsonWrite(w, http.StatusOK, map[string]any{"address": addr, "quantity": qty, "registers": regs})
}

func (s *state) handleWrite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonWrite(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	addr, err := parseAddress(r.URL.Path, "/api/write/")
	if err != nil {
		jsonWrite(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	var payload struct {
		Values []uint16 `json:"values"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		jsonWrite(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON payload"})
		return
	}
	if len(payload.Values) == 0 {
		jsonWrite(w, http.StatusBadRequest, map[string]string{"error": "values array cannot be empty"})
		return
	}

	if len(payload.Values) == 1 {
		addr16, convErr := toUint16(addr)
		if convErr != nil {
			jsonWrite(w, http.StatusBadRequest, map[string]string{"error": convErr.Error()})
			return
		}
		_, err = s.client.WriteSingleRegister(addr16, payload.Values[0])
	} else {
		buf := bytes.NewBuffer(nil)
		for _, v := range payload.Values {
			if e := binary.Write(buf, binary.BigEndian, v); e != nil {
				jsonWrite(w, http.StatusInternalServerError, map[string]string{"error": e.Error()})
				return
			}
		}
		addr16, convErr := toUint16(addr)
		if convErr != nil {
			jsonWrite(w, http.StatusBadRequest, map[string]string{"error": convErr.Error()})
			return
		}
		qty16, convErr := toUint16(len(payload.Values))
		if convErr != nil {
			jsonWrite(w, http.StatusBadRequest, map[string]string{"error": "too many values"})
			return
		}
		_, err = s.client.WriteMultipleRegisters(addr16, qty16, buf.Bytes())
	}

	s.setPoll(err)
	if err != nil {
		jsonWrite(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	jsonWrite(w, http.StatusOK, map[string]any{"address": addr, "written": payload.Values})
}

func (s *state) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonWrite(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	resp := map[string]any{"connected": s.lastErr == "", "last_error": s.lastErr}
	if s.lastPollTime != nil {
		resp["last_poll_time"] = s.lastPollTime.Format(time.RFC3339Nano)
	}
	jsonWrite(w, http.StatusOK, resp)
}

func (s *state) requireProfile() (*profile, []byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.prof == nil {
		return nil, nil, false
	}
	return s.prof, bytes.Clone(s.profRaw), true
}

func (s *state) handleProfile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonWrite(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	_, raw, ok := s.requireProfile()
	if !ok {
		jsonWrite(w, http.StatusNotFound, map[string]string{"error": "profile not loaded"})
		return
	}
	w.Header().Set("Content-Type", "application/x-yaml")
	_, _ = w.Write(raw)
}

func readProfileUpload(r *http.Request) ([]byte, error) {
	if strings.Contains(r.Header.Get("Content-Type"), "multipart/form-data") {
		if err := r.ParseMultipartForm(maxProfileSize); err != nil {
			return nil, err
		}
		for _, files := range r.MultipartForm.File {
			if len(files) > 0 {
				return readMultipartFile(files[0])
			}
		}
		return nil, errors.New("no file uploaded")
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxProfileSize+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > maxProfileSize {
		return nil, errors.New("payload too large")
	}
	return raw, nil
}

func readMultipartFile(fh *multipart.FileHeader) ([]byte, error) {
	f, err := fh.Open()
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(io.LimitReader(f, maxProfileSize))
}

func (s *state) handleImportProfile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonWrite(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	raw, err := readProfileUpload(r)
	if err != nil {
		jsonWrite(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	var p profile
	if err := yaml.Unmarshal(raw, &p); err != nil {
		jsonWrite(w, http.StatusBadRequest, map[string]string{"error": "invalid profile yaml"})
		return
	}

	s.mu.Lock()
	s.prof = &p
	s.profRaw = bytes.Clone(raw)
	s.mu.Unlock()

	jsonWrite(w, http.StatusOK, map[string]any{"status": "imported", "register_count": len(p.Registers)})
}

func (s *state) handleExportProfile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonWrite(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	_, raw, ok := s.requireProfile()
	if !ok {
		jsonWrite(w, http.StatusNotFound, map[string]string{"error": "profile not loaded"})
		return
	}
	w.Header().Set("Content-Type", "application/x-yaml")
	w.Header().Set("Content-Disposition", `attachment; filename="profile.yaml"`)
	_, _ = w.Write(raw)
}

func (s *state) handleParse(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonWrite(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	addr, err := parseAddress(r.URL.Path, "/api/parse/")
	if err != nil {
		jsonWrite(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	p, _, ok := s.requireProfile()
	if !ok {
		jsonWrite(w, http.StatusPreconditionFailed, map[string]string{"error": "profile required"})
		return
	}

	s.mu.RLock()
	regs, ok := s.cache[addr]
	s.mu.RUnlock()
	if !ok {
		jsonWrite(w, http.StatusNotFound, map[string]string{"error": "no cached response for address"})
		return
	}

	for _, reg := range p.Registers {
		if reg.Address != addr {
			continue
		}
		parsed, perr := parseByType(reg, regs)
		if perr != nil {
			jsonWrite(w, http.StatusBadRequest, map[string]string{"error": perr.Error()})
			return
		}
		jsonWrite(w, http.StatusOK, map[string]any{"address": addr, "name": reg.Name, "type": reg.Type, "value": parsed, "unit": reg.Unit})
		return
	}
	jsonWrite(w, http.StatusNotFound, map[string]string{"error": "address not found in profile"})
}

func parseByType(reg registerDef, regs []uint16) (any, error) {
	scale := reg.Scale
	if scale == 0 {
		scale = 1
	}

	switch strings.ToLower(reg.Type) {
	case "float32":
		if len(regs) < 2 {
			return nil, errors.New("float32 requires two registers")
		}
		w1, w2 := regs[0], regs[1]
		if strings.EqualFold(reg.ByteOrder, "little") {
			w1, w2 = w2, w1
		}
		b := make([]byte, 4)
		binary.BigEndian.PutUint16(b[0:2], w1)
		binary.BigEndian.PutUint16(b[2:4], w2)
		f := math.Float32frombits(binary.BigEndian.Uint32(b))
		return float64(f) * scale, nil
	case "bits":
		if len(regs) == 0 {
			return nil, errors.New("bits requires one register")
		}
		v := regs[0]
		out := map[string]bool{}
		for bit, name := range reg.Bits {
			out[name] = (v & (1 << bit)) != 0
		}
		return out, nil
	case "enum":
		if len(regs) == 0 {
			return nil, errors.New("enum requires one register")
		}
		if name, ok := reg.Mapping[int(regs[0])]; ok {
			return name, nil
		}
		return fmt.Sprintf("unknown(%d)", regs[0]), nil
	default:
		if len(regs) == 0 {
			return nil, errors.New("missing register")
		}
		return float64(regs[0]) * scale, nil
	}
}

func (s *state) handleScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonWrite(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	p := strings.TrimPrefix(r.URL.Path, "/api/scan/")
	parts := strings.Split(p, "-")
	if len(parts) != 2 {
		jsonWrite(w, http.StatusBadRequest, map[string]string{"error": "scan path must be /api/scan/{start}-{end}"})
		return
	}
	start, err1 := strconv.Atoi(parts[0])
	end, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil || start < 0 || end < start || end > math.MaxUint16 {
		jsonWrite(w, http.StatusBadRequest, map[string]string{"error": "invalid scan range"})
		return
	}
	if _, _, ok := s.requireProfile(); !ok {
		jsonWrite(w, http.StatusPreconditionFailed, map[string]string{"error": "profile required"})
		return
	}

	results := make([]map[string]any, 0, end-start+1)
	for addr := start; addr <= end; addr++ {
		addr16, convErr := toUint16(addr)
		if convErr != nil {
			results = append(results, map[string]any{"address": addr, "ok": false, "error": convErr.Error()})
			continue
		}
		b, err := s.client.ReadHoldingRegisters(addr16, 1)
		if err != nil {
			results = append(results, map[string]any{"address": addr, "ok": false, "error": err.Error()})
			continue
		}
		regs := decodeRegisters(b)
		s.mu.Lock()
		s.cache[addr] = regs
		s.mu.Unlock()
		results = append(results, map[string]any{"address": addr, "ok": true, "registers": regs})
	}
	s.setPoll(nil)
	jsonWrite(w, http.StatusOK, map[string]any{"start": start, "end": end, "results": results})
}

func newLiveClient() mbClient {
	modbusAddr := os.Getenv("MODBUS_ADDR")
	if modbusAddr == "" {
		modbusAddr = defaultModbusAddr
	}
	handler := modbus.NewTCPClientHandler(modbusAddr)
	handler.Timeout = defaultModbusTimeout
	if err := handler.Connect(); err != nil {
		log.Printf("modbus connect failed: %v", err)
		return &simClient{}
	}
	return &goburrowClient{client: modbus.NewClient(handler)}
}

type simClient struct{}

func (s *simClient) ReadHoldingRegisters(address, quantity uint16) ([]byte, error) {
	out := make([]byte, int(quantity)*2)
	for i := 0; i < int(quantity); i++ {
		binary.BigEndian.PutUint16(out[i*2:i*2+2], address+uint16(i))
	}
	return out, nil
}

func (s *simClient) WriteSingleRegister(address, value uint16) ([]byte, error) {
	return []byte{0, 0}, nil
}

func (s *simClient) WriteMultipleRegisters(address, quantity uint16, value []byte) ([]byte, error) {
	return []byte{0, 0}, nil
}

func main() {
	bindAddr := os.Getenv("BIND_ADDR")
	if bindAddr == "" {
		bindAddr = defaultBindAddr
	}

	st := newState(newLiveClient())
	if cwd, err := os.Getwd(); err == nil {
		log.Printf("serving web assets from %s", filepath.Join(cwd, "web"))
	}
	log.Printf("ModProbe listening on http://%s", bindAddr)
	if err := http.ListenAndServe(bindAddr, st.routes()); err != nil {
		log.Fatal(err)
	}
}
