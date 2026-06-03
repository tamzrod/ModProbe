# ModProbe Manual

## Run

```bash
go run .
```

Server binds to `localhost:8080` by default. Override with `BIND_ADDR`, for example:

```bash
BIND_ADDR=127.0.0.1:9090 go run .
```

## Open the web UI

1. Start ModProbe with `go run .`.
2. Open `http://localhost:8080`.
3. Configure connection and range in the **Config Panel**.

![ModProbe basic mode UI](https://github.com/user-attachments/assets/412b1272-15e4-4fe9-a77b-92763d06e29b)

## Basic mode workflow

1. Set **IP:Port**, **Unit ID**, **Timeout ms**, **Function Code**, **Start Address**, and **Quantity**.
   - Addressing is zero-based (`0` to `65535`).
2. Click **READ ALL** to read the full range.
3. Use per-row **READ** for a single-address refresh.
4. For FC 01 and FC 03, edit **Value (Dec)** then click per-row **WRITE**.
5. Click **Test** beside **IP:Port** to open **Test Connection** and run either:
   - **Run TCP Test**
   - **Run ICMP Test**
   - The modal keeps the last **4 test samples** with clear pass/fail indicators.
6. Enable **Poll Enable** to start backend polling. While polling is active:
   - table edits are read-only
   - WRITE is disabled
   - per-row READ remains available

## API

- `POST /api/read/bulk`
- `POST /api/read/single`
- `POST /api/write/single`
- `POST /api/test`
- `POST /api/polling/start`
- `POST /api/polling/stop`
- `GET /api/status`

### Common payload config

```json
{
  "config": {
    "target": "127.0.0.1:502",
    "unit_id": 1,
    "timeout_ms": 500,
    "function_code": 3,
    "start_address": 0,
    "quantity": 10
  }
}
```

- `/api/read/single` adds `address`
- `/api/write/single` adds `address` and `value`
- `/api/test` body:

```json
{
  "type": "tcp",
  "target": "127.0.0.1:502",
  "timeout_ms": 2500
}
```

- `/api/test` response:

```json
{
  "type": "tcp",
  "target": "127.0.0.1:502",
  "success": true,
  "latency_ms": 12,
  "error": ""
}
```

- `/api/polling/start` adds `interval_ms`

## Notes

- Only `github.com/tamzrod/modbus` is used for Modbus transport/protocol.
- No retry logic is implemented.
- Modbus exception codes are preserved in table responses and exposed in the Exception cell tooltip.
