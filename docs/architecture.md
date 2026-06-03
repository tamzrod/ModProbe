# ModProbe Architecture

## Overview

ModProbe is a single Go service that serves a static single-page web UI and exposes JSON APIs for Modbus diagnostics in basic mode.

Core components:

- HTTP API handlers under `/api/*`
- Static frontend from `/web`
- In-memory runtime state for status and latest row values
- Background poller goroutine with ticker and `IsActive()`
- Modbus TCP request path built only with `github.com/tamzrod/modbus`

## Backend modules

### HTTP API

Routes:

- `POST /api/read/bulk`
- `POST /api/read/single`
- `POST /api/write/single`
- `POST /api/polling/start`
- `POST /api/polling/stop`
- `GET /api/status`

### Runtime state

`appState` stores:

- Modbus requester dependency
- last read timestamp
- last error / connection status
- latest table rows by address
- poller instance

### Poller

`poller` manages lifecycle and interval polling:

- `Start(...)` spawns ticker goroutine
- `Stop()` terminates it cleanly
- `IsActive()` reports write lock state

Write requests are rejected while polling is active.

### Modbus transport/protocol

`tcpRequester`:

- opens TCP connection per request with configured timeout
- encodes request with `protocol.Request`
- sends with `transport/tcp.Client`
- decodes response with `protocol.DecodeTCP`

No retries are used.

## Frontend behavior

The web app includes:

- Config panel
- Polling controls
- Output table with per-row READ/WRITE actions
- Status bar

Rules enforced:

- per-row READ always enabled (including during polling)
- WRITE shown only for FC01/FC03
- WRITE blocked during polling
- FC02/FC04 rows are read-only
- edited values highlight until written
- exception name is displayed per row, with the raw exception code in the cell tooltip
