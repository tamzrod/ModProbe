# ModProbe Architecture

## Overview

ModProbe is a single Go service with one binary (`main.go`) that:

- establishes a Modbus TCP client (`goburrow/modbus`)
- serves a static web UI from `/web`
- exposes JSON/YAML HTTP endpoints under `/api/*`
- keeps runtime cache/profile/status in an in-memory shared state

## Runtime Components

### HTTP layer

The `state.routes()` router in `main.go` wires handlers for:

- basic operations (`/api/read`, `/api/write`, `/api/status`)
- profile lifecycle (`/api/profile`, `/api/profile/import`, `/api/profile/export`)
- advanced parsing/scanning (`/api/parse`, `/api/scan`)
- static web assets (`/` served from `web/`)

### State and synchronization

The `state` struct is the central runtime store:

- `client`: Modbus backend through `mbClient` interface
- `cache`: last read registers per address
- `prof` and `profRaw`: parsed and raw YAML profile
- `lastPollTime` and `lastErr`: connection/status tracking

A `sync.RWMutex` protects shared mutable data accessed by concurrent requests.

### Modbus abstraction

The `mbClient` interface isolates transport calls.

- `goburrowClient` is the production adapter.
- `simClient` is fallback behavior when live Modbus connection fails.

This separation also enables deterministic unit tests with a mock client.

### Profile model and parsing

Imported YAML profiles are unmarshaled into `profile` and `registerDef`.

`parseByType` interprets cached register values using profile metadata:

- numeric/default register values (with scaling)
- `float32` with configurable byte order
- `bits` into named boolean flags
- `enum` into mapped labels

### Web UI

`web/index.html` provides a minimal client-side app that:

- toggles Basic/Advanced views
- calls API endpoints via `fetch`
- renders responses in JSON/text panels

## Request Flow (high level)

1. Browser action or API call hits handler.
2. Handler validates input.
3. Handler reads/writes through `mbClient`.
4. Results update in-memory cache/status/profile state as needed.
5. Response is returned as JSON (or YAML/profile download for profile endpoints).

## Testing Strategy

`main_test.go` uses `httptest` plus a `mockClient` to validate:

- basic read/write/status behavior
- profile import/export/parse/scan behavior
- register parsing rules in `parseByType`
