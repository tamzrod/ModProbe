# ModProbe

Local Modbus diagnostic web UI with no authentication.

## Run

```bash
go run .
```

Server binds to `localhost:8080` by default. Override with `BIND_ADDR`, for example:

```bash
BIND_ADDR=127.0.0.1:9090 go run .
```

### Docker Compose

```bash
docker compose up --build
```

This starts ModProbe on `http://localhost:8080`.

## API

Basic mode:

- `GET /api/read/{address}?quantity=1`
- `POST /api/write/{address}` with JSON `{"values":[1,2]}`
- `GET /api/status`

Advanced mode (requires imported profile):

- `GET /api/parse/{address}`
- `GET /api/profile`
- `POST /api/profile/import` (YAML upload)
- `GET /api/profile/export`
- `GET /api/scan/{start}-{end}`