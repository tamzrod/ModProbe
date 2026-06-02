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

## How-to

### Open the web UI

1. Start ModProbe with `go run .` (or Docker Compose).
2. Open `http://localhost:8080` in your browser.

![ModProbe web UI](https://github.com/user-attachments/assets/2cc8d5af-0c38-4cda-a969-eb78a9acdb2b)

### Basic mode

1. Leave **Mode** set to **Basic**.
2. Use **Read** with:
   - **Address**: register address
   - **Quantity**: number of registers
3. Use **Write** with:
   - **Address**: register address
   - **Comma-separated values**: values to write
4. Click **Refresh Status** to check connectivity and current mode.

### Advanced mode

1. Switch **Mode** to **Advanced**.
2. Import a YAML profile with **Import Profile**.
3. Use **Parse Cached** to decode values using the imported profile.
4. Use **Scan** to scan an address range.
5. Use **Get Profile** or **Export Profile** as needed.

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