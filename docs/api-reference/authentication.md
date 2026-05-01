# Authentication

The LineageBridge API supports optional API key authentication via the `X-API-Key` header.

## Configuration

Authentication is **disabled by default**. To enable it, set the `LINEAGE_BRIDGE_API_KEY` environment variable:

```bash
export LINEAGE_BRIDGE_API_KEY="your-secret-key-here"
```

When this variable is set, all endpoints except `/api/v1/health` require authentication.

## Using API Keys

### cURL

Pass the API key in the `X-API-Key` header:

```bash
curl -H "X-API-Key: your-secret-key-here" \
  http://localhost:8000/api/v1/lineage/events
```

### Python (httpx)

```python
import httpx

client = httpx.Client(
    base_url="http://localhost:8000/api/v1",
    headers={"X-API-Key": "your-secret-key-here"}
)

response = client.get("/lineage/events")
print(response.json())
```

### Python (requests)

```python
import requests

headers = {"X-API-Key": "your-secret-key-here"}
response = requests.get(
    "http://localhost:8000/api/v1/lineage/events",
    headers=headers
)
print(response.json())
```

### JavaScript (fetch)

```javascript
const response = await fetch('http://localhost:8000/api/v1/lineage/events', {
  headers: {
    'X-API-Key': 'your-secret-key-here'
  }
});
const data = await response.json();
```

## Generating Secure Keys

Use a cryptographically secure random string for production:

```bash
# Linux/macOS
openssl rand -hex 32

# Python
python -c "import secrets; print(secrets.token_hex(32))"
```

## Unauthenticated Endpoints

The following endpoint is always accessible without authentication:

- `GET /api/v1/health` - Health check for monitoring

## Error Responses

### Missing API Key

When authentication is enabled and no API key is provided:

```json
{
  "detail": "Missing API Key"
}
```

HTTP Status: `403 Forbidden`

### Invalid API Key

When an incorrect API key is provided:

```json
{
  "detail": "Invalid API Key"
}
```

HTTP Status: `403 Forbidden`

## Security Best Practices

1. **Use environment variables** - Never hardcode API keys in source code
2. **Use HTTPS in production** - Always run behind a reverse proxy (nginx, Caddy) with TLS
3. **Rotate keys regularly** - Update keys periodically and when team members leave
4. **Use strong keys** - Generate keys with at least 32 bytes of entropy
5. **Restrict network access** - Use firewall rules to limit API access to trusted networks

## Production Deployment

Example nginx reverse proxy configuration with TLS:

```nginx
server {
    listen 443 ssl http2;
    server_name lineage-api.example.com;

    ssl_certificate /etc/letsencrypt/live/lineage-api.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/lineage-api.example.com/privkey.pem;

    location / {
        proxy_pass http://localhost:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Example Caddy configuration:

```
lineage-api.example.com {
    reverse_proxy localhost:8000
}
```

## Docker Deployment

When running in Docker, pass the API key via environment variable:

```bash
docker run -d \
  -e LINEAGE_BRIDGE_API_KEY="your-secret-key-here" \
  -p 8000:8000 \
  lineage-bridge:latest
```

Or use Docker Compose:

```yaml
version: '3.8'
services:
  api:
    image: lineage-bridge:latest
    ports:
      - "8000:8000"
    environment:
      LINEAGE_BRIDGE_API_KEY: ${LINEAGE_BRIDGE_API_KEY}
```

Then start with:

```bash
LINEAGE_BRIDGE_API_KEY="your-secret-key-here" docker-compose up -d
```

## Testing Authentication

Test that authentication is working:

```bash
# Should fail (no key)
curl http://localhost:8000/api/v1/lineage/events
# {"detail": "Missing API Key"}

# Should succeed (health check, no auth required)
curl http://localhost:8000/api/v1/health
# {"status": "ok"}

# Should succeed (correct key)
curl -H "X-API-Key: your-secret-key-here" \
  http://localhost:8000/api/v1/lineage/events
# [...]
```

## Future Enhancements

Planned authentication features:

- OAuth2/OIDC integration
- Role-based access control (RBAC)
- API key scopes (read-only, write, admin)
- Rate limiting per key
- Key expiration and rotation

See the [roadmap](../reference/roadmap.md) for more details.
