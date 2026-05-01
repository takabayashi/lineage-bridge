# Interactive API Explorer

Explore the LineageBridge API interactively using the Scalar API reference below.

## Live Documentation

<div id="api-reference"></div>

<link
  id="scalar-api-reference"
  rel="stylesheet"
  href="https://cdn.jsdelivr.net/npm/@scalar/api-reference/dist/style.css" />

<script
  id="api-reference"
  type="application/json"
  data-configuration='{"spec": {"url": "https://raw.githubusercontent.com/takabayashi/lineage-bridge/main/docs/openapi.yaml"}}'>
</script>

<script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>

## Alternative Tools

### Swagger UI

The API server includes a built-in Swagger UI available at:

```
http://localhost:8000/docs
```

Start the API server and open this URL in your browser to explore and test endpoints interactively.

### Redoc

Alternative OpenAPI documentation viewer available at:

```
http://localhost:8000/redoc
```

### Download OpenAPI Spec

Download the full OpenAPI 3.1 specification:

```bash
# Via API
curl http://localhost:8000/api/v1/openapi.yaml -o openapi.yaml

# From repository
curl https://raw.githubusercontent.com/takabayashi/lineage-bridge/main/docs/openapi.yaml -o openapi.yaml
```

## Local Development

When developing locally, you can point the Scalar viewer to your local API:

1. Start the API server:
   ```bash
   uv run lineage-bridge-api
   ```

2. Access the Swagger UI at `http://localhost:8000/docs`

3. Download the spec for offline use:
   ```bash
   curl http://localhost:8000/api/v1/openapi.yaml > openapi-local.yaml
   ```

## Integration Examples

### Postman

Import the OpenAPI spec into Postman:

1. Download the spec: `curl http://localhost:8000/api/v1/openapi.yaml -o openapi.yaml`
2. In Postman: **Import** > **File** > Select `openapi.yaml`
3. Collections and environments will be auto-generated

### Insomnia

Import into Insomnia:

1. Download the spec
2. **Import/Export** > **Import Data** > **From File**
3. Select `openapi.yaml`

### HTTPie Desktop

Use HTTPie Desktop for a beautiful API testing experience:

1. Download HTTPie Desktop from https://httpie.io/desktop
2. Import the OpenAPI spec
3. Test endpoints with syntax highlighting and auto-completion

## API Clients

Generate type-safe API clients from the OpenAPI spec:

### Python

```bash
# Using openapi-python-client
pip install openapi-python-client
openapi-python-client generate --url http://localhost:8000/api/v1/openapi.yaml
```

### TypeScript

```bash
# Using openapi-typescript
npm install openapi-typescript --save-dev
npx openapi-typescript http://localhost:8000/api/v1/openapi.yaml -o lineage-bridge.d.ts
```

### Go

```bash
# Using oapi-codegen
go install github.com/deepmap/oapi-codegen/cmd/oapi-codegen@latest
oapi-codegen -generate types,client -package lineagebridge openapi.yaml > client.go
```

### Java

```bash
# Using OpenAPI Generator
openapi-generator-cli generate \
  -i openapi.yaml \
  -g java \
  -o ./java-client \
  --additional-properties=library=okhttp-gson
```

## Schema Validation

Validate the OpenAPI spec:

```bash
# Using openapi-spec-validator
pip install openapi-spec-validator
openapi-spec-validator openapi.yaml
```

## Further Reading

- [OpenAPI Specification 3.1](https://spec.openapis.org/oas/v3.1.0)
- [Scalar Documentation](https://github.com/scalar/scalar)
- [OpenLineage Spec](https://openlineage.io/spec/2-0-2/OpenLineage.json)
