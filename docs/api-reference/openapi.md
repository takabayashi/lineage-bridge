# Interactive API Explorer

!!! success "Best Way to Explore the API"
    **Start the API server and visit [http://localhost:8000/docs](http://localhost:8000/docs)** for the best interactive experience with Swagger UI!
    
    ```bash
    make api
    # Then open http://localhost:8000/docs in your browser
    ```

## Built-in Interactive Documentation

LineageBridge includes three built-in API documentation interfaces:

### Swagger UI (Recommended)

**URL**: [http://localhost:8000/docs](http://localhost:8000/docs)

Full-featured interactive API explorer with:

- ✅ Test all endpoints directly in your browser
- ✅ Auto-populated request bodies with examples
- ✅ One-click authentication
- ✅ Response visualization
- ✅ Download responses as JSON

**How to use:**

1. Start the API server:
   ```bash
   make api
   ```

2. Open [http://localhost:8000/docs](http://localhost:8000/docs) in your browser

3. Click **"Authorize"** if using API key authentication

4. Expand any endpoint and click **"Try it out"** to test

### ReDoc

**URL**: [http://localhost:8000/redoc](http://localhost:8000/redoc)

Clean, modern API reference documentation:

- Hierarchical navigation
- Search functionality
- Code samples in multiple languages
- Markdown-rendered descriptions

### OpenAPI JSON

**URL**: [http://localhost:8000/openapi.json](http://localhost:8000/openapi.json)

Machine-readable OpenAPI 3.1 specification for:

- API client generation
- Testing automation
- Integration with API tools
- Schema validation

## Download OpenAPI Spec

Download the full OpenAPI specification for offline use or code generation:

```bash
# From running API server
curl http://localhost:8000/openapi.json -o openapi.yaml

# Or from repository (always latest)
curl https://raw.githubusercontent.com/takabayashi/lineage-bridge/main/docs/openapi.yaml -o openapi.yaml
```

Or download directly: [openapi.yaml](https://raw.githubusercontent.com/takabayashi/lineage-bridge/main/docs/openapi.yaml)

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
