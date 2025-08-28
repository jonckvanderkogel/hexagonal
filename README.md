# Hexagonal architecture sample app

### Dependencies
* Java 24
* Maven

### Running the application
```bash
docker compose up -d
```

```bash
mvn clean -f application spring-boot:run
```

#### Calling endpoints:

Create author:
```bash
curl -i -X POST http://localhost:8080/authors \
  -H "Content-Type: application/json" \
  -d '{"name": "Douglas Adams"}'
```

Expected response:
```
HTTP/1.1 201 Created
Content-Type: application/json

{
  "id": "1a2b3c4d-5678-90ab-cdef-1234567890ab",
  "name": "Douglas Adams"
}
```

Passing an invalid name:
```bash
curl -i -X POST http://localhost:8080/authors \
  -H "Content-Type: application/json" \
  -d '{"name": ""}'
```

Expected response:
```
HTTP/1.1 400 Bad Request
Content-Type: application/json

{
  "error": "Name is required"
}
```

Fetch author by id:
```bash
curl -i http://localhost:8080/authors/1a2b3c4d-5678-90ab-cdef-1234567890ab
```

Expected response:
```
HTTP/1.1 200 OK
Content-Type: application/json

{
  "id": "1a2b3c4d-5678-90ab-cdef-1234567890ab",
  "name": "Douglas Adams"
}
```

If the id doesn't exist:
```
HTTP/1.1 404 Not Found
Content-Type: application/json

{
  "error": "Author 1a2b3c4d-5678-90ab-cdef-1234567890ab not found"
}
```

If a persistence problem occurs (e.g. DB is down):
```
HTTP/1.1 500 Internal Server Error
Content-Type: application/json

{
  "error": "Database unavailable"
}
```