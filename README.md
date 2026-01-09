# Hexagonal architecture sample app

### Dependencies
* Java 25
* Maven

### Running the application
```bash
docker compose up -d
```

```bash
mvn clean -f application spring-boot:run
```

### Running tests
The application has a wide array of both unit tests and integration tests. The integration tests are based on 
docker-compose, using the spring-boot-docker-compose library to spin up the environment before the integration test
runs.

#### Running tests from the command line
To run from the command line is easy, simply run this command from your shell:
```shell
mvn clean verify
```

#### Running tests from IntelliJ
Since this is a multi-module project, you have to set up IntelliJ specifically so that it can run all your tests in one
go. You can run individual tests or tests from one module like you're used to, but running them all in one go requires
you to set up a run configuration. The most important thing is that since this project depends on docker compose to
scaffold the environment, you need to pass in the location of your docker compose file as a VM argument:
```
-Dspring.docker.compose.file=$PROJECT_DIR$/docker-compose.yml
```

For the rest it's very simple, the only thing you need to add is your working directory, which is just the complete path
to the root of where you checked out the project.

### Calling endpoints:

#### Create author:
```bash
curl -i -X POST http://localhost:8080/authors \
  -H "Content-Type: application/json" \
  -d '{
        "firstName": "Douglas",
        "lastName": "Adams"
      }'
```

Expected response:
```
HTTP/1.1 201 Created
Content-Type: application/json

{
  "id": "1a2b3c4d-5678-90ab-cdef-1234567890ab",
  "firstName": "Douglas",
  "lastName": "Adams",
  "books": [],
  "insertedAt": "2024-08-13T09:00:00Z"
}
```

##### Passing an invalid name:
```bash
curl -i -X POST http://localhost:8080/authors \
  -H "Content-Type: application/json" \
  -d '{
        "firstName": "",
        "lastName": ""
      }'
```

Expected response:
```
HTTP/1.1 400 Bad Request
Content-Type: application/json

{
  "error":"Invalid request: Author first name is required; Author last name is required"
}
```

#### Fetch author by id:
```bash
curl -i http://localhost:8080/authors/1a2b3c4d-5678-90ab-cdef-1234567890ab
```

Expected response:
```
HTTP/1.1 200 OK
Content-Type: application/json

{
  "id": "1a2b3c4d-5678-90ab-cdef-1234567890ab",
  "firstName": "Douglas",
  "lastName": "Adams",
  "books": [
    {
      "id": "9f8e7d6c-5432-10fe-dcba-0987654321ab",
      "authorId": "1a2b3c4d-5678-90ab-cdef-1234567890ab",
      "title": "The Hitchhiker's Guide to the Galaxy",
      "insertedAt": "2024-08-13T09:05:00Z"
    }
  ],
  "insertedAt": "2024-08-13T09:00:00Z"
}
```

##### If the id doesn't exist:
```
HTTP/1.1 404 Not Found
Content-Type: application/json

{
  "error": "Invalid resource identifier: Author 1a2b3c4d-5678-90ab-cdef-1234567890ab not found"
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

#### Add a book to an author:
```
curl -i -X POST http://localhost:8080/authors/1a2b3c4d-5678-90ab-cdef-1234567890ab/books \
  -H "Content-Type: application/json" \
  -d '{
        "title": "The Hitchhiker'\''s Guide to the Galaxy"
      }'
```

Expected response:
```
HTTP/1.1 201 Created
Content-Type: application/json

{
  "id": "9f8e7d6c-5432-10fe-dcba-0987654321ab",
  "authorId": "1a2b3c4d-5678-90ab-cdef-1234567890ab",
  "title": "The Hitchhiker's Guide to the Galaxy",
  "insertedAt": "2024-08-13T09:05:00Z"
}
```

##### For a non-existent author:
```
curl -i -X POST http://localhost:8080/authors/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaab/books \
  -H "Content-Type: application/json" \
  -d '{
        "title": "The Hitchhiker'\''s Guide to the Galaxy"
      }'
```

The expected response is:
```
HTTP/1.1 404
Content-Type: application/json
Transfer-Encoding: chunked

{"error":"Invalid resource identifier: Author aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaab not found"}
```

#### Add sale for book:
```bash
curl -i -X POST http://localhost:8080/sale \
  -H "Content-Type: application/json" \
  -d '{
        "bookId": "0a000000-0000-0000-0000-000000000001",
        "units": 100,
        "amountEur": 100.1
      }'
```

Expected response:
```
HTTP/1.1 201 Created
Content-Type: application/json

{
  "id": "f7ffc823-3586-43fc-9839-f0a00324d238",
  "bookId": "0a000000-0000-0000-0000-000000000001",
  "units": 100,
  "amountEur": 100.1,
  "soldAt": "2025-10-08T22:43:12.510266Z"
}
```

##### Passing an invalid body:
```bash
curl -i -X POST http://localhost:8080/sale \
  -H "Content-Type: application/json" \
  -d '{
        "units": 100,
        "amountEur": 100.1
      }'
```

Expected response:
```
HTTP/1.1 400 Bad Request
Content-Type: application/json

{
    "error":"Invalid request: Book ID is required"
}
```

#### Fetch monthly royalty report for an author:
```
curl -i http://localhost:8080/authors/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa/royalties/2024-02
```

Expected response:
```
HTTP/1.1 200
Content-Type: application/json
Transfer-Encoding: chunked

{
  "authorId": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
  "period": "2024-02",
  "totalUnits": 4375,
  "grossRevenue": 48245.25,
  "effectiveRate": 0.160000,
  "royaltyDue": 7719.24000000,
  "minimumGuarantee": 100,
  "breakdown": [
    {
      "unitsInTier": 1000,
      "appliedRate": 0.1,
      "royaltyAmount": 1102.74857143
    },
    {
      "unitsInTier": 1500,
      "appliedRate": 0.15,
      "royaltyAmount": 2481.18428571
    },
    {
      "unitsInTier": 1875,
      "appliedRate": 0.2,
      "royaltyAmount": 4135.30714286
    },
    {
      "unitsInTier": 0,
      "appliedRate": 0.25,
      "royaltyAmount": 0.0000
    }
  ]
}
```

##### For a non-existent author:
```
curl -i http://localhost:8080/authors/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaab/royalties/2024-02
```

The expected response is:
```
HTTP/1.1 404
Content-Type: application/json
Transfer-Encoding: chunked

{"error":"Invalid resource identifier: Author aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaab not found"}
```

#### Create a sale:
```
curl -X POST "http://localhost:8080/sale" \
  -H "Content-Type: application/json" \
  -d '{
    "bookId": "0a000000-0000-0000-0000-000000000001",
    "units": 3,
    "amountEur": 59.97
  }'
  
 curl -X POST "http://localhost:8080/sale" \
  -H "Content-Type: application/json" \
  -d '{
    "bookId": "0a000000-0000-0000-0000-000000000009",
    "units": 3,
    "amountEur": 59.97
  }'
```