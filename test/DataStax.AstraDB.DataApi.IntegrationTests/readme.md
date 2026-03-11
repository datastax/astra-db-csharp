## Running
### Local / Dev
Set the following environment variables before running:
```
export ASTRA_DB_TOKEN="your_token_here"
export ASTRA_DB_URL="your_db_url_here"
export ASTRA_DB_DESTINATION="hcd2" (or hcd, etc.)
```

The run the tests:
```
dotnet test
```

### HCD

```
export ASTRA_DB_TOKEN="Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh"
export ASTRA_DB_URL="http://127.0.0.1:8181"
export ASTRA_DB_DESTINATION="hcd"

curl -X 'POST' \
  'http://localhost:8181/v1' \
  -H 'accept: application/json' \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
  "createNamespace": {
    "name": "default_keyspace"
  }
}'

http://127.0.0.1:8181/api/json/v1

dotnet test -e ASTRA_DB_URL="127.0.0.1:8181" -e ASTRA_DB_TOKEN="Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh"
```