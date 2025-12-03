## Running
### Local / Dev
Set the following environment variables before running:
```
export ASTRA_DB_TOKEN="your_token_here"
export ASTRA_DB_URL="your_db_url_here"
export ASTRA_DB_DESTINATION="astra" (or hcd, etc.)
```

The run the tests:
```
dotnet test
```