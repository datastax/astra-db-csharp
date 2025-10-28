## Running
### Local / Dev
Set the following environment variables before running:
```
export ASTRA_DB_TOKEN="your_token_here"
export ASTRA_DB_URL="your_db_url_here"
export ASTRA_DB_DATABASE_NAME="your_db_name"
```

dotnet test -p:TestTfmsInParallel=false -- "xUnit.MaxParallelThreads=1"