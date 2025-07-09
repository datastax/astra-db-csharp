## Running
### Local / Dev
Set the following environment variables before running:
```
export ASTRA_DB_TOKEN="your_token_here"
export ASTRA_DB_URL="your_db_url_here"
```

dotnet test --collect:"XPlat Code Coverage" 

reportgenerator
-reports:"Path\To\TestProject\TestResults\{guid}\coverage.cobertura.xml"
-targetdir:"coveragereport"
-reporttypes:Html