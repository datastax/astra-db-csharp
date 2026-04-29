# astra-db-csharp

`astra-db-csharp` is a C# client for interacting with [DataStax Astra DB](https://astra.datastax.com/) and Hyper-Converged Database.

View the full client and Data API **documentation** at [docs.datastax.com](https://docs.datastax.com/en/astra-db-serverless/api-reference/dataapiclient.html#use-a-client).

## Quickstart

Install the client with:

```
dotnet add package DataStax.AstraDB.DataApi
```

Get the **API Endpoint** and the **Application Token** for your Astra DB instance at [astra.datastax.com](https://astra.datastax.com/).

Try the following code after replacing the connection parameters:

```csharp
using DataStax.AstraDB.DataApi;
using DataStax.AstraDB.DataApi.Core;

namespace Quickstart
{
  public class QuickstartConnect
  {
    public static Database ConnectToDatabase()
    {
      string? endpoint = Environment.GetEnvironmentVariable(
        "API_ENDPOINT"
      ); 
      string? token = Environment.GetEnvironmentVariable(
        "APPLICATION_TOKEN"
      );

      if (string.IsNullOrEmpty(endpoint) || string.IsNullOrEmpty(token))
      {
        throw new InvalidOperationException(
          "Environment variables API_ENDPOINT and APPLICATION_TOKEN must be defined"
        );
      }

      // Create an instance of the `DataAPIClient` class
      var client = new DataAPIClient();

      // Get the database specified by your endpoint and provide the token
      var database = client.GetDatabase(
        endpoint,
        new DatabaseCommandOptions() { Token = token }
      );

      Console.WriteLine("Connected to database.");

      return database;
    }
  }
}
```

## Learn more

Documentation: [docs.datastax.com](https://docs.datastax.com/en/astra-db-serverless/api-reference/dataapiclient.html#use-a-client).
