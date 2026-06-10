# astra-db-csharp

`astra-db-csharp` is a C# client for interacting with [DataStax Astra DB](https://astra.datastax.com/) and Hyper-Converged Database.

## Setup & requirements

Update to one of the following:

- .NET version 8 or later
- .NET Framework 4.6.2 or later
- .NET Standard 2.1 or later

Install the client:

```
dotnet add package DataStax.AstraDB.DataApi
```

A database is needed for the client to connect to.
The database can be either an Astra DB instance or a Hyper-Converged Database (HCD) instance:
follow the appropriate documentation links to get the connection parameters for your database.

## Documentation 

For Astra DB Serverless:

- [Quickstart for collections](https://docs.datastax.com/en/astra-db-serverless/get-started/quickstart.html)
- [Quickstart for tables](https://docs.datastax.com/en/astra-db-serverless/get-started/quickstart-tables.html)
- [Get started with the Data API](https://docs.datastax.com/en/astra-db-serverless/api-reference/dataapiclient.html)

For Hyper-Converged Database (HCD):

- [Quickstart for collections](https://docs.datastax.com/en/hyper-converged-database/1.2/api-reference/quickstart.html)
- [Quickstart for tables](https://docs.datastax.com/en/hyper-converged-database/1.2/api-reference/quickstart-tables.html)
- [Get started with the Data API](https://docs.datastax.com/en/hyper-converged-database/1.2/api-reference/dataapiclient.html)

## At a glance

```csharp
using DataStax.AstraDB.DataApi;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Query;

// ...
// ...

var client = new DataAPIClient();
var database = client.GetDatabase(
    "<Astra DB API Endpoint>",
    new DatabaseCommandOptions() { Token = "<Astra DB Application Token>" }
);

// ...

var collection = await database.CreateCollectionAsync<MyDocumentClass>("ExampleCollection");
await collection.InsertManyAsync(new List<MyDocumentClass>()
    {
        new() { Name = "Apple", Score = 10, Description = "..." },
        new() { Name = "Peach", Score = 5, Description = "..." },
        new() { Name = "Walnut", Score = 8, Description = "..." }
    }
);

// ...

var findOptions = new CollectionFindOneOptions<MyDocumentClass>() {
    Sort = Builders<MyDocumentClass>.CollectionSort.Vectorize("<search query>"),
    IncludeSimilarity = true
};
var matchingDocument = await collection.FindOneAsync<MyDocumentSearchResultClass>(findOptions);
if ( matchingDocument != null ){
    Console.WriteLine($"Match: '{matchingDocument.Name}' (similarity: {matchingDocument.Similarity})");
}
```
