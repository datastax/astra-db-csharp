# Overview

This C# Client Library simplifies using the DataStax Data API to manage and interact with Astra DB instances as well as other DataStax databases.

# Installation

```
dotnet add package DataStax.AstraDB.DataApi
```

# Quickstart

```
//instantiate a client
var client = new DataApiClient();

//connect to a database
var database = client.GetDatabase("YourDatabaseUrlHere", "YourTokenHere");

//create a new collection
var collection = await database.CreateCollectionAsync<User>("YourCollectionNameHere");

//insert a document into the collection
var documents = new List<User>
{
    new User()
    {
        Name = "Test User 1",
    },
    new User()
    {
        Name = "Test User 2",
    }
};
var insertResult = await collection.InsertManyAsync(documents);

//find a document
var filter = Builders<User>.Filter.Eq(x => x.Name, "Test User 1");
var results = await collection.Find(filter);
```