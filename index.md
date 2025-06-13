---
_layout: landing
---

# Overview

This C# Client Library simplifies using the DataStax Data API to manage and interact with Astra DB instances as well as other DataStax databases.

# Installation

```
dotnet add package DataStax.AstraDB.DataApi --prerelease
```

# Quickstart

```
//instantiate a client
var client = new DataApiClient("YourTokenHere");

//connect to a database
var database = client.GetDatabase("YourAPIEndpointHere");

//create a new collection
var collection = await database.CreateCollectionAsync<SimpleObject>("YourCollectionNameHere");

//insert a document into the collection
var documents = new List<SimpleObject>
{
    new SimpleObject()
    {
        Name = "Test Object 1",
    },
    new SimpleObject()
    {
        Name = "Test Object 2",
    }
};
var insertResult = await collection.InsertManyAsync(documents);

//find a document
var filter = Builders<SimpleObject>.Filter.Eq(so => so.Name, "Test Object 1");
var results = await collection.Find(filter);
```