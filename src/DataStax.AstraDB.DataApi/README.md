# Overview

This C# Client Library simplifies using the DataStax Data API to manage and interact with Astra DB instances as well as other DataStax databases.

# Installation

```
dotnet add package DataStax.AstraDB.DataApi
```

# Upgrade to 2.0.1-beta

If you were on a previous version of the beta, please note the following breaking change. When setting various options for a Collection or Table Find command, all of the options are chained to the Find command. The DocumentFindManyOptions parameter is no longer needed (and has been removed from the SDK)

Instead of this:
```
        var findOptions = new DocumentFindManyOptions<SimpleObject>()
        {
            Sort = sort,
            Limit = 1,
            Skip = 2,
            Projection = inclusiveProjection
        };
        var results = collection.Find(filter, findOptions).ToList();
```

Now do this:
```
        var results = collection.Find(filter)
            .Sort(sort)
            .Skip(2)
            .Limit(1)
            .Project(inclusiveProjection)
            .ToList();
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