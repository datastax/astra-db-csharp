/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Commands;
using DataStax.AstraDB.DataApi.Utils;
using System;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Collections;

public class Collection : Collection<Document>
{
    public Collection(string collectionName, Database database) : base(collectionName, database)
    { }
}

public class Collection<T> where T : class
{
    private readonly string _collectionName;
    private readonly Database _database;

    public string CollectionName => _collectionName;

    public Collection(string collectionName, Database database)
    {
        Guard.NotNullOrEmpty(collectionName, nameof(collectionName));
        Guard.NotNull(database, nameof(database));
        _collectionName = collectionName;
        _database = database;
    }

    public CollectionInsertOneResult InsertOne(T document)
    {
        return InsertOne(document, new CollectionInsertOneOptions());
    }

    public CollectionInsertOneResult InsertOne(T document, CollectionInsertOneOptions options)
    {
        return InsertOneAsync(document, options, runSynchronously: true).ResultSync();
    }

    public Task<CollectionInsertOneResult> InsertOneAsync(T document)
    {
        return InsertOneAsync(document, new CollectionInsertOneOptions());
    }

    public Task<CollectionInsertOneResult> InsertOneAsync(T document, CollectionInsertOneOptions options)
    {
        return InsertOneAsync(document, options, runSynchronously: false);
    }

    private async Task<CollectionInsertOneResult> InsertOneAsync(T document, CollectionInsertOneOptions options, bool runSynchronously)
    {
        Guard.NotNull(document, nameof(document));
        Guard.NotNull(options, nameof(options));

        var command = CreateCommand("insertOne").WithDocument(document);
        var response = await command.RunAsync<InsertDocumentsCommandResponse>(runSynchronously).ConfigureAwait(false);
        if (response == null)
        {
            //TODO: handle error
            throw new Exception();
        }

        return new CollectionInsertOneResult { InsertedId = response.Status.InsertedIds[0] };
    }

    internal Command CreateCommand(string name)
    {
        return new Command(name, _database.Client, _database.OptionsTree, new DatabaseCommandUrlBuilder(_database, _database.OptionsTree, _collectionName));
    }
}

//TODO move these classes
internal class InsertDocumentsCommandResponse
{
    [JsonPropertyName("insertedIds")]
    public object[] InsertedIds { get; set; }
}

public class CollectionInsertOneOptions
{
    //TODO implementation
}

public class CollectionInsertOneResult
{
    public object InsertedId { get; internal set; }
}
