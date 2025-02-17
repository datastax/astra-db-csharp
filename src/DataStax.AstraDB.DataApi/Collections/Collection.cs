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
<<<<<<< HEAD
using DataStax.AstraDB.DataApi.Core.Results;
using DataStax.AstraDB.DataApi.Utils;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Collections;
=======
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
>>>>>>> 2334bb53b9027422e19c36f2165408c636ddc279

public class Collection<T> where T : class
{
    private readonly string _collectionName;
    private readonly Database _database;
    private readonly CommandOptions _commandOptions;

    public string CollectionName => _collectionName;

    internal Collection(string collectionName, Database database, CommandOptions commandOptions)
    {
        Guard.NotNullOrEmpty(collectionName, nameof(collectionName));
        Guard.NotNull(database, nameof(database));
        _collectionName = collectionName;
        _database = database;
        _commandOptions = commandOptions;
    }

    public CollectionInsertOneResult InsertOne(T document)
    {
        return InsertOne(document, null);
    }

    public CollectionInsertOneResult InsertOne(T document, CommandOptions commandOptions)
    {
<<<<<<< HEAD
        return InsertOneAsync(document, commandOptions, runSynchronously: true).ResultSync();
=======
        return InsertOneAsync(document, options, runSynchronously: true).ResultSync();
>>>>>>> 2334bb53b9027422e19c36f2165408c636ddc279
    }

    public Task<CollectionInsertOneResult> InsertOneAsync(T document)
    {
        return InsertOneAsync(document, new CommandOptions());
    }

    public Task<CollectionInsertOneResult> InsertOneAsync(T document, CommandOptions commandOptions)
    {
<<<<<<< HEAD
        return InsertOneAsync(document, commandOptions, runSynchronously: false);
    }

    private async Task<CollectionInsertOneResult> InsertOneAsync(T document, CommandOptions commandOptions, bool runSynchronously)
=======
        return InsertOneAsync(document, options, runSynchronously: false);
    }

    private async Task<CollectionInsertOneResult> InsertOneAsync(T document, CollectionInsertOneOptions options, bool runSynchronously)
>>>>>>> 2334bb53b9027422e19c36f2165408c636ddc279
    {
        Guard.NotNull(document, nameof(document));

<<<<<<< HEAD
        var command = CreateCommand("insertOne").WithDocument(document).AddCommandOptions(commandOptions);
        var response = await command.RunAsync<InsertDocumentsCommandResponse>(runSynchronously).ConfigureAwait(false);
=======
        var command = CreateCommand("insertOne").WithDocument(document);
        var response = await command.RunAsync<InsertDocumentsCommandResponse>(runSynchronously).ConfigureAwait(false);
        if (response == null)
        {
            //TODO: handle error
            throw new Exception();
        }
>>>>>>> 2334bb53b9027422e19c36f2165408c636ddc279

        return new CollectionInsertOneResult { InsertedId = response.Result.InsertedIds[0] };
    }

    public void Drop()
    {
        _database.DropCollection(_collectionName);
    }

    public async Task DropAsync()
    {
        await _database.DropCollectionAsync(_collectionName).ConfigureAwait(false);
    }

    internal Command CreateCommand(string name)
    {
        return new Command(name, _database.Client, _database.OptionsTree, new DatabaseCommandUrlBuilder(_database, _database.OptionsTree, _collectionName));
    }
}