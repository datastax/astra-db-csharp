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
using DataStax.AstraDB.DataApi.Core.Results;
using DataStax.AstraDB.DataApi.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Collections;

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
        return InsertOneAsync(document, commandOptions, runSynchronously: true).ResultSync();
    }

    public Task<CollectionInsertOneResult> InsertOneAsync(T document)
    {
        return InsertOneAsync(document, new CommandOptions());
    }

    public Task<CollectionInsertOneResult> InsertOneAsync(T document, CommandOptions commandOptions)
    {
        return InsertOneAsync(document, commandOptions, runSynchronously: false);
    }

    private async Task<CollectionInsertOneResult> InsertOneAsync(T document, CommandOptions commandOptions, bool runSynchronously)
    {
        Guard.NotNull(document, nameof(document));
        var payload = new { document };
        var command = CreateCommand("insertOne").WithPayload(payload).AddCommandOptions(commandOptions);
        var response = await command.RunAsync<InsertDocumentsCommandResponse>(runSynchronously).ConfigureAwait(false);
        return new CollectionInsertOneResult { InsertedId = response.Result.InsertedIds[0] };
    }

    public CollectionInsertManyResult InsertMany(List<T> documents)
    {
        return InsertMany(documents, null, null);
    }

    public CollectionInsertManyResult InsertMany(List<T> documents, InsertManyOptions insertOptions)
    {
        return InsertMany(documents, insertOptions, null);
    }

    public CollectionInsertManyResult InsertMany(List<T> documents, CommandOptions commandOptions)
    {
        return InsertMany(documents, null, commandOptions);
    }

    public CollectionInsertManyResult InsertMany(List<T> documents, InsertManyOptions insertOptions, CommandOptions commandOptions)
    {
        return InsertManyAsync(documents, insertOptions, commandOptions, runSynchronously: true).ResultSync();
    }

    public Task<CollectionInsertManyResult> InsertManyAsync(List<T> documents)
    {
        return InsertManyAsync(documents, new CommandOptions());
    }

    public Task<CollectionInsertManyResult> InsertManyAsync(List<T> documents, InsertManyOptions insertOptions)
    {
        return InsertManyAsync(documents, insertOptions, null, runSynchronously: false);
    }

    public Task<CollectionInsertManyResult> InsertManyAsync(List<T> documents, CommandOptions commandOptions)
    {
        return InsertManyAsync(documents, null, commandOptions, runSynchronously: false);
    }

    private async Task<CollectionInsertManyResult> InsertManyAsync(List<T> documents, InsertManyOptions insertOptions, CommandOptions commandOptions, bool runSynchronously)
    {
        Guard.NotNullOrEmpty(documents, nameof(documents));

        if (insertOptions.Concurrency > 1 && insertOptions.InsertInOrder)
        {
            throw new ArgumentException("Cannot run ordered insert_many concurrently.");
        }
        if (insertOptions.ChunkSize > InsertManyOptions.MaxChunkSize)
        {
            throw new ArgumentException("Chunk size cannot be greater than the max chunk size of " + InsertManyOptions.MaxChunkSize + ".");
        }

        var start = DateTime.Now;

        var result = new CollectionInsertManyResult();
        var tasks = new List<Task>();
        var semaphore = new SemaphoreSlim(insertOptions.Concurrency);

        var chunks = documents.Chunk(insertOptions.ChunkSize);

        foreach (var chunk in chunks)
        {
            tasks.Add(Task.Run(async () =>
            {
                await semaphore.WaitAsync();
                try
                {
                    var runResult = await RunInsertManyAsync(chunk, insertOptions.InsertInOrder, commandOptions, runSynchronously).ConfigureAwait(false);
                    lock (result.InsertedIds)
                    {
                        result.InsertedIds.AddRange(runResult.InsertedIds);
                    }
                }
                finally
                {
                    semaphore.Release();
                }
            }));
        }

        await Task.WhenAll(tasks);
        return result;
    }

    private async Task<CollectionInsertManyResult> RunInsertManyAsync(List<T> documents, bool insertOrdered, CommandOptions commandOptions, bool runSynchronously)
    {
        var payload = new
        {
            documents,
            options = new
            {
                ordered = insertOrdered
            }
        };
        var command = CreateCommand("insertMany").WithPayload(payload).AddCommandOptions(commandOptions);
        var response = await command.RunAsync<InsertDocumentsCommandResponse>(runSynchronously).ConfigureAwait(false);
        return new CollectionInsertManyResult { InsertedIds = response.Result.InsertedIds.ToList() };
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