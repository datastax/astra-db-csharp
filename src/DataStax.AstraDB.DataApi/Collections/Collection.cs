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
using DataStax.AstraDB.DataApi.Core.Query;
using DataStax.AstraDB.DataApi.Core.Results;
using DataStax.AstraDB.DataApi.SerDes;
using DataStax.AstraDB.DataApi.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Collections;

public class Collection<T> : Collection<T, object> where T : class
{
    internal Collection(string collectionName, Database database, CommandOptions commandOptions)
        : base(collectionName, database, commandOptions) { }
}

public class Collection<T, TId> where T : class
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

    public CollectionInsertOneResult<TId> InsertOne(T document)
    {
        return InsertOne(document, null);
    }

    public CollectionInsertOneResult<TId> InsertOne(T document, CommandOptions commandOptions)
    {
        return InsertOneAsync(document, commandOptions, runSynchronously: true).ResultSync();
    }

    public Task<CollectionInsertOneResult<TId>> InsertOneAsync(T document)
    {
        return InsertOneAsync(document, new CommandOptions());
    }

    public Task<CollectionInsertOneResult<TId>> InsertOneAsync(T document, CommandOptions commandOptions)
    {
        return InsertOneAsync(document, commandOptions, runSynchronously: false);
    }

    private async Task<CollectionInsertOneResult<TId>> InsertOneAsync(T document, CommandOptions commandOptions, bool runSynchronously)
    {
        Guard.NotNull(document, nameof(document));
        var payload = new { document };
        var command = CreateCommand("insertOne").WithPayload(payload).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnStatus<InsertDocumentsCommandResponse<TId>>(runSynchronously).ConfigureAwait(false);
        return new CollectionInsertOneResult<TId> { InsertedId = response.Result.InsertedIds[0] };
    }

    public CollectionInsertManyResult<TId> InsertMany(List<T> documents)
    {
        return InsertMany(documents, null, null);
    }

    public CollectionInsertManyResult<TId> InsertMany(List<T> documents, InsertManyOptions insertOptions)
    {
        return InsertMany(documents, insertOptions, null);
    }

    public CollectionInsertManyResult<TId> InsertMany(List<T> documents, CommandOptions commandOptions)
    {
        return InsertMany(documents, null, commandOptions);
    }

    public CollectionInsertManyResult<TId> InsertMany(List<T> documents, InsertManyOptions insertOptions, CommandOptions commandOptions)
    {
        return InsertManyAsync(documents, insertOptions, commandOptions, runSynchronously: true).ResultSync();
    }

    public Task<CollectionInsertManyResult<TId>> InsertManyAsync(List<T> documents)
    {
        return InsertManyAsync(documents, new CommandOptions());
    }

    public Task<CollectionInsertManyResult<TId>> InsertManyAsync(List<T> documents, InsertManyOptions insertOptions)
    {
        return InsertManyAsync(documents, insertOptions, null, runSynchronously: false);
    }

    public Task<CollectionInsertManyResult<TId>> InsertManyAsync(List<T> documents, CommandOptions commandOptions)
    {
        return InsertManyAsync(documents, null, commandOptions, runSynchronously: false);
    }

    private async Task<CollectionInsertManyResult<TId>> InsertManyAsync(List<T> documents, InsertManyOptions insertOptions, CommandOptions commandOptions, bool runSynchronously)
    {
        Guard.NotNullOrEmpty(documents, nameof(documents));

        if (insertOptions == null) insertOptions = new InsertManyOptions();
        if (insertOptions.Concurrency > 1 && insertOptions.InsertInOrder)
        {
            throw new ArgumentException("Cannot run ordered insert_many concurrently.");
        }
        if (insertOptions.ChunkSize > InsertManyOptions.MaxChunkSize)
        {
            throw new ArgumentException("Chunk size cannot be greater than the max chunk size of " + InsertManyOptions.MaxChunkSize + ".");
        }

        var start = DateTime.Now;

        var result = new CollectionInsertManyResult<TId>();
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

    private async Task<CollectionInsertManyResult<TId>> RunInsertManyAsync(List<T> documents, bool insertOrdered, CommandOptions commandOptions, bool runSynchronously)
    {
        var payload = new
        {
            documents,
            options = new
            {
                ordered = insertOrdered
            }
        };
        commandOptions ??= new CommandOptions();
        var outputConverter = typeof(TId) == typeof(object) ? new IdListConverter() : null;
        commandOptions.SetConvertersIfNull(new DocumentConverter<T>(), outputConverter);
        var command = CreateCommand("insertMany").WithPayload(payload).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnStatus<InsertDocumentsCommandResponse<TId>>(runSynchronously).ConfigureAwait(false);
        return new CollectionInsertManyResult<TId> { InsertedIds = response.Result.InsertedIds.ToList() };
    }

    public void Drop()
    {
        _database.DropCollection(_collectionName);
    }

    public async Task DropAsync()
    {
        await _database.DropCollectionAsync(_collectionName).ConfigureAwait(false);
    }

    public T FindOne()
    {
        return FindOne(null, new FindOptions<T>(), null);
    }

    public T FindOne(CommandOptions commandOptions)
    {
        return FindOne(null, new FindOptions<T>(), commandOptions);
    }

    public T FindOne(Filter<T> filter)
    {
        return FindOne(filter, new FindOptions<T>(), null);
    }

    public T FindOne(Filter<T> filter, CommandOptions commandOptions)
    {
        return FindOne(filter, new FindOptions<T>(), commandOptions);
    }

    public T FindOne(Filter<T> filter, FindOptions<T> findOptions)
    {
        return FindOne(filter, findOptions, null);
    }

    public T FindOne(Filter<T> filter, FindOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOneAsync(filter, findOptions, commandOptions, true).ResultSync();
    }

    public Task<T> FindOneAsync()
    {
        return FindOneAsync(null, new FindOptions<T>(), null);
    }

    public Task<T> FindOneAsync(CommandOptions commandOptions)
    {
        return FindOneAsync(null, new FindOptions<T>(), commandOptions);
    }

    public Task<T> FindOneAsync(Filter<T> filter)
    {
        return FindOneAsync(filter, new FindOptions<T>(), null);
    }

    public Task<T> FindOneAsync(Filter<T> filter, CommandOptions commandOptions)
    {
        return FindOneAsync(filter, new FindOptions<T>(), commandOptions);
    }

    public Task<T> FindOneAsync(Filter<T> filter, FindOptions<T> findOptions)
    {
        return FindOneAsync(filter, findOptions, null);
    }

    public Task<T> FindOneAsync(Filter<T> filter, FindOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOneAsync(filter, findOptions, commandOptions, false);
    }

    private async Task<T> FindOneAsync(Filter<T> filter, FindOptions<T> findOptions, CommandOptions commandOptions, bool runSynchronously)
    {
        //TODO: Add and handle TProjection
        findOptions.Filter = filter;
        var command = CreateCommand("findOne").WithPayload(findOptions).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnData<T>(runSynchronously).ConfigureAwait(false);
        return response.Data.Document;
    }

    internal Command CreateCommand(string name)
    {
        var optionsTree = _commandOptions == null ? _database.OptionsTree : _database.OptionsTree.Concat(new[] { _commandOptions }).ToArray();
        return new Command(name, _database.Client, optionsTree, new DatabaseCommandUrlBuilder(_database, _collectionName));
    }
}
