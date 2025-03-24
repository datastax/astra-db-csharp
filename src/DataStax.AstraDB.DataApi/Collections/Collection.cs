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

    public T FindOne(FindOptions<T> findOptions)
    {
        return FindOne(null, findOptions, null);
    }

    public T FindOne(FindOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOne(null, findOptions, commandOptions);
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
        return FindOneAsync<T>(filter, findOptions, commandOptions, true).ResultSync();
    }

    public TResult FindOne<TResult>()
    {
        return FindOne<TResult>(null, new FindOptions<T>(), null);
    }

    public TResult FindOne<TResult>(CommandOptions commandOptions)
    {
        return FindOne<TResult>(null, new FindOptions<T>(), commandOptions);
    }

    public TResult FindOne<TResult>(Filter<T> filter)
    {
        return FindOne<TResult>(filter, new FindOptions<T>(), null);
    }

    public TResult FindOne<TResult>(FindOptions<T> findOptions)
    {
        return FindOne<TResult>(null, findOptions, null);
    }

    public TResult FindOne<TResult>(FindOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOne<TResult>(null, findOptions, commandOptions);
    }

    public TResult FindOne<TResult>(Filter<T> filter, CommandOptions commandOptions)
    {
        return FindOne<TResult>(filter, new FindOptions<T>(), commandOptions);
    }

    public TResult FindOne<TResult>(Filter<T> filter, FindOptions<T> findOptions)
    {
        return FindOne<TResult>(filter, findOptions, null);
    }

    public TResult FindOne<TResult>(Filter<T> filter, FindOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOneAsync<TResult>(filter, findOptions, commandOptions, true).ResultSync();
    }

    public Task<T> FindOneAsync()
    {
        return FindOneAsync(null, new FindOptions<T>(), null);
    }

    public Task<T> FindOneAsync(CommandOptions commandOptions)
    {
        return FindOneAsync(null, new FindOptions<T>(), commandOptions);
    }

    public Task<T> FindOneAsync(FindOptions<T> findOptions)
    {
        return FindOneAsync(null, findOptions, null);
    }

    public Task<T> FindOneAsync(FindOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOneAsync(null, findOptions, commandOptions);
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
        return FindOneAsync<T>(filter, findOptions, commandOptions, false);
    }

    public Task<TResult> FindOneAsync<TResult>()
    {
        return FindOneAsync<TResult>(null, new FindOptions<T>(), null);
    }

    public Task<TResult> FindOneAsync<TResult>(CommandOptions commandOptions)
    {
        return FindOneAsync<TResult>(null, new FindOptions<T>(), commandOptions);
    }

    public Task<TResult> FindOneAsync<TResult>(FindOptions<T> findOptions)
    {
        return FindOneAsync<TResult>(null, findOptions, null);
    }

    public Task<TResult> FindOneAsync<TResult>(FindOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOneAsync<TResult>(null, findOptions, commandOptions);
    }

    public Task<TResult> FindOneAsync<TResult>(Filter<T> filter)
    {
        return FindOneAsync<TResult>(filter, new FindOptions<T>(), null);
    }

    public Task<TResult> FindOneAsync<TResult>(Filter<T> filter, CommandOptions commandOptions)
    {
        return FindOneAsync<TResult>(filter, new FindOptions<T>(), commandOptions);
    }

    public Task<TResult> FindOneAsync<TResult>(Filter<T> filter, FindOptions<T> findOptions)
    {
        return FindOneAsync<TResult>(filter, findOptions, null);
    }

    public Task<TResult> FindOneAsync<TResult>(Filter<T> filter, FindOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOneAsync<TResult>(filter, findOptions, commandOptions, false);
    }

    private async Task<TResult> FindOneAsync<TResult>(Filter<T> filter, FindOptions<T> findOptions, CommandOptions commandOptions, bool runSynchronously)
    {
        findOptions.Filter = filter;
        var command = CreateCommand("findOne").WithPayload(findOptions).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnData<DocumentResult<TResult>, TResult, FindStatusResult>(runSynchronously).ConfigureAwait(false);
        return response.Data.Document;
    }

    public FluentFind<T, TId, T> Find()
    {
        return new FluentFind<T, TId, T>(this, null);
    }

    public FluentFind<T, TId, T> Find(Filter<T> filter)
    {
        return new FluentFind<T, TId, T>(this, filter);
    }

    public FluentFind<T, TId, TResult> Find<TResult>() where TResult : class
    {
        return new FluentFind<T, TId, TResult>(this, null);
    }

    public FluentFind<T, TId, TResult> Find<TResult>(Filter<T> filter) where TResult : class
    {
        return new FluentFind<T, TId, TResult>(this, filter);
    }

    public Cursor<T> FindMany()
    {
        return FindMany(null, new FindOptions<T>(), null);
    }

    public Cursor<T> FindMany(CommandOptions commandOptions)
    {
        return FindMany(null, new FindOptions<T>(), commandOptions);
    }

    public Cursor<T> FindMany(Filter<T> filter)
    {
        return FindMany(filter, new FindOptions<T>(), null);
    }

    public Cursor<T> FindMany(FindOptions<T> findOptions)
    {
        return FindMany(null, findOptions, null);
    }

    public Cursor<T> FindMany(FindOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindMany(null, findOptions, commandOptions);
    }

    public Cursor<T> FindMany(Filter<T> filter, CommandOptions commandOptions)
    {
        return FindMany(filter, new FindOptions<T>(), commandOptions);
    }

    public Cursor<T> FindMany(Filter<T> filter, FindOptions<T> findOptions)
    {
        return FindMany(filter, findOptions, null);
    }

    public Cursor<T> FindMany(Filter<T> filter, FindOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindManyAsyncCursor<T>(filter, findOptions, commandOptions, true).ResultSync();
    }

    public Cursor<TResult> FindMany<TResult>()
    {
        return FindManyAsyncCursor<TResult>(null, new FindOptions<T>(), null, true).ResultSync();
    }

    public Cursor<TResult> FindMany<TResult>(CommandOptions commandOptions)
    {
        return FindMany<TResult>(null, new FindOptions<T>(), commandOptions);
    }

    public Cursor<TResult> FindMany<TResult>(Filter<T> filter)
    {
        return FindMany<TResult>(filter, new FindOptions<T>(), null);
    }

    public Cursor<TResult> FindMany<TResult>(FindOptions<T> findOptions)
    {
        return FindMany<TResult>(null, findOptions, null);
    }

    public Cursor<TResult> FindMany<TResult>(FindOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindMany<TResult>(null, findOptions, commandOptions);
    }

    public Cursor<TResult> FindMany<TResult>(Filter<T> filter, CommandOptions commandOptions)
    {
        return FindMany<TResult>(filter, new FindOptions<T>(), commandOptions);
    }

    public Cursor<TResult> FindMany<TResult>(Filter<T> filter, FindOptions<T> findOptions)
    {
        return FindMany<TResult>(filter, findOptions, null);
    }

    public Cursor<TResult> FindMany<TResult>(Filter<T> filter, FindOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindManyAsyncCursor<TResult>(filter, findOptions, commandOptions, true).ResultSync();
    }

    public Task<Cursor<T>> FindManyAsync()
    {
        return FindManyAsync(null, new FindOptions<T>(), null);
    }

    public Task<Cursor<T>> FindManyAsync(CommandOptions commandOptions)
    {
        return FindManyAsync(null, new FindOptions<T>(), commandOptions);
    }

    public Task<Cursor<T>> FindManyAsync(FindOptions<T> findOptions)
    {
        return FindManyAsync(null, findOptions, null);
    }

    public Task<Cursor<T>> FindManyAsync(FindOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindManyAsync(null, findOptions, commandOptions);
    }

    public Task<Cursor<T>> FindManyAsync(Filter<T> filter)
    {
        return FindManyAsync(filter, new FindOptions<T>(), null);
    }

    public Task<Cursor<T>> FindManyAsync(Filter<T> filter, CommandOptions commandOptions)
    {
        return FindManyAsync(filter, new FindOptions<T>(), commandOptions);
    }

    public Task<Cursor<T>> FindManyAsync(Filter<T> filter, FindOptions<T> findOptions)
    {
        return FindManyAsync(filter, findOptions, null);
    }

    public Task<Cursor<TResult>> FindManyAsync<TResult>()
    {
        return FindManyAsync<TResult>(null, new FindOptions<T>(), null);
    }

    public Task<Cursor<TResult>> FindManyAsync<TResult>(CommandOptions commandOptions)
    {
        return FindManyAsync<TResult>(null, new FindOptions<T>(), commandOptions);
    }

    public Task<Cursor<TResult>> FindManyAsync<TResult>(FindOptions<T> findOptions)
    {
        return FindManyAsync<TResult>(null, findOptions, null);
    }

    public Task<Cursor<TResult>> FindManyAsync<TResult>(FindOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindManyAsync<TResult>(null, findOptions, commandOptions);
    }

    public Task<Cursor<TResult>> FindManyAsync<TResult>(Filter<T> filter)
    {
        return FindManyAsync<TResult>(filter, new FindOptions<T>(), null);
    }

    public Task<Cursor<TResult>> FindManyAsync<TResult>(Filter<T> filter, CommandOptions commandOptions)
    {
        return FindManyAsync<TResult>(filter, new FindOptions<T>(), commandOptions);
    }

    public Task<Cursor<TResult>> FindManyAsync<TResult>(Filter<T> filter, FindOptions<T> findOptions)
    {
        return FindManyAsync<TResult>(filter, findOptions, null);
    }

    public Task<Cursor<T>> FindManyAsync(Filter<T> filter, FindOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindManyAsyncCursor<T>(filter, findOptions, commandOptions, false);
    }

    public Task<Cursor<TResult>> FindManyAsync<TResult>(Filter<T> filter, FindOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindManyAsyncCursor<TResult>(filter, findOptions, commandOptions, false);
    }

    internal async Task<Cursor<TResult>> FindManyAsyncCursor<TResult>(Filter<T> filter, FindOptions<T> findOptions, CommandOptions commandOptions, bool runSynchronously)
    {
        findOptions.Filter = filter;
        var command = CreateCommand("find").WithPayload(findOptions).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnData<DocumentsResult<TResult>, TResult, FindStatusResult>(runSynchronously).ConfigureAwait(false);
        return new Cursor<TResult>(response, (string pageState, bool runSynchronously) =>
        {
            findOptions ??= new FindOptions<T>();
            findOptions.PageState = pageState;
            return FindManyAsync<TResult>(filter, findOptions, commandOptions, runSynchronously);
        });
    }

    internal async Task<ApiResponseWithData<DocumentsResult<TResult>, FindStatusResult>> FindManyAsync<TResult>(Filter<T> filter, FindOptions<T> findOptions, CommandOptions commandOptions, bool runSynchronously)
    {
        findOptions.Filter = filter;
        var command = CreateCommand("find").WithPayload(findOptions).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnData<DocumentsResult<TResult>, TResult, FindStatusResult>(runSynchronously).ConfigureAwait(false);
        return response;
    }

    public DocumentsCountResult CountDocuments()
    {
        return CountDocumentsAsync(null, null, true).ResultSync();
    }

    public DocumentsCountResult CountDocuments(CommandOptions commandOptions)
    {
        return CountDocumentsAsync(null, commandOptions, true).ResultSync();
    }

    public Task<DocumentsCountResult> CountDocumentsAsync()
    {
        return CountDocumentsAsync(null, null, false);
    }

    public Task<DocumentsCountResult> CountDocumentsAsync(CommandOptions commandOptions)
    {
        return CountDocumentsAsync(null, commandOptions, false);
    }

    public DocumentsCountResult CountDocuments(Filter<T> filter)
    {
        return CountDocumentsAsync(filter, null, true).ResultSync();
    }

    public DocumentsCountResult CountDocuments(Filter<T> filter, CommandOptions commandOptions)
    {
        return CountDocumentsAsync(filter, commandOptions, true).ResultSync();
    }

    public Task<DocumentsCountResult> CountDocumentsAsync(Filter<T> filter)
    {
        return CountDocumentsAsync(filter, null, false);
    }

    public Task<DocumentsCountResult> CountDocumentsAsync(Filter<T> filter, CommandOptions commandOptions)
    {
        return CountDocumentsAsync(filter, commandOptions, false);
    }

    internal async Task<DocumentsCountResult> CountDocumentsAsync(Filter<T> filter, CommandOptions commandOptions, bool runSynchronously)
    {
        var findOptions = new FindOptions<T>()
        {
            Filter = filter,
        };
        var command = CreateCommand("countDocuments").WithPayload(findOptions).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnStatus<DocumentsCountResult>(runSynchronously).ConfigureAwait(false);
        return response.Result;
    }

    public EstimatedDocumentsCountResult EstimateDocumentCount()
    {
        return EstimateDocumentCountAsync(null, true).ResultSync();
    }

    public EstimatedDocumentsCountResult EstimateDocumentCount(CommandOptions commandOptions)
    {
        return EstimateDocumentCountAsync(commandOptions, true).ResultSync();
    }

    public Task<EstimatedDocumentsCountResult> EstimateDocumentCountAsync()
    {
        return EstimateDocumentCountAsync(null, false);
    }

    public Task<EstimatedDocumentsCountResult> EstimateDocumentCountAsync(CommandOptions commandOptions)
    {
        return EstimateDocumentCountAsync(commandOptions, false);
    }

    internal async Task<EstimatedDocumentsCountResult> EstimateDocumentCountAsync(CommandOptions commandOptions, bool runSynchronously)
    {
        var command = CreateCommand("estimatedDocumentCount").WithPayload(new { }).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnStatus<EstimatedDocumentsCountResult>(runSynchronously).ConfigureAwait(false);
        return response.Result;
    }

    internal Command CreateCommand(string name)
    {
        var optionsTree = _commandOptions == null ? _database.OptionsTree : _database.OptionsTree.Concat(new[] { _commandOptions }).ToArray();
        return new Command(name, _database.Client, optionsTree, new DatabaseCommandUrlBuilder(_database, _collectionName));
    }
}
