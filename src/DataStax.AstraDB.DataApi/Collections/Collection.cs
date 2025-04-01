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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Collections;

public class Collection : Collection<Document>
{
    internal Collection(string collectionName, Database database, CommandOptions commandOptions)
        : base(collectionName, database, commandOptions) { }
}

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
        InsertValidator.Validate(document);
        var payload = new { document };
        commandOptions ??= new CommandOptions();
        var outputConverter = typeof(TId) == typeof(object) ? new IdListConverter() : null;
        commandOptions.SetConvertersIfNull(new DocumentConverter<T>(), outputConverter);
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

        foreach (var doc in documents)
        {
            InsertValidator.Validate(doc);
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
        return Find(null, null, null);
    }

    public FluentFind<T, TId, T> Find(Filter<T> filter)
    {
        return Find(filter, null, null);
    }

    public FluentFind<T, TId, T> Find(FindOptions<T> findOptions)
    {
        return Find(null, findOptions, null);
    }

    public FluentFind<T, TId, T> Find(CommandOptions commandOptions)
    {
        return Find(null, new FindOptions<T>(), commandOptions);
    }

    public FluentFind<T, TId, T> Find(FindOptions<T> findOptions, CommandOptions commandOptions)
    {
        return Find(null, findOptions, commandOptions);
    }

    public FluentFind<T, TId, T> Find(Filter<T> filter, CommandOptions commandOptions)
    {
        return Find(filter, new FindOptions<T>(), commandOptions);
    }

    public FluentFind<T, TId, T> Find(Filter<T> filter, FindOptions<T> findOptions)
    {
        return Find(filter, findOptions, null);
    }

    public FluentFind<T, TId, T> Find(Filter<T> filter, FindOptions<T> findOptions, CommandOptions commandOptions)
    {
        return new FluentFind<T, TId, T>(this, filter, findOptions, commandOptions);
    }

    public FluentFind<T, TId, TResult> Find<TResult>() where TResult : class
    {
        return Find<TResult>(null, null, null);
    }

    public FluentFind<T, TId, TResult> Find<TResult>(Filter<T> filter) where TResult : class
    {
        return Find<TResult>(filter, null, null);
    }

    public FluentFind<T, TId, TResult> Find<TResult>(FindOptions<T> findOptions) where TResult : class
    {
        return Find<TResult>(null, findOptions, null);
    }

    public FluentFind<T, TId, TResult> Find<TResult>(CommandOptions commandOptions) where TResult : class
    {
        return Find<TResult>(null, new FindOptions<T>(), commandOptions);
    }

    public FluentFind<T, TId, TResult> Find<TResult>(FindOptions<T> findOptions, CommandOptions commandOptions) where TResult : class
    {
        return Find<TResult>(null, findOptions, commandOptions);
    }

    public FluentFind<T, TId, TResult> Find<TResult>(Filter<T> filter, CommandOptions commandOptions) where TResult : class
    {
        return Find<TResult>(filter, new FindOptions<T>(), commandOptions);
    }

    public FluentFind<T, TId, TResult> Find<TResult>(Filter<T> filter, FindOptions<T> findOptions) where TResult : class
    {
        return Find<TResult>(filter, findOptions, null);
    }

    public FluentFind<T, TId, TResult> Find<TResult>(Filter<T> filter, FindOptions<T> findOptions, CommandOptions commandOptions) where TResult : class
    {
        return new FluentFind<T, TId, TResult>(this, filter, findOptions, commandOptions);
    }

    internal async Task<ApiResponseWithData<DocumentsResult<TResult>, FindStatusResult>> RunFindManyAsync<TResult>(Filter<T> filter, FindOptions<T> findOptions, CommandOptions commandOptions, bool runSynchronously)
    {
        findOptions.Filter = filter;
        var command = CreateCommand("find").WithPayload(findOptions).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnData<DocumentsResult<TResult>, TResult, FindStatusResult>(runSynchronously).ConfigureAwait(false);
        return response;
    }

    public T FindOneAndUpdate(Filter<T> filter, UpdateBuilder<T> update)
    {
        return FindOneAndUpdate(filter, update, new FindOneAndUpdateOptions<T>(), null);
    }

    public T FindOneAndUpdate(Filter<T> filter, UpdateBuilder<T> update, FindOneAndUpdateOptions<T> updateOptions)
    {
        return FindOneAndUpdate(filter, update, updateOptions, null);
    }

    public T FindOneAndUpdate(Filter<T> filter, UpdateBuilder<T> update, FindOneAndUpdateOptions<T> updateOptions, CommandOptions commandOptions)
    {
        var response = FindOneAndUpdateAsync<T>(filter, update, updateOptions, commandOptions, true).ResultSync();
        return response;
    }

    public Task<T> FindOneAndUpdateAsync(Filter<T> filter, UpdateBuilder<T> update)
    {
        return FindOneAndUpdateAsync(filter, update, new FindOneAndUpdateOptions<T>(), null);
    }

    public Task<T> FindOneAndUpdateAsync(Filter<T> filter, UpdateBuilder<T> update, FindOneAndUpdateOptions<T> updateOptions)
    {
        return FindOneAndUpdateAsync(filter, update, updateOptions, null);
    }

    public async Task<T> FindOneAndUpdateAsync(Filter<T> filter, UpdateBuilder<T> update, FindOneAndUpdateOptions<T> updateOptions, CommandOptions commandOptions)
    {
        var response = await FindOneAndUpdateAsync<T>(filter, update, updateOptions, commandOptions, false).ConfigureAwait(false);
        return response;
    }

    public TResult FindOneAndUpdate<TResult>(Filter<T> filter, UpdateBuilder<T> update)
    {
        return FindOneAndUpdate<TResult>(filter, update, new FindOneAndUpdateOptions<T>(), null);
    }

    public TResult FindOneAndUpdate<TResult>(Filter<T> filter, UpdateBuilder<T> update, FindOneAndUpdateOptions<T> updateOptions)
    {
        return FindOneAndUpdate<TResult>(filter, update, updateOptions, null);
    }

    public TResult FindOneAndUpdate<TResult>(Filter<T> filter, UpdateBuilder<T> update, FindOneAndUpdateOptions<T> updateOptions, CommandOptions commandOptions)
    {
        var response = FindOneAndUpdateAsync<TResult>(filter, update, updateOptions, commandOptions, true).ResultSync();
        return response;
    }

    public Task<TResult> FindOneAndUpdateAsync<TResult>(Filter<T> filter, UpdateBuilder<T> update)
    {
        return FindOneAndUpdateAsync<TResult>(filter, update, new FindOneAndUpdateOptions<T>(), null);
    }

    public Task<TResult> FindOneAndUpdateAsync<TResult>(Filter<T> filter, UpdateBuilder<T> update, FindOneAndUpdateOptions<T> updateOptions)
    {
        return FindOneAndUpdateAsync<TResult>(filter, update, updateOptions, null);
    }

    public async Task<TResult> FindOneAndUpdateAsync<TResult>(Filter<T> filter, UpdateBuilder<T> update, FindOneAndUpdateOptions<T> updateOptions, CommandOptions commandOptions)
    {
        var response = await FindOneAndUpdateAsync<TResult>(filter, update, updateOptions, commandOptions, false).ConfigureAwait(false);
        return response;
    }

    internal async Task<TResult> FindOneAndUpdateAsync<TResult>(Filter<T> filter, UpdateBuilder<T> update, FindOneAndUpdateOptions<T> updateOptions, CommandOptions commandOptions, bool runSynchronously)
    {
        updateOptions.Filter = filter;
        updateOptions.Update = update;
        var command = CreateCommand("findOneAndUpdate").WithPayload(updateOptions).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnData<DocumentResult<TResult>, T, UpdateResult>(runSynchronously).ConfigureAwait(false);
        return response.Data.Document;
    }

    public T FindOneAndReplace(T replacement)
    {
        return FindOneAndReplace(replacement, new ReplaceOptions<T>(), null);
    }

    public T FindOneAndReplace(T replacement, ReplaceOptions<T> replaceOptions)
    {
        return FindOneAndReplace(replacement, replaceOptions, null);
    }

    public T FindOneAndReplace(T replacement, ReplaceOptions<T> replaceOptions, CommandOptions commandOptions)
    {
        var response = FindOneAndReplaceAsync<T>(null, replacement, replaceOptions, commandOptions, true).ResultSync();
        return response;
    }

    public Task<T> FindOneAndReplaceAsync(T replacement)
    {
        return FindOneAndReplaceAsync(replacement, new ReplaceOptions<T>(), null);
    }

    public Task<T> FindOneAndReplaceAsync(T replacement, ReplaceOptions<T> replaceOptions)
    {
        return FindOneAndReplaceAsync(replacement, replaceOptions, null);
    }

    public async Task<T> FindOneAndReplaceAsync(T replacement, ReplaceOptions<T> replaceOptions, CommandOptions commandOptions)
    {
        var response = await FindOneAndReplaceAsync<T>(null, replacement, replaceOptions, commandOptions, false).ConfigureAwait(false);
        return response;
    }

    public TResult FindOneAndReplace<TResult>(T replacement)
    {
        return FindOneAndReplace<TResult>(replacement, new ReplaceOptions<T>(), null);
    }

    public TResult FindOneAndReplace<TResult>(T replacement, ReplaceOptions<T> replaceOptions)
    {
        return FindOneAndReplace<TResult>(replacement, replaceOptions, null);
    }

    public TResult FindOneAndReplace<TResult>(T replacement, ReplaceOptions<T> replaceOptions, CommandOptions commandOptions)
    {
        var response = FindOneAndReplaceAsync<TResult>(null, replacement, replaceOptions, commandOptions, true).ResultSync();
        return response;
    }

    public Task<TResult> FindOneAndReplaceAsync<TResult>(T replacement)
    {
        return FindOneAndReplaceAsync<TResult>(replacement, new ReplaceOptions<T>(), null);
    }

    public Task<TResult> FindOneAndReplaceAsync<TResult>(T replacement, ReplaceOptions<T> replaceOptions)
    {
        return FindOneAndReplaceAsync<TResult>(replacement, replaceOptions, null);
    }

    public async Task<TResult> FindOneAndReplaceAsync<TResult>(T replacement, ReplaceOptions<T> replaceOptions, CommandOptions commandOptions)
    {
        var response = await FindOneAndReplaceAsync<TResult>(null, replacement, replaceOptions, commandOptions, false).ConfigureAwait(false);
        return response;
    }

    public T FindOneAndReplace(Filter<T> filter, T replacement)
    {
        return FindOneAndReplace(filter, replacement, new ReplaceOptions<T>(), null);
    }

    public T FindOneAndReplace(Filter<T> filter, T replacement, ReplaceOptions<T> replaceOptions)
    {
        return FindOneAndReplace(filter, replacement, replaceOptions, null);
    }

    public T FindOneAndReplace(Filter<T> filter, T replacement, ReplaceOptions<T> replaceOptions, CommandOptions commandOptions)
    {
        var response = FindOneAndReplaceAsync<T>(filter, replacement, replaceOptions, commandOptions, true).ResultSync();
        return response;
    }

    public Task<T> FindOneAndReplaceAsync(Filter<T> filter, T replacement)
    {
        return FindOneAndReplaceAsync(filter, replacement, new ReplaceOptions<T>(), null);
    }

    public Task<T> FindOneAndReplaceAsync(Filter<T> filter, T replacement, ReplaceOptions<T> replaceOptions)
    {
        return FindOneAndReplaceAsync(filter, replacement, replaceOptions, null);
    }

    public async Task<T> FindOneAndReplaceAsync(Filter<T> filter, T replacement, ReplaceOptions<T> replaceOptions, CommandOptions commandOptions)
    {
        var response = await FindOneAndReplaceAsync<T>(filter, replacement, replaceOptions, commandOptions, false).ConfigureAwait(false);
        return response;
    }

    public TResult FindOneAndReplace<TResult>(Filter<T> filter, T replacement)
    {
        return FindOneAndReplace<TResult>(filter, replacement, new ReplaceOptions<T>(), null);
    }

    public TResult FindOneAndReplace<TResult>(Filter<T> filter, T replacement, ReplaceOptions<T> replaceOptions)
    {
        return FindOneAndReplace<TResult>(filter, replacement, replaceOptions, null);
    }

    public TResult FindOneAndReplace<TResult>(Filter<T> filter, T replacement, ReplaceOptions<T> replaceOptions, CommandOptions commandOptions)
    {
        var response = FindOneAndReplaceAsync<TResult>(filter, replacement, replaceOptions, commandOptions, true).ResultSync();
        return response;
    }

    public Task<TResult> FindOneAndReplaceAsync<TResult>(Filter<T> filter, T replacement)
    {
        return FindOneAndReplaceAsync<TResult>(filter, replacement, new ReplaceOptions<T>(), null);
    }

    public Task<TResult> FindOneAndReplaceAsync<TResult>(Filter<T> filter, T replacement, ReplaceOptions<T> replaceOptions)
    {
        return FindOneAndReplaceAsync<TResult>(filter, replacement, replaceOptions, null);
    }

    public async Task<TResult> FindOneAndReplaceAsync<TResult>(Filter<T> filter, T replacement, ReplaceOptions<T> replaceOptions, CommandOptions commandOptions)
    {
        var response = await FindOneAndReplaceAsync<TResult>(filter, replacement, replaceOptions, commandOptions, false).ConfigureAwait(false);
        return response;
    }

    internal async Task<TResult> FindOneAndReplaceAsync<TResult>(Filter<T> filter, T replacement, ReplaceOptions<T> replaceOptions, CommandOptions commandOptions, bool runSynchronously)
    {
        replaceOptions.Filter = filter;
        replaceOptions.Replacement = replacement;
        var command = CreateCommand("findOneAndReplace").WithPayload(replaceOptions).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnData<DocumentResult<TResult>, T, ReplaceResult>(runSynchronously).ConfigureAwait(false);
        return response.Data.Document;
    }

    public ReplaceResult ReplaceOne(Filter<T> filter, T replacement)
    {
        return ReplaceOne(filter, replacement, new ReplaceOptions<T>(), null);
    }

    public ReplaceResult ReplaceOne(Filter<T> filter, T replacement, ReplaceOptions<T> replaceOptions)
    {
        return ReplaceOne(filter, replacement, replaceOptions, null);
    }

    public ReplaceResult ReplaceOne(Filter<T> filter, T replacement, ReplaceOptions<T> replaceOptions, CommandOptions commandOptions)
    {
        var response = ReplaceOneAsync(filter, replacement, replaceOptions, commandOptions, true).ResultSync();
        return response.Result;
    }

    public Task<ReplaceResult> ReplaceOneAsync(Filter<T> filter, T replacement)
    {
        return ReplaceOneAsync(filter, replacement, new ReplaceOptions<T>(), null);
    }

    public Task<ReplaceResult> ReplaceOneAsync(Filter<T> filter, T replacement, ReplaceOptions<T> replaceOptions)
    {
        return ReplaceOneAsync(filter, replacement, replaceOptions, null);
    }

    public async Task<ReplaceResult> ReplaceOneAsync(Filter<T> filter, T replacement, ReplaceOptions<T> replaceOptions, CommandOptions commandOptions)
    {
        var response = await ReplaceOneAsync(filter, replacement, replaceOptions, commandOptions, false).ConfigureAwait(false);
        return response.Result;
    }

    internal async Task<ApiResponseWithStatus<ReplaceResult>> ReplaceOneAsync(Filter<T> filter, T replacement, ReplaceOptions<T> replaceOptions, CommandOptions commandOptions, bool runSynchronously)
    {
        replaceOptions.Filter = filter;
        replaceOptions.Replacement = replacement;
        replaceOptions.Projection = new ExclusiveProjectionBuilder<T>().Exclude("*");
        var command = CreateCommand("findOneAndReplace").WithPayload(replaceOptions).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnStatus<ReplaceResult>(runSynchronously).ConfigureAwait(false);
        return response;
    }

    public T FindOneAndDelete()
    {
        return FindOneAndDelete(null, new FindOneAndDeleteOptions<T>(), null);
    }

    public T FindOneAndDelete(CommandOptions commandOptions)
    {
        return FindOneAndDelete(null, new FindOneAndDeleteOptions<T>(), commandOptions);
    }

    public T FindOneAndDelete(Filter<T> filter)
    {
        return FindOneAndDelete(filter, new FindOneAndDeleteOptions<T>(), null);
    }

    public T FindOneAndDelete(FindOneAndDeleteOptions<T> findOptions)
    {
        return FindOneAndDelete(null, findOptions, null);
    }

    public T FindOneAndDelete(FindOneAndDeleteOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOneAndDelete(null, findOptions, commandOptions);
    }

    public T FindOneAndDelete(Filter<T> filter, CommandOptions commandOptions)
    {
        return FindOneAndDelete(filter, new FindOneAndDeleteOptions<T>(), commandOptions);
    }

    public T FindOneAndDelete(Filter<T> filter, FindOneAndDeleteOptions<T> findOptions)
    {
        return FindOneAndDelete(filter, findOptions, null);
    }

    public T FindOneAndDelete(Filter<T> filter, FindOneAndDeleteOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOneAndDeleteAsync<T>(filter, findOptions, commandOptions, true).ResultSync();
    }

    public TResult FindOneAndDelete<TResult>()
    {
        return FindOneAndDelete<TResult>(null, new FindOneAndDeleteOptions<T>(), null);
    }

    public TResult FindOneAndDelete<TResult>(CommandOptions commandOptions)
    {
        return FindOneAndDelete<TResult>(null, new FindOneAndDeleteOptions<T>(), commandOptions);
    }

    public TResult FindOneAndDelete<TResult>(Filter<T> filter)
    {
        return FindOneAndDelete<TResult>(filter, new FindOneAndDeleteOptions<T>(), null);
    }

    public TResult FindOneAndDelete<TResult>(FindOneAndDeleteOptions<T> findOptions)
    {
        return FindOneAndDelete<TResult>(null, findOptions, null);
    }

    public TResult FindOneAndDelete<TResult>(FindOneAndDeleteOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOneAndDelete<TResult>(null, findOptions, commandOptions);
    }

    public TResult FindOneAndDelete<TResult>(Filter<T> filter, CommandOptions commandOptions)
    {
        return FindOneAndDelete<TResult>(filter, new FindOneAndDeleteOptions<T>(), commandOptions);
    }

    public TResult FindOneAndDelete<TResult>(Filter<T> filter, FindOneAndDeleteOptions<T> findOptions)
    {
        return FindOneAndDelete<TResult>(filter, findOptions, null);
    }

    public TResult FindOneAndDelete<TResult>(Filter<T> filter, FindOneAndDeleteOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOneAndDeleteAsync<TResult>(filter, findOptions, commandOptions, true).ResultSync();
    }

    public Task<T> FindOneAndDeleteAsync()
    {
        return FindOneAndDeleteAsync(null, new FindOneAndDeleteOptions<T>(), null);
    }

    public Task<T> FindOneAndDeleteAsync(CommandOptions commandOptions)
    {
        return FindOneAndDeleteAsync(null, new FindOneAndDeleteOptions<T>(), commandOptions);
    }

    public Task<T> FindOneAndDeleteAsync(FindOneAndDeleteOptions<T> findOptions)
    {
        return FindOneAndDeleteAsync(null, findOptions, null);
    }

    public Task<T> FindOneAndDeleteAsync(FindOneAndDeleteOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOneAndDeleteAsync(null, findOptions, commandOptions);
    }

    public Task<T> FindOneAndDeleteAsync(Filter<T> filter)
    {
        return FindOneAndDeleteAsync(filter, new FindOneAndDeleteOptions<T>(), null);
    }

    public Task<T> FindOneAndDeleteAsync(Filter<T> filter, CommandOptions commandOptions)
    {
        return FindOneAndDeleteAsync(filter, new FindOneAndDeleteOptions<T>(), commandOptions);
    }

    public Task<T> FindOneAndDeleteAsync(Filter<T> filter, FindOneAndDeleteOptions<T> findOptions)
    {
        return FindOneAndDeleteAsync(filter, findOptions, null);
    }

    public Task<T> FindOneAndDeleteAsync(Filter<T> filter, FindOneAndDeleteOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOneAndDeleteAsync<T>(filter, findOptions, commandOptions, false);
    }

    public Task<TResult> FindOneAndDeleteAsync<TResult>()
    {
        return FindOneAndDeleteAsync<TResult>(null, new FindOneAndDeleteOptions<T>(), null);
    }

    public Task<TResult> FindOneAndDeleteAsync<TResult>(CommandOptions commandOptions)
    {
        return FindOneAndDeleteAsync<TResult>(null, new FindOneAndDeleteOptions<T>(), commandOptions);
    }

    public Task<TResult> FindOneAndDeleteAsync<TResult>(FindOneAndDeleteOptions<T> findOptions)
    {
        return FindOneAndDeleteAsync<TResult>(null, findOptions, null);
    }

    public Task<TResult> FindOneAndDeleteAsync<TResult>(FindOneAndDeleteOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOneAndDeleteAsync<TResult>(null, findOptions, commandOptions);
    }

    public Task<TResult> FindOneAndDeleteAsync<TResult>(Filter<T> filter)
    {
        return FindOneAndDeleteAsync<TResult>(filter, new FindOneAndDeleteOptions<T>(), null);
    }

    public Task<TResult> FindOneAndDeleteAsync<TResult>(Filter<T> filter, CommandOptions commandOptions)
    {
        return FindOneAndDeleteAsync<TResult>(filter, new FindOneAndDeleteOptions<T>(), commandOptions);
    }

    public Task<TResult> FindOneAndDeleteAsync<TResult>(Filter<T> filter, FindOneAndDeleteOptions<T> findOptions)
    {
        return FindOneAndDeleteAsync<TResult>(filter, findOptions, null);
    }

    public Task<TResult> FindOneAndDeleteAsync<TResult>(Filter<T> filter, FindOneAndDeleteOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOneAndDeleteAsync<TResult>(filter, findOptions, commandOptions, false);
    }

    private async Task<TResult> FindOneAndDeleteAsync<TResult>(Filter<T> filter, FindOneAndDeleteOptions<T> findOptions, CommandOptions commandOptions, bool runSynchronously)
    {
        findOptions.Filter = filter;
        var command = CreateCommand("findOneAndDelete").WithPayload(findOptions).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnData<DocumentResult<TResult>, TResult, FindStatusResult>(runSynchronously).ConfigureAwait(false);
        return response.Data.Document;
    }

    public DeleteResult DeleteOne(DeleteOptions<T> deleteOptions)
    {
        return DeleteOne(null, deleteOptions, null);
    }

    public DeleteResult DeleteOne(Filter<T> filter)
    {
        return DeleteOne(filter, new DeleteOptions<T>(), null);
    }

    public DeleteResult DeleteOne(Filter<T> filter, CommandOptions commandOptions)
    {
        return DeleteOne(filter, new DeleteOptions<T>(), commandOptions);
    }

    public DeleteResult DeleteOne(DeleteOptions<T> deleteOptions, CommandOptions commandOptions)
    {
        return DeleteOne(null, deleteOptions, commandOptions);
    }

    public DeleteResult DeleteOne(Filter<T> filter, DeleteOptions<T> deleteOptions)
    {
        return DeleteOne(filter, deleteOptions, null);
    }

    public DeleteResult DeleteOne(Filter<T> filter, DeleteOptions<T> deleteOptions, CommandOptions commandOptions)
    {
        var response = DeleteOneAsync(filter, deleteOptions, commandOptions, true).ResultSync();
        return response;
    }

    public Task<DeleteResult> DeleteOneAsync(DeleteOptions<T> deleteOptions)
    {
        return DeleteOneAsync(null, deleteOptions, null);
    }

    public Task<DeleteResult> DeleteOneAsync(Filter<T> filter)
    {
        return DeleteOneAsync(filter, new DeleteOptions<T>(), null);
    }

    public Task<DeleteResult> DeleteOneAsync(Filter<T> filter, CommandOptions commandOptions)
    {
        return DeleteOneAsync(filter, new DeleteOptions<T>(), commandOptions);
    }

    public Task<DeleteResult> DeleteOneAsync(DeleteOptions<T> deleteOptions, CommandOptions commandOptions)
    {
        return DeleteOneAsync(null, deleteOptions, commandOptions);
    }

    public Task<DeleteResult> DeleteOneAsync(Filter<T> filter, DeleteOptions<T> deleteOptions)
    {
        return DeleteOneAsync(filter, deleteOptions, null);
    }

    public Task<DeleteResult> DeleteOneAsync(Filter<T> filter, DeleteOptions<T> deleteOptions, CommandOptions commandOptions)
    {
        return DeleteOneAsync(filter, deleteOptions, commandOptions, false);
    }

    internal async Task<DeleteResult> DeleteOneAsync(Filter<T> filter, DeleteOptions<T> deleteOptions, CommandOptions commandOptions, bool runSynchronously)
    {
        deleteOptions.Filter = filter;
        var command = CreateCommand("deleteOne").WithPayload(deleteOptions).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnStatus<DeleteResult>(runSynchronously).ConfigureAwait(false);
        return response.Result;
    }

    public DeleteResult DeleteAll()
    {
        return DeleteAll(null);
    }

    public DeleteResult DeleteAll(CommandOptions commandOptions)
    {
        return DeleteMany(null, commandOptions);
    }

    public DeleteResult DeleteMany(Filter<T> filter)
    {
        return DeleteMany(filter, null);
    }

    public DeleteResult DeleteMany(Filter<T> filter, CommandOptions commandOptions)
    {
        var response = DeleteManyAsync(filter, commandOptions, true).ResultSync();
        return response;
    }

    public Task<DeleteResult> DeleteAllAsync()
    {
        return DeleteManyAsync(null, null);
    }

    public Task<DeleteResult> DeleteAllAsync(CommandOptions commandOptions)
    {
        return DeleteManyAsync(null, commandOptions);
    }

    public Task<DeleteResult> DeleteManyAsync(Filter<T> filter)
    {
        return DeleteManyAsync(filter, null);
    }

    public Task<DeleteResult> DeleteManyAsync(Filter<T> filter, CommandOptions commandOptions)
    {
        return DeleteManyAsync(filter, commandOptions, false);
    }

    internal async Task<DeleteResult> DeleteManyAsync(Filter<T> filter, CommandOptions commandOptions, bool runSynchronously)
    {
        var deleteOptions = new DeleteManyOptions<T>
        {
            Filter = filter
        };

        var keepProcessing = true;
        var deleteResult = new DeleteResult();
        while (keepProcessing)
        {
            var command = CreateCommand("deleteMany").WithPayload(deleteOptions).AddCommandOptions(commandOptions);
            var response = await command.RunAsyncReturnStatus<DeleteResult>(runSynchronously).ConfigureAwait(false);
            deleteResult.DeletedCount += response.Result.DeletedCount;
            keepProcessing = response.Result.MoreData;
        }

        return deleteResult;
    }

    public UpdateResult UpdateOne(UpdateBuilder<T> update, UpdateOneOptions<T> updateOptions)
    {
        return UpdateOne(null, update, updateOptions, null);
    }

    public UpdateResult UpdateOne(UpdateBuilder<T> update, UpdateOneOptions<T> updateOptions, CommandOptions commandOptions)
    {
        return UpdateOne(null, update, updateOptions, commandOptions);
    }

    public UpdateResult UpdateOne(Filter<T> filter, UpdateBuilder<T> update)
    {
        return UpdateOne(filter, update, new UpdateOneOptions<T>(), null);
    }

    public UpdateResult UpdateOne(Filter<T> filter, UpdateBuilder<T> update, UpdateOneOptions<T> updateOptions)
    {
        return UpdateOne(filter, update, updateOptions, null);
    }

    public UpdateResult UpdateOne(Filter<T> filter, UpdateBuilder<T> update, UpdateOneOptions<T> updateOptions, CommandOptions commandOptions)
    {
        var response = UpdateOneAsync(filter, update, updateOptions, commandOptions, true).ResultSync();
        return response.Result;
    }

    public Task<UpdateResult> UpdateOneAsync(UpdateBuilder<T> update, UpdateOneOptions<T> updateOptions)
    {
        return UpdateOneAsync(null, update, updateOptions, null);
    }

    public Task<UpdateResult> UpdateOneAsync(UpdateBuilder<T> update, UpdateOneOptions<T> updateOptions, CommandOptions commandOptions)
    {
        return UpdateOneAsync(null, update, updateOptions, commandOptions);
    }

    public Task<UpdateResult> UpdateOneAsync(Filter<T> filter, UpdateBuilder<T> update)
    {
        return UpdateOneAsync(filter, update, new UpdateOneOptions<T>(), null);
    }

    public Task<UpdateResult> UpdateOneAsync(Filter<T> filter, UpdateBuilder<T> update, UpdateOneOptions<T> updateOptions)
    {
        return UpdateOneAsync(filter, update, updateOptions, null);
    }

    public async Task<UpdateResult> UpdateOneAsync(Filter<T> filter, UpdateBuilder<T> update, UpdateOneOptions<T> updateOptions, CommandOptions commandOptions)
    {
        var response = await UpdateOneAsync(filter, update, updateOptions, commandOptions, false).ConfigureAwait(false);
        return response.Result;
    }

    internal async Task<ApiResponseWithStatus<UpdateResult>> UpdateOneAsync(Filter<T> filter, UpdateBuilder<T> update, UpdateOneOptions<T> updateOptions, CommandOptions commandOptions, bool runSynchronously)
    {
        updateOptions.Filter = filter;
        updateOptions.Update = update;
        var command = CreateCommand("updateOne").WithPayload(updateOptions).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnStatus<UpdateResult>(runSynchronously).ConfigureAwait(false);
        return response;
    }

    public UpdateResult UpdateMany(Filter<T> filter, UpdateBuilder<T> update)
    {
        return UpdateMany(filter, update, new UpdateManyOptions<T>(), null);
    }

    public UpdateResult UpdateMany(Filter<T> filter, UpdateBuilder<T> update, UpdateManyOptions<T> updateOptions)
    {
        return UpdateMany(filter, update, updateOptions, null);
    }

    public UpdateResult UpdateMany(Filter<T> filter, UpdateBuilder<T> update, UpdateManyOptions<T> updateOptions, CommandOptions commandOptions)
    {
        var response = UpdateManyAsync(filter, update, updateOptions, commandOptions, false).ResultSync();
        return response.Result;
    }

    public Task<UpdateResult> UpdateManyAsync(Filter<T> filter, UpdateBuilder<T> update)
    {
        return UpdateManyAsync(filter, update, new UpdateManyOptions<T>(), null);
    }

    public Task<UpdateResult> UpdateManyAsync(Filter<T> filter, UpdateBuilder<T> update, UpdateManyOptions<T> updateOptions)
    {
        return UpdateManyAsync(filter, update, updateOptions, null);
    }

    public async Task<UpdateResult> UpdateManyAsync(Filter<T> filter, UpdateBuilder<T> update, UpdateManyOptions<T> updateOptions, CommandOptions commandOptions)
    {
        var response = await UpdateManyAsync(filter, update, updateOptions, commandOptions, false).ConfigureAwait(false);
        return response.Result;
    }

    internal async Task<ApiResponseWithStatus<UpdateResult>> UpdateManyAsync(Filter<T> filter, UpdateBuilder<T> update, UpdateManyOptions<T> updateOptions, CommandOptions commandOptions, bool runSynchronously)
    {
        updateOptions.Filter = filter;
        updateOptions.Update = update;

        var keepProcessing = true;
        var updateResult = new UpdateResult();
        string nextPageState = null;
        while (keepProcessing)
        {
            updateOptions ??= new UpdateManyOptions<T>();
            updateOptions.NextPageState = nextPageState;
            var command = CreateCommand("updateMany").WithPayload(updateOptions).AddCommandOptions(commandOptions);
            var response = await command.RunAsyncReturnStatus<PagedUpdateResult>(runSynchronously).ConfigureAwait(false);
            updateResult.MatchedCount += response.Result.MatchedCount;
            updateResult.ModifiedCount += response.Result.ModifiedCount;
            updateResult.UpsertedId = response.Result.UpsertedId;
            nextPageState = response.Result.NextPageState;
            if (string.IsNullOrEmpty(nextPageState))
            {
                keepProcessing = false;
            }
        }
        return new ApiResponseWithStatus<UpdateResult>() { Result = updateResult };
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

    public string CheckSerialization(T document)
    {
        return CheckSerialization(document, null);
    }

    public string CheckSerialization(T document, CommandOptions commandOptions)
    {
        var command = CreateCommand("checkSerialization").AddCommandOptions(commandOptions);
        var serializationOptions = new JsonSerializerOptions()
        {
            WriteIndented = true
        };
        return command.Serialize(document, serializationOptions, true);
    }

    public T CheckDeserialization(string json)
    {
        return CheckDeserialization(json, null);
    }

    public T CheckDeserialization(string json, CommandOptions commandOptions)
    {
        var command = CreateCommand("checkSerialization").AddCommandOptions(commandOptions);
        return command.Deserialize<T>(json);
    }

    internal Command CreateCommand(string name)
    {
        var optionsTree = _commandOptions == null ? _database.OptionsTree : _database.OptionsTree.Concat(new[] { _commandOptions }).ToArray();
        return new Command(name, _database.Client, optionsTree, new DatabaseCommandUrlBuilder(_database, _collectionName));
    }
}
