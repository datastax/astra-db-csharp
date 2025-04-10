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

/// <summary>
/// This is the entrypoint for interacting with an existing collection in a <see cref="Database"/>.
/// 
/// This base version handles serialization/deserialization as <see cref="Dictionary{String, Object}"/> via the <see cref="Document"/> class.
/// </summary>
public class Collection : Collection<Document>
{
    internal Collection(string collectionName, Database database, CommandOptions commandOptions)
        : base(collectionName, database, commandOptions) { }
}

/// <summary>
/// This is the entrypoint for interacting with an existing collection in a <see cref="Database"/>.
/// 
/// This version handles serialization/deserialization via a custom type <typeparamref name="T"/>.
/// 
/// Ids are expected to be of type <see cref="object"/>. It is recommended to use the strongly-typed version <see cref="Collection{T, TId}"/>.
/// </summary>
/// <typeparam name="T">The type of the documents in the collection.</typeparam>
public class Collection<T> : Collection<T, object> where T : class
{
    internal Collection(string collectionName, Database database, CommandOptions commandOptions)
        : base(collectionName, database, commandOptions) { }
}

/// <summary>
/// This is the entrypoint for interacting with an existing collection in a <see cref="Database"/>.
/// 
/// This version handles serialization/deserialization via a custom type <typeparamref name="T"/>.
/// 
/// Ids are strongly-typed as <typeparamref name="TId"/>.
/// </summary>
/// <typeparam name="T">The type of the documents in the collection.</typeparam>
/// <typeparam name="TId">The type of the id field for documents in the collection.</typeparam>
public class Collection<T, TId> where T : class
{
    private readonly string _collectionName;
    private readonly Database _database;
    private readonly CommandOptions _commandOptions;

    /// <summary>
    /// Access the name of the collection
    /// </summary>
    public string CollectionName => _collectionName;

    internal Collection(string collectionName, Database database, CommandOptions commandOptions)
    {
        Guard.NotNullOrEmpty(collectionName, nameof(collectionName));
        Guard.NotNull(database, nameof(database));
        _collectionName = collectionName;
        _database = database;
        _commandOptions = commandOptions;
    }

    /// <summary>
    /// Synchronous version of <see cref="InsertOneAsync(T)"/>
    /// </summary>
    /// <inheritdoc cref="InsertOneAsync(T)"/>
    public CollectionInsertOneResult<TId> InsertOne(T document)
    {
        return InsertOne(document, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="InsertOneAsync(T, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="InsertOneAsync(T, CommandOptions)"/>
    public CollectionInsertOneResult<TId> InsertOne(T document, CommandOptions commandOptions)
    {
        return InsertOneAsync(document, commandOptions, runSynchronously: true).ResultSync();
    }

    /// <summary>
    /// Asynchronously insert a single document into the collection.
    /// </summary>
    /// <param name="document">The document to insert.</param>
    /// <returns></returns>
    public Task<CollectionInsertOneResult<TId>> InsertOneAsync(T document)
    {
        return InsertOneAsync(document, new CommandOptions());
    }

    /// <inheritdoc cref="InsertOneAsync(T)"/>
    /// <param name="commandOptions"></param>
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

    /// <summary>
    /// Synchronous version of <see cref="InsertManyAsync(List{T})"/>
    /// </summary>
    /// <inheritdoc cref="InsertManyAsync(List{T})"/>
    public CollectionInsertManyResult<TId> InsertMany(List<T> documents)
    {
        return InsertMany(documents, null, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="InsertManyAsync(List{T}, InsertManyOptions)"/>
    /// </summary>
    /// <inheritdoc cref="InsertManyAsync(List{T}, InsertManyOptions)"/>
    public CollectionInsertManyResult<TId> InsertMany(List<T> documents, InsertManyOptions insertOptions)
    {
        return InsertMany(documents, insertOptions, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="InsertManyAsync(List{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="InsertManyAsync(List{T}, CommandOptions)"/>
    public CollectionInsertManyResult<TId> InsertMany(List<T> documents, CommandOptions commandOptions)
    {
        return InsertMany(documents, null, commandOptions);
    }

    /// <summary>
    /// Synchronous version of <see cref="InsertManyAsync(List{T}, InsertManyOptions, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="InsertManyAsync(List{T}, InsertManyOptions, CommandOptions)"/>
    public CollectionInsertManyResult<TId> InsertMany(List<T> documents, InsertManyOptions insertOptions, CommandOptions commandOptions)
    {
        return InsertManyAsync(documents, insertOptions, commandOptions, runSynchronously: true).ResultSync();
    }

    /// <summary>
    /// Asynchronously insert multiple documents into the collection.
    /// </summary>
    /// <param name="documents">The list of documents to insert.</param>
    /// <returns></returns>
    public Task<CollectionInsertManyResult<TId>> InsertManyAsync(List<T> documents)
    {
        return InsertManyAsync(documents, new CommandOptions());
    }

    /// <inheritdoc cref="InsertManyAsync(List{T})"/>
    /// <param name="insertOptions">Allows specifying whether the documents should be inserted in order as well as the chunk size.</param>
    public Task<CollectionInsertManyResult<TId>> InsertManyAsync(List<T> documents, InsertManyOptions insertOptions)
    {
        return InsertManyAsync(documents, insertOptions, null, runSynchronously: false);
    }

    /// <inheritdoc cref="InsertManyAsync(List{T})"/>
    /// <param name="commandOptions"></param>
    public Task<CollectionInsertManyResult<TId>> InsertManyAsync(List<T> documents, CommandOptions commandOptions)
    {
        return InsertManyAsync(documents, null, commandOptions, runSynchronously: false);
    }

    /// <inheritdoc cref="InsertManyAsync(List{T}, insertOptions)"/>
    /// <param name="commandOptions"></param>
    public Task<CollectionInsertManyResult<TId>> InsertManyAsync(List<T> documents, InsertManyOptions insertOptions, CommandOptions commandOptions)
    {
        return InsertManyAsync(documents, insertOptions, commandOptions, runSynchronously: false);
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

    /// <summary>
    /// Drops the collection from the database.
    /// </summary>
    public void Drop()
    {
        _database.DropCollection(_collectionName);
    }

    /// <summary>
    /// Asynchronously drops the collection from the database.
    /// </summary>
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

    /// <summary>
    /// Returns a single document from the collection.
    /// </summary>
    /// <returns></returns>
    public Task<T> FindOneAsync()
    {
        return FindOneAsync(null, new FindOptions<T>(), null);
    }

    /// <inheritdoc cref="FindOneAsync()"/>
    /// <param name="commandOptions"></param>
    public Task<T> FindOneAsync(CommandOptions commandOptions)
    {
        return FindOneAsync(null, new FindOptions<T>(), commandOptions);
    }

    /// <summary>
    /// Returns a single document from the collection based on the provided <see cref="FindOptions{T}"/>.
    /// This will return the first document found, most often used in conjunction with <see cref="FindOptions{T}.Sort"/>.
    /// See <see cref="FindOptions{T}"/> for more details on sorting, projecting and the other options for finding a document.
    /// </summary>
    /// <param name="findOptions"></param>
    /// <returns></returns>
    public Task<T> FindOneAsync(FindOptions<T> findOptions)
    {
        return FindOneAsync(null, findOptions, null);
    }

    /// <inheritdoc cref="FindOneAsync(FindOptions{T})"/>
    /// <param name="commandOptions"></param>
    public Task<T> FindOneAsync(FindOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOneAsync(null, findOptions, commandOptions);
    }

    /// <summary>
    /// Returns a single document from the collection based on the provided filter
    /// </summary>
    /// <param name="filter"></param>
    /// <returns></returns>
    /// <example>
    /// <code>
    /// var filter = Builders&lt;DifferentIdsObject&gt;.Filter.Eq(d => d.TheId, 1);
    /// var result = await collection.FindOneAsync(filter);
    /// </code>
    /// </example>
    public Task<T> FindOneAsync(Filter<T> filter)
    {
        return FindOneAsync(filter, new FindOptions<T>(), null);
    }

    /// <inheritdoc cref="FindOneAsync(Filter{T})"/>
    /// <param name="commandOptions"></param>
    public Task<T> FindOneAsync(Filter<T> filter, CommandOptions commandOptions)
    {
        return FindOneAsync(filter, new FindOptions<T>(), commandOptions);
    }

    /// <inheritdoc cref="FindOneAsync(Filter{T})"/>
    /// <param name="findOptions"></param>
    public Task<T> FindOneAsync(Filter<T> filter, FindOptions<T> findOptions)
    {
        return FindOneAsync(filter, findOptions, null);
    }

    /// <inheritdoc cref="FindOneAsync(Filter{T}, FindOptions{T})"/>
    /// <param name="commandOptions"></param>
    public Task<T> FindOneAsync(Filter<T> filter, FindOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOneAsync<T>(filter, findOptions, commandOptions, false);
    }

    /// <summary>
    /// Returns a single document from the collection.
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    /// <returns></returns>
    /// <remarks>
    /// The FindOneAsync alternatives that accept a TResult type parameter allow for deserializing the document as a different type
    /// (most commonly used when using projection to return a subset of fields)
    /// </remarks>
    public Task<TResult> FindOneAsync<TResult>()
    {
        return FindOneAsync<TResult>(null, new FindOptions<T>(), null);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}()"/>
    /// <param name="commandOptions"></param>
    public Task<TResult> FindOneAsync<TResult>(CommandOptions commandOptions)
    {
        return FindOneAsync<TResult>(null, new FindOptions<T>(), commandOptions);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}()"/>
    /// <param name="findOptions"></param>
    /// <example>
    /// <code>
    /// var exclusiveProjection = Builders&lt;FullObject&gt;.Projection
    ///     .Exclude("PropertyTwo");
    /// var findOptions = new FindOptions&lt;FullObject&gt;()
    /// {
    ///     Projection = exclusiveProjection
    /// };
    /// var result = await collection.FindOneAsync&lt;ObjectWithoutPropertyTwo&gt;(findOptions);
    /// </code>
    /// </example>
    public Task<TResult> FindOneAsync<TResult>(FindOptions<T> findOptions)
    {
        return FindOneAsync<TResult>(null, findOptions, null);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}(FindOptions{T})"/>
    /// <param name="commandOptions"></param>
    public Task<TResult> FindOneAsync<TResult>(FindOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOneAsync<TResult>(null, findOptions, commandOptions);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}()"/>
    /// <param name="commandOptions"></param>
    public Task<TResult> FindOneAsync<TResult>(Filter<T> filter, CommandOptions commandOptions)
    {
        return FindOneAsync<TResult>(filter, new FindOptions<T>(), commandOptions);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}(Filter{T})"/>
    /// <param name="findOptions"></param>
    public Task<TResult> FindOneAsync<TResult>(Filter<T> filter, FindOptions<T> findOptions)
    {
        return FindOneAsync<TResult>(filter, findOptions, null);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}(Filter{T}, FindOptions{T})"/>
    /// <param name="commandOptions"></param>
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

    /// <summary>
    /// Find all documents in the collection.
    /// 
    /// The Find() methods return a <see cref="FluentFind{T, TId, T}"/> object that can be used to further structure the query
    /// by adding Sort, Projection, Skip, Limit, etc. to affect the final results.
    /// 
    /// The <see cref="FluentFind{T, TId, T}"/> object can be directly enumerated both synchronously and asynchronously.
    /// Secondly, the results can be paged through more manually by using the <see cref="FluentFind{T, TId, T}.ToCursor()"/> method.
    /// </summary>
    /// <returns></returns>
    /// <example>
    /// Synchronous Enumeration:
    /// <code>
    /// var fluentFind = collection.Find();
    /// foreach (var document in fluentFind)
    /// {
    ///     // Process document
    /// }
    /// </code>
    /// </example>
    /// <example>
    /// Asynchronous Enumeration:
    /// <code>
    /// var results = collection.Find();
    /// await foreach (var document in results)
    /// {
    ///     // Process document
    /// }
    /// </code>
    /// </example>
    public FluentFind<T, TId, T> Find()
    {
        return Find(null, null, null);
    }

    /// <inheritdoc cref="Find()"/>
    /// <param name="filter"></param>
    /// <example>
    /// <code>
    /// var builder = Builders&lt;SimpleObject&gt;.Filter;
    /// var filter = builder.Gt(so => so.Properties.IntProperty, 20);
    /// var sort = Builders&lt;SimpleObject&gt;.Sort.Ascending(o => o.Properties.IntProperty);
    /// var results = collection.Find(filter).Sort(sort);
    /// </code>
    /// </example>
    public FluentFind<T, TId, T> Find(Filter<T> filter)
    {
        return Find(filter, null, null);
    }

    /// <summary>
    /// As an alternative to <see cref="Find()"/>, this method allows for controlling the results
    /// by setting properties on the <paramref name="findOptions"/> parameter.
    /// </summary>
    /// <inheritdoc cref="Find()"/>
    /// <param name="findOptions"></param>
    /// <example>
    /// <code>
    /// var filter = Builders&lt;SimpleObject&gt;.Filter.Eq(so => so.Properties.PropertyOne, "grouptwo");
    /// var sort = Builders&lt;SimpleObject&gt;.Sort.Descending(o => o.Properties.PropertyTwo);
    /// var inclusiveProjection = Builders&lt;SimpleObject&gt;.Projection
    ///     .Include("Properties.PropertyTwo");
    /// var findOptions = new FindOptions&lt;SimpleObject&gt;()
    /// {
    ///     Sort = sort,
    ///     Limit = 1,
    ///     Skip = 2,
    ///     Projection = inclusiveProjection
    /// };
    /// var results = collection.Find(filter, findOptions).ToList();
    /// </code>
    /// </example>
    public FluentFind<T, TId, T> Find(FindOptions<T> findOptions)
    {
        return Find(null, findOptions, null);
    }

    /// <inheritdoc cref="Find()"/>
    /// <param name="commandOptions"></param>
    public FluentFind<T, TId, T> Find(CommandOptions commandOptions)
    {
        return Find(null, new FindOptions<T>(), commandOptions);
    }

    /// <inheritdoc cref="Find(FindOptions{T})"/>
    /// <param name="commandOptions"></param>
    public FluentFind<T, TId, T> Find(FindOptions<T> findOptions, CommandOptions commandOptions)
    {
        return Find(null, findOptions, commandOptions);
    }

    /// <inheritdoc cref="Find(Filter{T})"/>
    /// <param name="commandOptions"></param>
    public FluentFind<T, TId, T> Find(Filter<T> filter, CommandOptions commandOptions)
    {
        return Find(filter, new FindOptions<T>(), commandOptions);
    }

    /// <inheritdoc cref="Find(Filter{T})"/>
    /// <param name="findOptions"></param>
    public FluentFind<T, TId, T> Find(Filter<T> filter, FindOptions<T> findOptions)
    {
        return Find(filter, findOptions, null);
    }

    /// <inheritdoc cref="Find(Filter{T}, FindOptions{T})"/>
    /// <param name="commandOptions"></param>
    public FluentFind<T, TId, T> Find(Filter<T> filter, FindOptions<T> findOptions, CommandOptions commandOptions)
    {
        return new FluentFind<T, TId, T>(this, filter, findOptions, commandOptions);
    }

    /// <inheritdoc cref="Find()"/>
    /// <remarks>
    /// The Find alternatives that accept a TResult type parameter allow for deserializing the document as a different type
    /// (most commonly used when using projection to return a subset of fields)
    /// </remarks>
    public FluentFind<T, TId, TResult> Find<TResult>() where TResult : class
    {
        return Find<TResult>(null, null, null);
    }

    /// <inheritdoc cref="Find(Filter{T})"/>
    /// <remarks>
    /// The Find alternatives that accept a TResult type parameter allow for deserializing the document as a different type
    /// (most commonly used when using projection to return a subset of fields)
    /// </remarks>
    public FluentFind<T, TId, TResult> Find<TResult>(Filter<T> filter) where TResult : class
    {
        return Find<TResult>(filter, null, null);
    }

    /// <inheritdoc cref="Find(FindOptions{T})"/>
    /// <remarks>
    /// The Find alternatives that accept a TResult type parameter allow for deserializing the document as a different type
    /// (most commonly used when using projection to return a subset of fields)
    /// </remarks>
    public FluentFind<T, TId, TResult> Find<TResult>(FindOptions<T> findOptions) where TResult : class
    {
        return Find<TResult>(null, findOptions, null);
    }

    /// <inheritdoc cref="Find(CommandOptions)"/>
    /// <remarks>
    /// The Find alternatives that accept a TResult type parameter allow for deserializing the document as a different type
    /// (most commonly used when using projection to return a subset of fields)
    /// </remarks>
    public FluentFind<T, TId, TResult> Find<TResult>(CommandOptions commandOptions) where TResult : class
    {
        return Find<TResult>(null, new FindOptions<T>(), commandOptions);
    }

    /// <inheritdoc cref="Find{TResult}(FindOptions{T})"/>
    /// <param name="commandOptions"></param>
    public FluentFind<T, TId, TResult> Find<TResult>(FindOptions<T> findOptions, CommandOptions commandOptions) where TResult : class
    {
        return Find<TResult>(null, findOptions, commandOptions);
    }

    /// <inheritdoc cref="Find{TResult}(Filter{T})"/>
    /// <param name="commandOptions"></param>
    public FluentFind<T, TId, TResult> Find<TResult>(Filter<T> filter, CommandOptions commandOptions) where TResult : class
    {
        return Find<TResult>(filter, new FindOptions<T>(), commandOptions);
    }

    /// <inheritdoc cref="Find{TResult}(Filter{T})"/>
    /// <param name="findOptions"></param>
    public FluentFind<T, TId, TResult> Find<TResult>(Filter<T> filter, FindOptions<T> findOptions) where TResult : class
    {
        return Find<TResult>(filter, findOptions, null);
    }

    /// <inheritdoc cref="Find{TResult}(Filter{T}, FindOptions{T})"/>
    /// <param name="commandOptions"></param>
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

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndUpdateAsync(Filter{T}, UpdateBuilder{T})"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndUpdateAsync(Filter{T}, UpdateBuilder{T})"/>
    public T FindOneAndUpdate(Filter<T> filter, UpdateBuilder<T> update)
    {
        return FindOneAndUpdate(filter, update, new FindOneAndUpdateOptions<T>(), null);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndUpdateAsync(Filter{T}, UpdateBuilder{T}, FindOneAndUpdateOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndUpdateAsync(Filter{T}, UpdateBuilder{T}, FindOneAndUpdateOptions{T})"/>
    public T FindOneAndUpdate(Filter<T> filter, UpdateBuilder<T> update, FindOneAndUpdateOptions<T> updateOptions)
    {
        return FindOneAndUpdate(filter, update, updateOptions, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndUpdateAsync(Filter{T}, UpdateBuilder{T}, FindOneAndUpdateOptions{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndUpdateAsync(Filter{T}, UpdateBuilder{T}, FindOneAndUpdateOptions{T}, CommandOptions)"/>
    public T FindOneAndUpdate(Filter<T> filter, UpdateBuilder<T> update, FindOneAndUpdateOptions<T> updateOptions, CommandOptions commandOptions)
    {
        var response = FindOneAndUpdateAsync<T>(filter, update, updateOptions, commandOptions, true).ResultSync();
        return response;
    }

    /// <summary>
    /// Finds a single document and update it using the provided updates.
    /// </summary>
    /// <param name="filter"></param>
    /// <param name="update"></param>
    /// <returns></returns>
    /// <example>
    /// <code>
    /// var updater = Builders&lt;SimpleObject&gt;.Update;
    /// var combinedUpdate = updater.Combine(
    ///     updater.Set(so => so.Properties.PropertyOne, "Updated"),
    ///     updater.Unset(so => so.Properties.PropertyTwo)
    /// );
    /// var result = await collection.FindOneAndUpdateAsync(filter, combinedUpdate);
    /// </code>
    /// </example>
    public Task<T> FindOneAndUpdateAsync(Filter<T> filter, UpdateBuilder<T> update)
    {
        return FindOneAndUpdateAsync(filter, update, new FindOneAndUpdateOptions<T>(), null);
    }

    /// <inheritdoc cref="FindOneAndUpdateAsync(Filter{T}, UpdateBuilder{T})"/>
    /// <param name="updateOptions">Set Sort, Projection, Upsert options</param>
    public Task<T> FindOneAndUpdateAsync(Filter<T> filter, UpdateBuilder<T> update, FindOneAndUpdateOptions<T> updateOptions)
    {
        return FindOneAndUpdateAsync(filter, update, updateOptions, null);
    }

    /// <inheritdoc cref="FindOneAndUpdateAsync(Filter{T}, UpdateBuilder{T}, FindOneAndUpdateOptions{T})"/>
    /// <param name="commandOptions"></param>
    public async Task<T> FindOneAndUpdateAsync(Filter<T> filter, UpdateBuilder<T> update, FindOneAndUpdateOptions<T> updateOptions, CommandOptions commandOptions)
    {
        var response = await FindOneAndUpdateAsync<T>(filter, update, updateOptions, commandOptions, false).ConfigureAwait(false);
        return response;
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndUpdateAsync(Filter{T}, UpdateBuilder{T})"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndUpdateAsync(Filter{T}, UpdateBuilder{T})"/>
    public TResult FindOneAndUpdate<TResult>(Filter<T> filter, UpdateBuilder<T> update)
    {
        return FindOneAndUpdate<TResult>(filter, update, new FindOneAndUpdateOptions<T>(), null);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndUpdateAsync(Filter{T}, UpdateBuilder{T}, FindOneAndUpdateOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndUpdateAsync(Filter{T}, UpdateBuilder{T}, FindOneAndUpdateOptions{T})"/>
    public TResult FindOneAndUpdate<TResult>(Filter<T> filter, UpdateBuilder<T> update, FindOneAndUpdateOptions<T> updateOptions)
    {
        return FindOneAndUpdate<TResult>(filter, update, updateOptions, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndUpdateAsync(Filter{T}, UpdateBuilder{T}, FindOneAndUpdateOptions{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndUpdateAsync(Filter{T}, UpdateBuilder{T}, FindOneAndUpdateOptions{T}, CommandOptions)"/>
    public TResult FindOneAndUpdate<TResult>(Filter<T> filter, UpdateBuilder<T> update, FindOneAndUpdateOptions<T> updateOptions, CommandOptions commandOptions)
    {
        var response = FindOneAndUpdateAsync<TResult>(filter, update, updateOptions, commandOptions, true).ResultSync();
        return response;
    }

    /// <inheritdoc cref="FindOneAndUpdateAsync(Filter{T}, UpdateBuilder{T})"/>
    /// <remarks>
    /// The FindOneAndUpdate alternatives that accept a TResult type parameter allow for deserializing the document as a different type
    /// (most commonly used when using projection to return a subset of fields)
    /// </remarks>
    public Task<TResult> FindOneAndUpdateAsync<TResult>(Filter<T> filter, UpdateBuilder<T> update)
    {
        return FindOneAndUpdateAsync<TResult>(filter, update, new FindOneAndUpdateOptions<T>(), null);
    }

    /// <inheritdoc cref="FindOneAndUpdateAsync{TResult}(Filter{T}, UpdateBuilder{T})"/>
    /// <param name="updateOptions">Set Sort, Projection, Upsert options</param>
    public Task<TResult> FindOneAndUpdateAsync<TResult>(Filter<T> filter, UpdateBuilder<T> update, FindOneAndUpdateOptions<T> updateOptions)
    {
        return FindOneAndUpdateAsync<TResult>(filter, update, updateOptions, null);
    }

    /// <inheritdoc cref="FindOneAndUpdateAsync{TResult}(Filter{T}, UpdateBuilder{T}, FindOneAndUpdateOptions{T})"/>
    /// <param name="commandOptions"></param>
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

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndReplaceAsync(T)"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndReplaceAsync(T)"/>
    public T FindOneAndReplace(T replacement)
    {
        return FindOneAndReplace(replacement, new ReplaceOptions<T>(), null);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndReplaceAsync(T, ReplaceOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndReplaceAsync(T, ReplaceOptions{T})"/>
    public T FindOneAndReplace(T replacement, ReplaceOptions<T> replaceOptions)
    {
        return FindOneAndReplace(replacement, replaceOptions, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndReplaceAsync(T, ReplaceOptions{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndReplaceAsync(T, ReplaceOptions{T}, CommandOptions)"/>
    public T FindOneAndReplace(T replacement, ReplaceOptions<T> replaceOptions, CommandOptions commandOptions)
    {
        var response = FindOneAndReplaceAsync<T>(null, replacement, replaceOptions, commandOptions, true).ResultSync();
        return response;
    }

    /// <summary>
    /// Find a document and replace it with the provided replacement
    /// </summary>
    /// <param name="replacement"></param>
    /// <returns></returns>
    public Task<T> FindOneAndReplaceAsync(T replacement)
    {
        return FindOneAndReplaceAsync(replacement, new ReplaceOptions<T>(), null);
    }

    /// <inheritdoc cref="FindOneAndReplaceAsync(T)"/>
    /// <param name="replaceOptions"></param>
    public Task<T> FindOneAndReplaceAsync(T replacement, ReplaceOptions<T> replaceOptions)
    {
        return FindOneAndReplaceAsync(replacement, replaceOptions, null);
    }

    /// <inheritdoc cref="FindOneAndReplaceAsync(T, ReplaceOptions{T})"/>
    /// <param name="commandOptions"></param>
    public async Task<T> FindOneAndReplaceAsync(T replacement, ReplaceOptions<T> replaceOptions, CommandOptions commandOptions)
    {
        var response = await FindOneAndReplaceAsync<T>(null, replacement, replaceOptions, commandOptions, false).ConfigureAwait(false);
        return response;
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndReplaceAsync{TResult}(T)"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndReplaceAsync{TResult}(T)"/>
    public TResult FindOneAndReplace<TResult>(T replacement)
    {
        return FindOneAndReplace<TResult>(replacement, new ReplaceOptions<T>(), null);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndReplaceAsync{TResult}(T, ReplaceOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndReplaceAsync{TResult}(T, ReplaceOptions{T})"/>
    public TResult FindOneAndReplace<TResult>(T replacement, ReplaceOptions<T> replaceOptions)
    {
        return FindOneAndReplace<TResult>(replacement, replaceOptions, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndReplaceAsync{TResult}(T, ReplaceOptions{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndReplaceAsync{TResult}(T, ReplaceOptions{T}, CommandOptions)"/>
    public TResult FindOneAndReplace<TResult>(T replacement, ReplaceOptions<T> replaceOptions, CommandOptions commandOptions)
    {
        var response = FindOneAndReplaceAsync<TResult>(null, replacement, replaceOptions, commandOptions, true).ResultSync();
        return response;
    }

    /// <inheritdoc cref="FindOneAndReplaceAsync(T)"/>
    /// <remarks>
    /// The FindOneAndReplace alternatives that accept a TResult type parameter allow for deserializing the resulting document
    /// as a different type (most commonly used when using projection to return a subset of fields)
    /// </remarks>
    public Task<TResult> FindOneAndReplaceAsync<TResult>(T replacement)
    {
        return FindOneAndReplaceAsync<TResult>(replacement, new ReplaceOptions<T>(), null);
    }

    /// <inheritdoc cref="FindOneAndReplaceAsync{TResult}(T)"/>
    /// <param name="replaceOptions"></param>
    public Task<TResult> FindOneAndReplaceAsync<TResult>(T replacement, ReplaceOptions<T> replaceOptions)
    {
        return FindOneAndReplaceAsync<TResult>(replacement, replaceOptions, null);
    }

    /// <inheritdoc cref="FindOneAndReplaceAsync{TResult}(T, ReplaceOptions{T})"/>
    /// <param name="commandOptions"></param>
    public async Task<TResult> FindOneAndReplaceAsync<TResult>(T replacement, ReplaceOptions<T> replaceOptions, CommandOptions commandOptions)
    {
        var response = await FindOneAndReplaceAsync<TResult>(null, replacement, replaceOptions, commandOptions, false).ConfigureAwait(false);
        return response;
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndReplaceAsync(T)"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndReplaceAsync(T)"/>
    public T FindOneAndReplace(Filter<T> filter, T replacement)
    {
        return FindOneAndReplace(filter, replacement, new ReplaceOptions<T>(), null);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndReplaceAsync(T, ReplaceOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndReplaceAsync(T, ReplaceOptions{T})"/>
    public T FindOneAndReplace(Filter<T> filter, T replacement, ReplaceOptions<T> replaceOptions)
    {
        return FindOneAndReplace(filter, replacement, replaceOptions, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndReplaceAsync(T, ReplaceOptions{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndReplaceAsync(T, ReplaceOptions{T}, CommandOptions)"/>
    public T FindOneAndReplace(Filter<T> filter, T replacement, ReplaceOptions<T> replaceOptions, CommandOptions commandOptions)
    {
        var response = FindOneAndReplaceAsync<T>(filter, replacement, replaceOptions, commandOptions, true).ResultSync();
        return response;
    }

    /// <summary>
    /// Find a document and replace it with the provided replacement using the provided filter
    /// </summary>
    /// <param name="filter"></param>
    /// <param name="replacement"></param>
    /// <returns></returns>
    public Task<T> FindOneAndReplaceAsync(Filter<T> filter, T replacement)
    {
        return FindOneAndReplaceAsync(filter, replacement, new ReplaceOptions<T>(), null);
    }

    /// <inheritdoc cref="FindOneAndReplaceAsync(Filter{T}, T)"/>
    /// <param name="replaceOptions"></param>
    public Task<T> FindOneAndReplaceAsync(Filter<T> filter, T replacement, ReplaceOptions<T> replaceOptions)
    {
        return FindOneAndReplaceAsync(filter, replacement, replaceOptions, null);
    }

    /// <inheritdoc cref="FindOneAndReplaceAsync(Filter{T}, T, ReplaceOptions{T})"/>
    /// <param name="commandOptions"></param>
    public async Task<T> FindOneAndReplaceAsync(Filter<T> filter, T replacement, ReplaceOptions<T> replaceOptions, CommandOptions commandOptions)
    {
        var response = await FindOneAndReplaceAsync<T>(filter, replacement, replaceOptions, commandOptions, false).ConfigureAwait(false);
        return response;
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndReplaceAsync{TResult}(T)"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndReplaceAsync{TResult}(T)"/>
    public TResult FindOneAndReplace<TResult>(Filter<T> filter, T replacement)
    {
        return FindOneAndReplace<TResult>(filter, replacement, new ReplaceOptions<T>(), null);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndReplaceAsync{TResult}(Filter{T}, T, ReplaceOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndReplaceAsync{TResult}(Filter{T}, T, ReplaceOptions{T})"/>
    public TResult FindOneAndReplace<TResult>(Filter<T> filter, T replacement, ReplaceOptions<T> replaceOptions)
    {
        return FindOneAndReplace<TResult>(filter, replacement, replaceOptions, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndReplaceAsync{TResult}(Filter{T}, T, ReplaceOptions{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndReplaceAsync{TResult}(Filter{T}, T, ReplaceOptions{T}, CommandOptions)"/>
    public TResult FindOneAndReplace<TResult>(Filter<T> filter, T replacement, ReplaceOptions<T> replaceOptions, CommandOptions commandOptions)
    {
        var response = FindOneAndReplaceAsync<TResult>(filter, replacement, replaceOptions, commandOptions, true).ResultSync();
        return response;
    }

    /// <inheritdoc cref="FindOneAndReplaceAsync(Filter{T}, T)"/>
    /// <remarks>
    /// The FindOneAndReplace alternatives that accept a TResult type parameter allow for deserializing the resulting document
    /// as a different type (most commonly used when using projection to return a subset of fields)
    /// </remarks>
    public Task<TResult> FindOneAndReplaceAsync<TResult>(Filter<T> filter, T replacement)
    {
        return FindOneAndReplaceAsync<TResult>(filter, replacement, new ReplaceOptions<T>(), null);
    }

    /// <inheritdoc cref="FindOneAndReplaceAsync(Filter{T}, T)"/>
    /// <remarks>
    /// The FindOneAndReplace alternatives that accept a TResult type parameter allow for deserializing the resulting document
    /// as a different type (most commonly used when using projection to return a subset of fields)
    /// </remarks>
    public Task<TResult> FindOneAndReplaceAsync<TResult>(Filter<T> filter, T replacement, ReplaceOptions<T> replaceOptions)
    {
        return FindOneAndReplaceAsync<TResult>(filter, replacement, replaceOptions, null);
    }

    /// <inheritdoc cref="FindOneAndReplaceAsync(Filter{T}, T, ReplaceOptions{T}, CommandOptions)"/>
    /// <remarks>
    /// The FindOneAndReplace alternatives that accept a TResult type parameter allow for deserializing the resulting document
    /// as a different type (most commonly used when using projection to return a subset of fields)
    /// </remarks>
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

    /// <summary>
    /// Synchronous version of <see cref="ReplaceOneAsync(Filter{T}, T)"/>
    /// </summary>
    /// <inheritdoc cref="ReplaceOneAsync(Filter{T}, T)"/>
    public ReplaceResult ReplaceOne(Filter<T> filter, T replacement)
    {
        return ReplaceOne(filter, replacement, new ReplaceOptions<T>(), null);
    }

    /// <summary>
    /// Synchronous version of <see cref="ReplaceOneAsync(Filter{T}, T, ReplaceOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="ReplaceOneAsync(Filter{T}, T, ReplaceOptions{T})"/>
    public ReplaceResult ReplaceOne(Filter<T> filter, T replacement, ReplaceOptions<T> replaceOptions)
    {
        return ReplaceOne(filter, replacement, replaceOptions, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="ReplaceOneAsync(Filter{T}, T, ReplaceOptions{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="ReplaceOneAsync(Filter{T}, T, ReplaceOptions{T}, CommandOptions)"/>
    public ReplaceResult ReplaceOne(Filter<T> filter, T replacement, ReplaceOptions<T> replaceOptions, CommandOptions commandOptions)
    {
        var response = ReplaceOneAsync(filter, replacement, replaceOptions, commandOptions, true).ResultSync();
        return response.Result;
    }

    /// <summary>
    /// Replace a document in the collection that matches the provided filter with the provided replacement.
    /// This is similar to <see cref="FindOneAndReplaceAsync(Filter{T},T)"/> but does not return the replaced document.
    /// </summary>
    /// <param name="filter"></param>
    /// <param name="replacement"></param>
    /// <returns></returns>
    public Task<ReplaceResult> ReplaceOneAsync(Filter<T> filter, T replacement)
    {
        return ReplaceOneAsync(filter, replacement, new ReplaceOptions<T>(), null);
    }

    /// <inheritdoc cref="ReplaceOneAsync(Filter{T}, T)"/>
    /// <param name="replaceOptions"></param>
    public Task<ReplaceResult> ReplaceOneAsync(Filter<T> filter, T replacement, ReplaceOptions<T> replaceOptions)
    {
        return ReplaceOneAsync(filter, replacement, replaceOptions, null);
    }

    /// <inheritdoc cref="ReplaceOneAsync(Filter{T}, T, ReplaceOptions{T})"/>
    /// <param name="commandOptions"></param>
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

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndDeleteAsync()"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndDeleteAsync()"/>
    public T FindOneAndDelete()
    {
        return FindOneAndDelete(null, new FindOneAndDeleteOptions<T>(), null);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndDeleteAsync(CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndDeleteAsync(CommandOptions)"/>
    public T FindOneAndDelete(CommandOptions commandOptions)
    {
        return FindOneAndDelete(null, new FindOneAndDeleteOptions<T>(), commandOptions);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndDeleteAsync(Filter{T})"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndDeleteAsync(Filter{T})"/>
    public T FindOneAndDelete(Filter<T> filter)
    {
        return FindOneAndDelete(filter, new FindOneAndDeleteOptions<T>(), null);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndDeleteAsync(FindOneAndDeleteOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndDeleteAsync(FindOneAndDeleteOptions{T})"/>
    public T FindOneAndDelete(FindOneAndDeleteOptions<T> findOptions)
    {
        return FindOneAndDelete(null, findOptions, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndDeleteAsync(FindOneAndDeleteOptions{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndDeleteAsync(FindOneAndDeleteOptions{T}, CommandOptions)"/>
    public T FindOneAndDelete(FindOneAndDeleteOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOneAndDelete(null, findOptions, commandOptions);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndDeleteAsync(Filter{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndDeleteAsync(Filter{T}, CommandOptions)"/>
    public T FindOneAndDelete(Filter<T> filter, CommandOptions commandOptions)
    {
        return FindOneAndDelete(filter, new FindOneAndDeleteOptions<T>(), commandOptions);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndDeleteAsync(Filter{T}, FindOneAndDeleteOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndDeleteAsync(Filter{T}, FindOneAndDeleteOptions{T})"/>
    public T FindOneAndDelete(Filter<T> filter, FindOneAndDeleteOptions<T> findOptions)
    {
        return FindOneAndDelete(filter, findOptions, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndDeleteAsync(Filter{T}, FindOneAndDeleteOptions{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndDeleteAsync(Filter{T}, FindOneAndDeleteOptions{T}, CommandOptions)"/>
    public T FindOneAndDelete(Filter<T> filter, FindOneAndDeleteOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOneAndDeleteAsync<T>(filter, findOptions, commandOptions, true).ResultSync();
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndDeleteAsync{TResult}()"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndDeleteAsync{TResult}()"/>
    public TResult FindOneAndDelete<TResult>()
    {
        return FindOneAndDelete<TResult>(null, new FindOneAndDeleteOptions<T>(), null);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndDeleteAsync{TResult}(CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndDeleteAsync{TResult}(CommandOptions)"/>
    public TResult FindOneAndDelete<TResult>(CommandOptions commandOptions)
    {
        return FindOneAndDelete<TResult>(null, new FindOneAndDeleteOptions<T>(), commandOptions);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndDeleteAsync{TResult}(Filter{T})"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndDeleteAsync{TResult}(Filter{T})"/>
    public TResult FindOneAndDelete<TResult>(Filter<T> filter)
    {
        return FindOneAndDelete<TResult>(filter, new FindOneAndDeleteOptions<T>(), null);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndDeleteAsync{TResult}(FindOneAndDeleteOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndDeleteAsync{TResult}(FindOneAndDeleteOptions{T})"/>
    public TResult FindOneAndDelete<TResult>(FindOneAndDeleteOptions<T> findOptions)
    {
        return FindOneAndDelete<TResult>(null, findOptions, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndDeleteAsync{TResult}(FindOneAndDeleteOptions{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndDeleteAsync{TResult}(FindOneAndDeleteOptions{T}, CommandOptions)"/>
    public TResult FindOneAndDelete<TResult>(FindOneAndDeleteOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOneAndDelete<TResult>(null, findOptions, commandOptions);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndDeleteAsync{TResult}(Filter{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndDeleteAsync{TResult}(Filter{T}, CommandOptions)"/>
    public TResult FindOneAndDelete<TResult>(Filter<T> filter, CommandOptions commandOptions)
    {
        return FindOneAndDelete<TResult>(filter, new FindOneAndDeleteOptions<T>(), commandOptions);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndDeleteAsync{TResult}(Filter{T}, FindOneAndDeleteOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndDeleteAsync{TResult}(Filter{T}, FindOneAndDeleteOptions{T})"/>
    public TResult FindOneAndDelete<TResult>(Filter<T> filter, FindOneAndDeleteOptions<T> findOptions)
    {
        return FindOneAndDelete<TResult>(filter, findOptions, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndDeleteAsync{TResult}(Filter{T}, FindOneAndDeleteOptions{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndDeleteAsync{TResult}(Filter{T}, FindOneAndDeleteOptions{T}, CommandOptions)"/>
    public TResult FindOneAndDelete<TResult>(Filter<T> filter, FindOneAndDeleteOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOneAndDeleteAsync<TResult>(filter, findOptions, commandOptions, true).ResultSync();
    }

    /// <summary>
    /// Find a document and delete it from the collection
    /// </summary>
    /// <returns>The deleted document</returns>
    public Task<T> FindOneAndDeleteAsync()
    {
        return FindOneAndDeleteAsync(null, new FindOneAndDeleteOptions<T>(), null);
    }

    /// <inheritdoc cref="FindOneAndDeleteAsync()"/>
    /// <param name="commandOptions"></param>
    public Task<T> FindOneAndDeleteAsync(CommandOptions commandOptions)
    {
        return FindOneAndDeleteAsync(null, new FindOneAndDeleteOptions<T>(), commandOptions);
    }

    /// <inheritdoc cref="FindOneAndDeleteAsync()"/>
    /// <param name="findOptions"></param>
    public Task<T> FindOneAndDeleteAsync(FindOneAndDeleteOptions<T> findOptions)
    {
        return FindOneAndDeleteAsync(null, findOptions, null);
    }

    /// <inheritdoc cref="FindOneAndDeleteAsync(FindOneAndDeleteOptions{T})"/>
    /// <param name="commandOptions"></param>
    public Task<T> FindOneAndDeleteAsync(FindOneAndDeleteOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOneAndDeleteAsync(null, findOptions, commandOptions);
    }

    /// <inheritdoc cref="FindOneAndDeleteAsync()"/>
    /// <param name="filter"></param>
    public Task<T> FindOneAndDeleteAsync(Filter<T> filter)
    {
        return FindOneAndDeleteAsync(filter, new FindOneAndDeleteOptions<T>(), null);
    }

    /// <inheritdoc cref="FindOneAndDeleteAsync(Filter{T})"/>
    /// <param name="commandOptions"></param>
    public Task<T> FindOneAndDeleteAsync(Filter<T> filter, CommandOptions commandOptions)
    {
        return FindOneAndDeleteAsync(filter, new FindOneAndDeleteOptions<T>(), commandOptions);
    }

    /// <inheritdoc cref="FindOneAndDeleteAsync(Filter{T})"/>
    /// <param name="findOptions"></param>
    public Task<T> FindOneAndDeleteAsync(Filter<T> filter, FindOneAndDeleteOptions<T> findOptions)
    {
        return FindOneAndDeleteAsync(filter, findOptions, null);
    }

    /// <inheritdoc cref="FindOneAndDeleteAsync(Filter{T}, FindOneAndDeleteOptions{T})"/>
    /// <param name="commandOptions"></param>
    public Task<T> FindOneAndDeleteAsync(Filter<T> filter, FindOneAndDeleteOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOneAndDeleteAsync<T>(filter, findOptions, commandOptions, false);
    }

    /// <inheritdoc cref="FindOneAndDeleteAsync()"/>
    /// <remarks>
    /// The FindOneAndDeleteAsync alternatives that accept a TResult type parameter allow for deserializing the resulting document
    /// as a different type (most commonly used when using projection to return a subset of fields)
    /// </remarks>
    public Task<TResult> FindOneAndDeleteAsync<TResult>()
    {
        return FindOneAndDeleteAsync<TResult>(null, new FindOneAndDeleteOptions<T>(), null);
    }

    /// <inheritdoc cref="FindOneAndDeleteAsync{TResult}()"/>
    /// <param name="commandOptions"></param>
    public Task<TResult> FindOneAndDeleteAsync<TResult>(CommandOptions commandOptions)
    {
        return FindOneAndDeleteAsync<TResult>(null, new FindOneAndDeleteOptions<T>(), commandOptions);
    }

    /// <inheritdoc cref="FindOneAndDeleteAsync{TResult}()"/>
    /// <param name="findOptions"></param>
    public Task<TResult> FindOneAndDeleteAsync<TResult>(FindOneAndDeleteOptions<T> findOptions)
    {
        return FindOneAndDeleteAsync<TResult>(null, findOptions, null);
    }

    /// <inheritdoc cref="FindOneAndDeleteAsync{TResult}(FindOneAndDeleteOptions{T})"/>
    /// <param name="commandOptions"></param>
    public Task<TResult> FindOneAndDeleteAsync<TResult>(FindOneAndDeleteOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOneAndDeleteAsync<TResult>(null, findOptions, commandOptions);
    }

    /// <inheritdoc cref="FindOneAndDeleteAsync{TResult}()"/>
    /// <param name="filter"></param>
    public Task<TResult> FindOneAndDeleteAsync<TResult>(Filter<T> filter)
    {
        return FindOneAndDeleteAsync<TResult>(filter, new FindOneAndDeleteOptions<T>(), null);
    }

    /// <inheritdoc cref="FindOneAndDeleteAsync{TResult}(Filter{T})"/>
    /// <param name="commandOptions"></param>
    public Task<TResult> FindOneAndDeleteAsync<TResult>(Filter<T> filter, CommandOptions commandOptions)
    {
        return FindOneAndDeleteAsync<TResult>(filter, new FindOneAndDeleteOptions<T>(), commandOptions);
    }

    /// <inheritdoc cref="FindOneAndDeleteAsync{TResult}(Filter{T})"/>
    /// <param name="findOptions"></param>
    public Task<TResult> FindOneAndDeleteAsync<TResult>(Filter<T> filter, FindOneAndDeleteOptions<T> findOptions)
    {
        return FindOneAndDeleteAsync<TResult>(filter, findOptions, null);
    }

    /// <inheritdoc cref="FindOneAndDeleteAsync{TResult}(Filter{T}, FindOneAndDeleteOptions{T})"/>
    /// <param name="commandOptions"></param>
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

    /// <summary>
    /// This is a synchronous version of <see cref="DeleteOneAsync(DeleteOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="DeleteOneAsync(DeleteOptions{T})"/>
    public DeleteResult DeleteOne(DeleteOptions<T> deleteOptions)
    {
        return DeleteOne(null, deleteOptions, null);
    }

    /// <summary>
    /// This is a synchronous version of <see cref="DeleteOneAsync(Filter{T})"/>
    /// </summary>
    /// <inheritdoc cref="DeleteOneAsync(Filter{T})"/>
    public DeleteResult DeleteOne(Filter<T> filter)
    {
        return DeleteOne(filter, new DeleteOptions<T>(), null);
    }

    /// <summary>
    /// This is a synchronous version of <see cref="DeleteOneAsync(Filter{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="DeleteOneAsync(Filter{T}, CommandOptions)"/>
    public DeleteResult DeleteOne(Filter<T> filter, CommandOptions commandOptions)
    {
        return DeleteOne(filter, new DeleteOptions<T>(), commandOptions);
    }

    /// <summary>
    /// This is a synchronous version of <see cref="DeleteOneAsync(DeleteOptions{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="DeleteOneAsync(DeleteOptions{T}, CommandOptions)"/>
    public DeleteResult DeleteOne(DeleteOptions<T> deleteOptions, CommandOptions commandOptions)
    {
        return DeleteOne(null, deleteOptions, commandOptions);
    }

    /// <summary>
    /// This is a synchronous version of <see cref="DeleteOneAsync(Filter{T}, DeleteOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="DeleteOneAsync(Filter{T}, DeleteOptions{T})"/>
    public DeleteResult DeleteOne(Filter<T> filter, DeleteOptions<T> deleteOptions)
    {
        return DeleteOne(filter, deleteOptions, null);
    }

    /// <summary>
    /// This is a synchronous version of <see cref="DeleteOneAsync(Filter{T}, DeleteOptions{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="DeleteOneAsync(Filter{T}, DeleteOptions{T}, CommandOptions)"/>
    public DeleteResult DeleteOne(Filter<T> filter, DeleteOptions<T> deleteOptions, CommandOptions commandOptions)
    {
        var response = DeleteOneAsync(filter, deleteOptions, commandOptions, true).ResultSync();
        return response;
    }

    /// <summary>
    /// Delete a document from the collection.
    /// This is similar to <see cref="FindOneAndDeleteAsync(FindOneAndDeleteOptions{T}"/> but does not return the deleted document.
    /// </summary>
    /// <param name="deleteOptions"></param>
    /// <returns></returns>
    public Task<DeleteResult> DeleteOneAsync(DeleteOptions<T> deleteOptions)
    {
        return DeleteOneAsync(null, deleteOptions, null);
    }

    /// <summary>
    /// Delete a document from the collection.
    /// This is similar to <see cref="FindOneAndDeleteAsync(Filter{T})"/> but does not return the deleted document.
    /// </summary>
    /// <param name="filter"></param>
    /// <returns></returns>
    public Task<DeleteResult> DeleteOneAsync(Filter<T> filter)
    {
        return DeleteOneAsync(filter, new DeleteOptions<T>(), null);
    }

    /// <inheritdoc cref="DeleteOneAsync(Filter{T})"/>
    /// <param name="commandOptions"></param>
    public Task<DeleteResult> DeleteOneAsync(Filter<T> filter, CommandOptions commandOptions)
    {
        return DeleteOneAsync(filter, new DeleteOptions<T>(), commandOptions);
    }

    /// <inheritdoc cref="DeleteOneAsync(DeleteOptions{T})"/>
    /// <param name="commandOptions"></param>
    public Task<DeleteResult> DeleteOneAsync(DeleteOptions<T> deleteOptions, CommandOptions commandOptions)
    {
        return DeleteOneAsync(null, deleteOptions, commandOptions);
    }

    /// <inheritdoc cref="DeleteOneAsync(Filter{T})"/>
    /// <param name="deleteOptions"></param>
    public Task<DeleteResult> DeleteOneAsync(Filter<T> filter, DeleteOptions<T> deleteOptions)
    {
        return DeleteOneAsync(filter, deleteOptions, null);
    }

    /// <inheritdoc cref="DeleteOneAsync(Filter{T}, DeleteOptions{T})"/>
    /// <param name="commandOptions"></param>
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

    /// <summary>
    /// Synchronous version of <see cref="DeleteAllAsync()"/>
    /// </summary>
    /// <inheritdoc cref="DeleteAllAsync()"/>
    public DeleteResult DeleteAll()
    {
        return DeleteAll(null);
    }

    /// <summary>
    /// Synchronous version of <see cref="DeleteAllAsync(CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="DeleteAllAsync(CommandOptions)"/>
    public DeleteResult DeleteAll(CommandOptions commandOptions)
    {
        return DeleteMany(null, commandOptions);
    }

    /// <summary>
    /// Synchronous version of <see cref="DeleteManyAsync(Filter{T})"/>
    /// </summary>
    /// <inheritdoc cref="DeleteManyAsync(Filter{T})"/>
    public DeleteResult DeleteMany(Filter<T> filter)
    {
        return DeleteMany(filter, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="DeleteManyAsync(Filter{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="DeleteManyAsync(Filter{T}, CommandOptions)"/>
    public DeleteResult DeleteMany(Filter<T> filter, CommandOptions commandOptions)
    {
        var response = DeleteManyAsync(filter, commandOptions, true).ResultSync();
        return response;
    }

    /// <summary>
    /// Delete all documents from the collection.
    /// </summary>
    /// <returns></returns>
    public Task<DeleteResult> DeleteAllAsync()
    {
        return DeleteManyAsync(null, null);
    }

    /// <inheritdoc cref="DeleteAllAsync()"/>
    /// <param name="commandOptions"></param>
    public Task<DeleteResult> DeleteAllAsync(CommandOptions commandOptions)
    {
        return DeleteManyAsync(null, commandOptions);
    }

    /// <summary>
    /// Delete all documents matching the filter from the collection.
    /// </summary>
    /// <param name="filter"></param>
    /// <returns></returns>
    public Task<DeleteResult> DeleteManyAsync(Filter<T> filter)
    {
        return DeleteManyAsync(filter, null);
    }

    /// <inheritdoc cref="DeleteManyAsync(Filter{T})"/>
    /// <param name="commandOptions"></param>
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

    /// <summary>
    /// Synchronous version of <see cref="UpdateOneAsync(UpdateBuilder{T}, UpdateOneOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="UpdateOneAsync(UpdateBuilder{T}, UpdateOneOptions{T})"/>
    public UpdateResult UpdateOne(UpdateBuilder<T> update, UpdateOneOptions<T> updateOptions)
    {
        return UpdateOne(null, update, updateOptions, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="UpdateOneAsync(UpdateBuilder{T}, UpdateOneOptions{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="UpdateOneAsync(UpdateBuilder{T}, UpdateOneOptions{T}, CommandOptions)"/>
    public UpdateResult UpdateOne(UpdateBuilder<T> update, UpdateOneOptions<T> updateOptions, CommandOptions commandOptions)
    {
        return UpdateOne(null, update, updateOptions, commandOptions);
    }

    /// <summary>
    /// Synchronous version of <see cref="UpdateOneAsync(Filter{T}, UpdateBuilder{T})"/>
    /// </summary>
    /// <inheritdoc cref="UpdateOneAsync(Filter{T}, UpdateBuilder{T})"/>
    public UpdateResult UpdateOne(Filter<T> filter, UpdateBuilder<T> update)
    {
        return UpdateOne(filter, update, new UpdateOneOptions<T>(), null);
    }

    /// <summary>
    /// Synchronous version of <see cref="UpdateOneAsync(Filter{T}, UpdateBuilder{T}, UpdateOneOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="UpdateOneAsync(Filter{T}, UpdateBuilder{T}, UpdateOneOptions{T})"/>
    public UpdateResult UpdateOne(Filter<T> filter, UpdateBuilder<T> update, UpdateOneOptions<T> updateOptions)
    {
        return UpdateOne(filter, update, updateOptions, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="UpdateOneAsync(Filter{T}, UpdateBuilder{T}, UpdateOneOptions{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="UpdateOneAsync(Filter{T}, UpdateBuilder{T}, UpdateOneOptions{T}, CommandOptions)"/>
    public UpdateResult UpdateOne(Filter<T> filter, UpdateBuilder<T> update, UpdateOneOptions<T> updateOptions, CommandOptions commandOptions)
    {
        var response = UpdateOneAsync(filter, update, updateOptions, commandOptions, true).ResultSync();
        return response.Result;
    }

    /// <summary>
    /// Update a single document in the collection using the provided update builder and options.
    /// 
    /// This is similar to <see cref="FindOneAndUpdateAsync(Filter{T}, UpdateBuilder{T})"/> but does not return the updated document.
    /// </summary>
    /// <param name="update"></param>
    /// <param name="updateOptions"></param>
    /// <returns></returns>
    public Task<UpdateResult> UpdateOneAsync(UpdateBuilder<T> update, UpdateOneOptions<T> updateOptions)
    {
        return UpdateOneAsync(null, update, updateOptions, null);
    }

    /// <inheritdoc cref="UpdateOneAsync(UpdateBuilder{T}, UpdateOneOptions{T})"/>
    /// <param name="commandOptions"></param>
    public Task<UpdateResult> UpdateOneAsync(UpdateBuilder<T> update, UpdateOneOptions<T> updateOptions, CommandOptions commandOptions)
    {
        return UpdateOneAsync(null, update, updateOptions, commandOptions);
    }

    /// <summary>
    /// Update a single document in the collection using the provided filter and update builder.
    /// 
    /// This is similar to <see cref="FindOneAndUpdateAsync(Filter{T}, UpdateBuilder{T})"/> but does not return the updated document.
    /// </summary>
    /// <param name="filter"></param>
    /// <param name="update"></param>
    /// <returns></returns>
    public Task<UpdateResult> UpdateOneAsync(Filter<T> filter, UpdateBuilder<T> update)
    {
        return UpdateOneAsync(filter, update, new UpdateOneOptions<T>(), null);
    }

    /// <inheritdoc cref="UpdateOneAsync(Filter{T}, UpdateBuilder{T})"/>
    /// <param name="updateOptions"></param>
    public Task<UpdateResult> UpdateOneAsync(Filter<T> filter, UpdateBuilder<T> update, UpdateOneOptions<T> updateOptions)
    {
        return UpdateOneAsync(filter, update, updateOptions, null);
    }

    /// <inheritdoc cref="UpdateOneAsync(Filter{T}, UpdateBuilder{T}, UpdateOneOptions{T})"/>
    /// <param name="commandOptions"></param>
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

    /// <summary>
    /// Synchronous version of <see cref="UpdateManyAsync(Filter{T}, UpdateBuilder{T})"/>
    /// </summary>
    /// <inheritdoc cref="UpdateManyAsync(Filter{T}, UpdateBuilder{T})"/>
    public UpdateResult UpdateMany(Filter<T> filter, UpdateBuilder<T> update)
    {
        return UpdateMany(filter, update, new UpdateManyOptions<T>(), null);
    }

    /// <summary>
    /// Synchronous version of <see cref="UpdateManyAsync(Filter{T}, UpdateBuilder{T}, UpdateManyOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="UpdateManyAsync(Filter{T}, UpdateBuilder{T}, UpdateManyOptions{T})"/>
    public UpdateResult UpdateMany(Filter<T> filter, UpdateBuilder<T> update, UpdateManyOptions<T> updateOptions)
    {
        return UpdateMany(filter, update, updateOptions, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="UpdateManyAsync(Filter{T}, UpdateBuilder{T}, UpdateManyOptions{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="UpdateManyAsync(Filter{T}, UpdateBuilder{T}, UpdateManyOptions{T}, CommandOptions)"/>
    public UpdateResult UpdateMany(Filter<T> filter, UpdateBuilder<T> update, UpdateManyOptions<T> updateOptions, CommandOptions commandOptions)
    {
        var response = UpdateManyAsync(filter, update, updateOptions, commandOptions, false).ResultSync();
        return response.Result;
    }

    /// <summary>
    /// Update all documents matching the filter by applying the provided updates.
    /// </summary>
    /// <param name="filter"></param>
    /// <param name="update"></param>
    /// <returns></returns>
    public Task<UpdateResult> UpdateManyAsync(Filter<T> filter, UpdateBuilder<T> update)
    {
        return UpdateManyAsync(filter, update, new UpdateManyOptions<T>(), null);
    }

    /// <inheritdoc cref="UpdateManyAsync(Filter{T}, UpdateBuilder{T})"/>
    /// <param name="updateOptions"></param>
    public Task<UpdateResult> UpdateManyAsync(Filter<T> filter, UpdateBuilder<T> update, UpdateManyOptions<T> updateOptions)
    {
        return UpdateManyAsync(filter, update, updateOptions, null);
    }

    /// <inheritdoc cref="UpdateManyAsync(Filter{T}, UpdateBuilder{T}, UpdateManyOptions{T})"/>
    /// <param name="commandOptions"></param>
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

    /// <summary>
    /// Synchronous version of <see cref="CountDocumentsAsync()"/>
    /// </summary>
    /// <inheritdoc cref="CountDocumentsAsync()"/>
    public DocumentsCountResult CountDocuments()
    {
        return CountDocumentsAsync(null, null, true).ResultSync();
    }

    /// <summary>
    /// Synchronous version of <see cref="CountDocumentsAsync(CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CountDocumentsAsync(CommandOptions)"/>
    public DocumentsCountResult CountDocuments(CommandOptions commandOptions)
    {
        return CountDocumentsAsync(null, commandOptions, true).ResultSync();
    }

    /// <summary>
    /// Count the number of documents in the collection.
    /// </summary>
    /// <returns></returns>
    public Task<DocumentsCountResult> CountDocumentsAsync()
    {
        return CountDocumentsAsync(null, null, false);
    }

    /// <summary>
    /// Count the number of documents in the collection.
    /// </summary>
    /// <param name="commandOptions"></param>
    /// <returns></returns>
    public Task<DocumentsCountResult> CountDocumentsAsync(CommandOptions commandOptions)
    {
        return CountDocumentsAsync(null, commandOptions, false);
    }

    /// <summary>
    /// Synchronous version of <see cref="CountDocumentsAsync(Filter{T})"/>
    /// </summary>
    /// <inheritdoc cref="CountDocumentsAsync(Filter{T})"/>
    public DocumentsCountResult CountDocuments(Filter<T> filter)
    {
        return CountDocumentsAsync(filter, null, true).ResultSync();
    }

    /// <summary>
    /// Synchronous version of <see cref="CountDocumentsAsync(Filter{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CountDocumentsAsync(Filter{T}, CommandOptions)"/>
    public DocumentsCountResult CountDocuments(Filter<T> filter, CommandOptions commandOptions)
    {
        return CountDocumentsAsync(filter, commandOptions, true).ResultSync();
    }

    /// <summary>
    /// Count the number of documents in the collection that match the provided filter.
    /// </summary>
    /// <param name="filter"></param>
    /// <returns></returns>
    public Task<DocumentsCountResult> CountDocumentsAsync(Filter<T> filter)
    {
        return CountDocumentsAsync(filter, null, false);
    }

    /// <inheritdoc cref="CountDocumentsAsync(Filter{T}"/>
    /// <param name="commandOptions"></param>
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

    /// <summary>
    /// Synchronous version of <see cref="EstimateDocumentCountAsync()"/>
    /// </summary>
    /// <inheritdoc cref="EstimateDocumentCountAsync()"/>
    public EstimatedDocumentsCountResult EstimateDocumentCount()
    {
        return EstimateDocumentCountAsync(null, true).ResultSync();
    }

    /// <summary>
    /// Synchronous version of <see cref="EstimateDocumentCountAsync(CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="EstimateDocumentCountAsync(CommandOptions)"/>
    public EstimatedDocumentsCountResult EstimateDocumentCount(CommandOptions commandOptions)
    {
        return EstimateDocumentCountAsync(commandOptions, true).ResultSync();
    }

    /// <summary>
    /// Estimate the number of documents in the collection.
    /// </summary>
    /// <returns></returns>
    public Task<EstimatedDocumentsCountResult> EstimateDocumentCountAsync()
    {
        return EstimateDocumentCountAsync(null, false);
    }

    /// <summary>
    /// Estimate the number of documents in the collection.
    /// </summary>
    /// <param name="commandOptions"></param>
    /// <returns></returns>
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

    /// <summary>
    /// Test method that allows testing how a document will be serialized to the database.
    /// </summary>
    /// <param name="document"></param>
    /// <returns></returns>
    public string CheckSerialization(T document)
    {
        return CheckSerialization(document, null);
    }

    /// <summary>
    /// Test method that allows testing how a document will be serialized to the database.
    /// </summary>
    /// <param name="document"></param>
    /// <param name="commandOptions"></param>
    /// <returns></returns>
    public string CheckSerialization(T document, CommandOptions commandOptions)
    {
        var command = CreateCommand("checkSerialization").AddCommandOptions(commandOptions);
        var serializationOptions = new JsonSerializerOptions()
        {
            WriteIndented = true
        };
        return command.Serialize(document, serializationOptions, true);
    }

    /// <summary>
    /// Test method that allows testing how a document will be deserialized from the database.
    /// </summary>
    /// <param name="json"></param>
    /// <returns></returns>
    public T CheckDeserialization(string json)
    {
        return CheckDeserialization(json, null);
    }

    /// <summary>
    /// Test method that allows testing how a document will be deserialized from the database.
    /// </summary>
    /// <param name="json"></param>
    /// <param name="commandOptions"></param>
    /// <returns></returns>
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
