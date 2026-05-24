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
using DataStax.AstraDB.DataApi.Core.Enumeration;
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
    private readonly Database _database;
    private readonly CommandOptions _commandOptions;

    /// <summary>
    /// Access the name of the collection
    /// </summary>
    public string CollectionName { get; }

    internal Collection(string collectionName, Database database, CommandOptions commandOptions)
    {
        Guard.NotNullOrEmpty(collectionName, nameof(collectionName));
        Guard.NotNull(database, nameof(database));
        CollectionName = collectionName;
        _database = database;
        _commandOptions = commandOptions;
    }

    /// <summary>
    /// Synchronous version of <see cref="InsertOneAsync(T, CollectionInsertOneOptions)"/>
    /// </summary>
    /// <inheritdoc cref="InsertOneAsync(T, CollectionInsertOneOptions)"/>
    public CollectionInsertOneResult<TId> InsertOne(T document, CollectionInsertOneOptions options = null)
    {
        return InsertOneAsync(document, options, runSynchronously: true).ResultSync();
    }

    /// <summary>
    /// Insert a document into the collection.
    /// </summary>
    /// <param name="document">The document to insert.</param>
    /// <param name="options">Options for the insert operation.</param>
    public Task<CollectionInsertOneResult<TId>> InsertOneAsync(T document, CollectionInsertOneOptions options = null)
    {
        return InsertOneAsync(document, options, runSynchronously: false);
    }

    private async Task<CollectionInsertOneResult<TId>> InsertOneAsync(T document, CollectionInsertOneOptions options, bool runSynchronously)
    {
        Guard.NotNull(document, nameof(document));
        
        var outputConverter = (typeof(TId) == typeof(object))
            ? new IdListConverter() 
            : null;
        
        options = options?.ShallowCopy() ?? new();
        options.SetConvertersIfNull(new DocumentConverter<T>(), outputConverter);
        
        var response = await CreateCommand("insertOne")
            .WithPayload(options.ToPayload(document))
            .AddCommandOptions(options)
            .RunAsyncReturnStatus<CollectionInsertManyResult<TId>>(runSynchronously)
            .ConfigureAwait(false);
        
        return new CollectionInsertOneResult<TId>
        {
            InsertedId = response.Result.InsertedIds[0],
        };
    }

    /// <summary>
    /// Synchronous version of <see cref="InsertManyAsync(List{T}, CollectionInsertManyOptions)"/>
    /// </summary>
    /// <inheritdoc cref="InsertManyAsync(List{T}, CollectionInsertManyOptions)"/>
    public CollectionInsertManyResult<TId> InsertMany(List<T> documents, CollectionInsertManyOptions options = null)
    {
        return InsertManyAsync(documents, options, runSynchronously: true).ResultSync();
    }

    /// <summary>
    /// Insert multiple documents into the collection.
    /// </summary>
    /// <param name="documents">The list of documents to insert.</param>
    /// <param name="options">Allows specifying the insertion chunk size, ordered/unordered mode, concurrency, as well as other generic command-execution options.</param>
    public Task<CollectionInsertManyResult<TId>> InsertManyAsync(List<T> documents, CollectionInsertManyOptions options = null)
    {
        return InsertManyAsync(documents, options, runSynchronously: false);
    }

    private async Task<CollectionInsertManyResult<TId>> InsertManyAsync(List<T> documents, CollectionInsertManyOptions options, bool runSynchronously)
    {
        Guard.NotNull(documents, nameof(documents));

        var outputConverter = typeof(TId) == typeof(object)
            ? new IdListConverter() 
            : null;
        
        options = options?.ShallowClone() ?? new();
        options.SetConvertersIfNull(new DocumentConverter<T>(), outputConverter);
        
        if (options.Concurrency > 1 && options.Ordered)
        {
            throw new ArgumentException("Cannot run ordered insert_many concurrently.");
        }
        
        var result = new CollectionInsertManyResult<TId>();
        var tasks = new List<Task>();
        var semaphore = new SemaphoreSlim(options.Concurrency);
        var (timeout, cts) = BulkOperationHelper.InitTimeout(new(), options);

        using (cts)
        {
            var bulkOperationTimeoutToken = cts.Token;
            try
            {
                var chunks = documents.CreateBatch(options.ChunkSize);

                foreach (var chunk in chunks)
                {
                    tasks.Add(Task.Run(async () =>
                    {
                        await semaphore.WaitAsync(bulkOperationTimeoutToken);
                        try
                        {
                            var runResult = await RunInsertManyAsync(chunk, options, runSynchronously).ConfigureAwait(false);
                            lock (result.InsertedIds)
                            {
                                result.InsertedIds.AddRange(runResult.InsertedIds);
                            }
                        }
                        finally
                        {
                            semaphore.Release();
                        }
                    }, bulkOperationTimeoutToken));
                }

                await Task.WhenAll(tasks).WithCancellation(bulkOperationTimeoutToken);
                return result;
            }
            catch (OperationCanceledException)
            {
                var innerException = new TimeoutException($"Bulk operation timed out after {timeout.TotalSeconds} seconds. Consider increasing the timeout using the CollectionInsertManyOptions.TimeoutOptions.BulkOperationTimeout parameter.");
                throw new BulkOperationException<CollectionInsertManyResult<TId>>(innerException, result);
            }
            catch (Exception ex)
            {
                throw new BulkOperationException<CollectionInsertManyResult<TId>>(ex, result);
            }
        }
    }

    private async Task<CollectionInsertManyResult<TId>> RunInsertManyAsync(IEnumerable<T> documents, CollectionInsertManyOptions insertOptions, bool runSynchronously)
    {
        var response = await  CreateCommand("insertMany")
            .WithPayload(insertOptions.ToPayload(documents))
            .AddCommandOptions(insertOptions)
            .RunAsyncReturnStatus<CollectionInsertManyResult<TId>>(runSynchronously)
            .ConfigureAwait(false);

        return response.Result;
    }

    /// <summary>
    /// Drops the collection from the database.
    /// </summary>
    public void Drop()
    {
        _database.DropCollection(CollectionName);
    }

    /// <summary>
    /// Asynchronously drops the collection from the database.
    /// </summary>
    public Task DropAsync()
    {
        return _database.DropCollectionAsync(CollectionName);
    }

    /// <inheritdoc cref="FindOneAsync(CollectionFindOneOptions{T})"/>
    /// Synchronous version of <see cref="FindOneAsync(CollectionFindOneOptions{T})"/>
    public T FindOne(CollectionFindOneOptions<T> options = null)
    {
        return FindOne<T>(null, options);
    }

    /// <inheritdoc cref="FindOneAsync(CollectionFilter{T}, CollectionFindOneOptions{T})"/>
    /// Synchronous version of <see cref="FindOneAsync(CollectionFilter{T}, CollectionFindOneOptions{T})"/>
    public T FindOne(CollectionFilter<T> filter, CollectionFindOneOptions<T> options = null)
    {
        return FindOne<T>(filter, options);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}(CollectionFindOneOptions{T})"/>
    /// Synchronous version of <see cref="FindOneAsync{TResult}(CollectionFindOneOptions{T})"/>
    public TResult FindOne<TResult>(CollectionFindOneOptions<T> options = null) where TResult : class
    {
        return FindOne<TResult>(null, options);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}(CollectionFilter{T}, CollectionFindOneOptions{T})"/>
    /// Synchronous version of <see cref="FindOneAsync{TResult}(CollectionFilter{T}, CollectionFindOneOptions{T})"/>
    public TResult FindOne<TResult>(CollectionFilter<T> filter, CollectionFindOneOptions<T> options = null) where TResult : class
    {
        return FindOneAsync<TResult>(filter, options, true).ResultSync();
    }

    /// <summary>
    /// Returns a single document from the collection based on the provided <see cref="CollectionFindOneOptions{T}"/>.
    /// This will return the first document found, most often used in conjunction with <see cref="BaseFindOneOptions{T, TSort}.Sort"/>.
    /// See <see cref="CollectionFindOneOptions{T}"/> for more details on sorting, projecting and the other options for finding a document.
    /// </summary>
    /// <param name="options"></param>
    /// <returns></returns>
    public Task<T> FindOneAsync(CollectionFindOneOptions<T> options = null)
    {
        return FindOneAsync<T>(null, options);
    }

    /// <summary>
    /// Returns a single document from the collection based on the provided filter
    /// </summary>
    /// <param name="filter"></param>
    /// <param name="options"></param>
    /// <returns></returns>
    /// <example>
    /// <code>
    /// var filter = Builders&lt;DifferentIdsObject&gt;.CollectionFilter.Eq(d => d.TheId, 1);
    /// var result = await collection.FindOneAsync(filter);
    /// </code>
    /// </example>
    public Task<T> FindOneAsync(CollectionFilter<T> filter, CollectionFindOneOptions<T> options = null)
    {
        return FindOneAsync<T>(filter, options);
    }

    /// <summary>
    /// Returns a single document from the collection.
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    /// <param name="options"></param>
    /// <returns></returns>
    /// <remarks>
    /// The FindOneAsync alternatives that accept a TResult type parameter allow for deserializing the document as a different type
    /// (most commonly used when using projection to return a subset of fields)
    /// </remarks>
    /// <example>
    /// <code>
    /// var exclusiveProjection = Builders&lt;FullObject&gt;.Projection
    ///     .Exclude("PropertyTwo");
    /// var findOptions = new CollectionFindOneOptions&lt;FullObject&gt;()
    /// {
    ///     Projection = exclusiveProjection
    /// };
    /// var result = await collection.FindOneAsync&lt;ObjectWithoutPropertyTwo&gt;(findOptions);
    /// </code>
    /// </example>
    public Task<TResult> FindOneAsync<TResult>(CollectionFindOneOptions<T> options = null) where TResult : class
    {
        return FindOneAsync<TResult>(null, options);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}(CollectionFindOneOptions{T})"/>
    /// <param name="filter"></param>
    /// <param name="options"></param>
    public Task<TResult> FindOneAsync<TResult>(CollectionFilter<T> filter, CollectionFindOneOptions<T> options = null) where TResult : class
    {
        return FindOneAsync<TResult>(filter, options, false);
    }

    private async Task<TResult> FindOneAsync<TResult>(CollectionFilter<T> filter, CollectionFindOneOptions<T> options, bool runSynchronously)
    {
        options ??= new();
        
        var response = await CreateCommand("findOne")
            .WithPayload(options.ToPayload(filter))
            .AddCommandOptions(options)
            .RunAsyncReturnDocumentData<DocumentResult<TResult>, TResult, FindStatusResult>(runSynchronously)
            .ConfigureAwait(false);
        
        return response.Data.Document;
    }

    /// <summary>
    /// Find documents in the collection.
    /// 
    /// The Find() methods return a <see cref="Core.Enumeration.CollectionFindCursor{T,TResult}"/> object that can be used to further structure the query
    /// by adding Sort, Projection, Skip, Limit, etc. to affect the final results.
    /// 
    /// The <see cref="Core.Enumeration.CollectionFindCursor{T,TResult}"/> object can be directly enumerated both synchronously and asynchronously.
    /// </summary>
    /// <returns></returns>
    /// <example>
    /// Synchronous Enumeration:
    /// <code>
    /// var cursor = collection.Find();
    /// foreach (var document in cursor)
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
    /// <remarks>
    /// Timeouts passed in the <see cref="CollectionFindManyOptions{T}"/> (<see cref="TimeoutOptions.ConnectionTimeout"/>
    /// and <see cref="TimeoutOptions.RequestTimeout"/>) will be used for each batched request to the API,
    /// however <c>BulkOperationCancellationToken</c> settings are ignored due to the nature of Enumeration.
    /// If you need to enforce a timeout for the entire operation, you can pass a <see cref="CancellationToken"/> to GetAsyncEnumerator.
    /// </remarks>
    public CollectionFindCursor<T> Find(CollectionFindManyOptions<T> options = null)
    {
        return Find(null, options);
    }

    /// <inheritdoc cref="Find(CollectionFindManyOptions{T})"/>
    /// <param name="filter"></param>
    /// <param name="options"></param>
    public CollectionFindCursor<T> Find(CollectionFilter<T> filter, CollectionFindManyOptions<T> options = null)
    {
        return new(filter, options, RunFindManyAsync);
    }
    
    /// <inheritdoc cref="Find(CollectionFindManyOptions{T})"/>
    /// <remarks>
    /// The Find alternatives that accept a TResult type parameter allow for deserializing the document as a different type
    /// (most commonly used when using projection to return a subset of fields)
    /// </remarks>
    public CollectionFindCursor<T, TResult> Find<TResult>(CollectionFindManyOptions<T> options = null) where TResult : class
    {
        return Find<TResult>(null, options);
    }

    /// <inheritdoc cref="Find(CollectionFilter{T}, CollectionFindManyOptions{T})"/>
    /// <remarks>
    /// The Find alternatives that accept a TResult type parameter allow for deserializing the document as a different type
    /// (most commonly used when using projection to return a subset of fields)
    /// </remarks>
    public CollectionFindCursor<T, TResult> Find<TResult>(CollectionFilter<T> filter, CollectionFindManyOptions<T> options = null) where TResult : class
    {
        return new(filter, options, RunFindManyAsync);
    }

    private async Task<FindPage<TResult>> RunFindManyAsync<TResult>(CollectionFindCursor<T, TResult> cursor, string nextPageState, bool runSynchronously) where TResult : class
    {
        var response = await CreateCommand("find")
            .WithPayload(cursor.FindOptions.ToPayload(cursor.CurrentFilter, nextPageState))
            .AddCommandOptions(cursor.FindOptions)
            .RunAsyncReturnDocumentData<APIFindResult<TResult>, TResult, FindStatusResult>(runSynchronously)
            .ConfigureAwait(false);
        
        return new FindPage<TResult>(
            response.Data.NextPageState,
            response.Data.Items,
            response.Status?.SortVector
        );
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndUpdateAsync(CollectionFilter{T}, UpdateBuilder{T}, CollectionFindOneAndUpdateOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndUpdateAsync(CollectionFilter{T}, UpdateBuilder{T}, CollectionFindOneAndUpdateOptions{T})"/>
    public T FindOneAndUpdate(CollectionFilter<T> filter, UpdateBuilder<T> update, CollectionFindOneAndUpdateOptions<T> options = null)
    {
        return FindOneAndUpdate<T>(filter, update, options);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndUpdateAsync{TResult}(CollectionFilter{T}, UpdateBuilder{T}, CollectionFindOneAndUpdateOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndUpdateAsync{TResult}(CollectionFilter{T}, UpdateBuilder{T}, CollectionFindOneAndUpdateOptions{T})"/>
    public TResult FindOneAndUpdate<TResult>(CollectionFilter<T> filter, UpdateBuilder<T> update, CollectionFindOneAndUpdateOptions<T> options = null) where TResult : class
    {
        return FindOneAndUpdateAsync<TResult>(filter, update, options, runSynchronously: true).ResultSync();
    }

    /// <summary>
    /// Find a document and update it using the provided updates.
    /// This is similar to <see cref="UpdateOneAsync(CollectionFilter{T}, UpdateBuilder{T}, CollectionUpdateOneOptions{T})"/> but returns the updated document.
    /// </summary>
    /// <param name="filter">The filter to match documents.</param>
    /// <param name="update">The update operations to apply.</param>
    /// <param name="options">Options for the find and update operation.</param>
    /// <returns>The updated document, or null if not found</returns>
    /// <example>
    /// <code>
    /// var updater = Builders&lt;SimpleObject&gt;.CollectionUpdate;
    /// var combinedUpdate = updater.Combine(
    ///     updater.Set(so => so.Properties.PropertyOne, "Updated"),
    ///     updater.Unset(so => so.Properties.PropertyTwo)
    /// );
    /// var result = await collection.FindOneAndUpdateAsync(filter, combinedUpdate);
    /// </code>
    /// </example>
    public Task<T> FindOneAndUpdateAsync(CollectionFilter<T> filter, UpdateBuilder<T> update, CollectionFindOneAndUpdateOptions<T> options = null)
    {
        return FindOneAndUpdateAsync<T>(filter, update, options);
    }

    /// <inheritdoc cref="FindOneAndUpdateAsync(CollectionFilter{T}, UpdateBuilder{T}, CollectionFindOneAndUpdateOptions{T})"/>
    /// <remarks>
    /// The FindOneAndUpdateAsync alternatives that accept a TResult type parameter allow for deserializing the resulting document
    /// as a different type (most commonly used when using projection to return a subset of fields)
    /// </remarks>
    public Task<TResult> FindOneAndUpdateAsync<TResult>(CollectionFilter<T> filter, UpdateBuilder<T> update, CollectionFindOneAndUpdateOptions<T> options = null) where TResult : class
    {
        return FindOneAndUpdateAsync<TResult>(filter, update, options, runSynchronously: false);
    }

    private async Task<TResult> FindOneAndUpdateAsync<TResult>(CollectionFilter<T> filter, UpdateBuilder<T> update, CollectionFindOneAndUpdateOptions<T> options, bool runSynchronously) where TResult : class
    {
        Guard.NotNull(update, nameof(update));
        
        options ??= new();
        
        var response = await CreateCommand("findOneAndUpdate")
            .WithPayload(options.ToPayload(filter, update))
            .AddCommandOptions(options)
            .RunAsyncReturnDocumentData<DocumentResult<TResult>, TResult, UpdateResult>(runSynchronously)
            .ConfigureAwait(false);
        
        return response.Data.Document;
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndReplaceAsync(CollectionFilter{T}, T, CollectionFindOneAndReplaceOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndReplaceAsync(CollectionFilter{T}, T, CollectionFindOneAndReplaceOptions{T})"/>
    public T FindOneAndReplace(CollectionFilter<T> filter, T replacement, CollectionFindOneAndReplaceOptions<T> options = null)
    {
        return FindOneAndReplace<T>(filter, replacement, options);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndReplaceAsync{TResult}(CollectionFilter{T}, T, CollectionFindOneAndReplaceOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndReplaceAsync{TResult}(CollectionFilter{T}, T, CollectionFindOneAndReplaceOptions{T})"/>
    public TResult FindOneAndReplace<TResult>(CollectionFilter<T> filter, T replacement, CollectionFindOneAndReplaceOptions<T> options = null) where TResult : class
    {
        return FindOneAndReplaceAsync<TResult>(filter, replacement, options, runSynchronously: true).ResultSync();
    }

    /// <summary>
    /// Find a document and replace it with the provided replacement.
    /// This is similar to <see cref="ReplaceOneAsync(CollectionFilter{T}, T, CollectionReplaceOneOptions{T})"/> but returns the replaced document.
    /// </summary>
    /// <param name="filter">The filter to match documents.</param>
    /// <param name="replacement">The replacement document.</param>
    /// <param name="options">Options for the find and replace operation.</param>
    /// <returns>The replaced document, or null if not found</returns>
    public Task<T> FindOneAndReplaceAsync(CollectionFilter<T> filter, T replacement, CollectionFindOneAndReplaceOptions<T> options = null)
    {
        return FindOneAndReplaceAsync<T>(filter, replacement, options);
    }

    /// <inheritdoc cref="FindOneAndReplaceAsync(CollectionFilter{T}, T, CollectionFindOneAndReplaceOptions{T})"/>
    /// <remarks>
    /// The FindOneAndReplaceAsync alternatives that accept a TResult type parameter allow for deserializing the resulting document
    /// as a different type (most commonly used when using projection to return a subset of fields)
    /// </remarks>
    public Task<TResult> FindOneAndReplaceAsync<TResult>(CollectionFilter<T> filter, T replacement, CollectionFindOneAndReplaceOptions<T> options = null) where TResult : class
    {
        return FindOneAndReplaceAsync<TResult>(filter, replacement, options, runSynchronously: false);
    }

    private async Task<TResult> FindOneAndReplaceAsync<TResult>(CollectionFilter<T> filter, T replacement, CollectionFindOneAndReplaceOptions<T> options, bool runSynchronously) where TResult : class
    {
        options ??= new();
        
        var response = await CreateCommand("findOneAndReplace")
            .WithPayload(options.ToPayload(filter, replacement))
            .AddCommandOptions(options)
            .RunAsyncReturnDocumentData<DocumentResult<TResult>, TResult, UpdateResult>(runSynchronously)
            .ConfigureAwait(false);
        
        return response.Data.Document;
    }

    // /// <summary>
    // /// Finds documents in a collection through a retrieval process that uses a reranker model to combine results from a vector search and a lexical search (hybrid search)
    // /// </summary>
    // /// <returns></returns>
    // public RerankSorter<T, T> FindAndRerank()
    // {
    //     return FindAndRerank<T>(null);
    // }
    //
    // /// <inheritdoc cref="FindAndRerank()"/>
    // /// <param name="filter"></param>
    // public RerankSorter<T, T> FindAndRerank(CollectionFilter<T> filter)
    // {
    //     return FindAndRerank<T>(filter);
    // }
    //
    // /// <inheritdoc cref="FindAndRerank()"/>
    // /// <param name="filter"></param>
    // /// <param name="commandOptions"></param>
    // public RerankSorter<T, T> FindAndRerank(CollectionFilter<T> filter, CommandOptions commandOptions)
    // {
    //     return FindAndRerank<T>(filter, commandOptions);
    // }
    //
    // /// <summary>
    // /// Finds documents in a collection through a retrieval process that uses a reranker model to combine results from a vector search and a lexical search (hybrid search)
    // /// </summary>
    // /// <typeparam name="TResult">If you are using projection to return a subset of fields, TResult can be used to receive the projected document.</typeparam>
    // /// <returns></returns>
    // public RerankSorter<T, TResult> FindAndRerank<TResult>() where TResult : class
    // {
    //     return FindAndRerank<TResult>(null, null);
    // }
    //
    // /// <inheritdoc cref="FindAndRerank{TResult}()"/>
    // /// <param name="filter"></param>
    // public RerankSorter<T, TResult> FindAndRerank<TResult>(CollectionFilter<T> filter) where TResult : class
    // {
    //     return FindAndRerank<TResult>(filter, null);
    // }
    //
    // /// <inheritdoc cref="FindAndRerank{TResult}()"/>
    // /// <param name="filter"></param>
    // /// <param name="commandOptions"></param>
    // public RerankSorter<T, TResult> FindAndRerank<TResult>(CollectionFilter<T> filter, CommandOptions commandOptions) where TResult : class
    // {
    //     return new RerankSorter<T, TResult>(() => CreateCommand("findAndRerank"), filter, commandOptions);
    // }

    /// <summary>
    /// Synchronous version of <see cref="ReplaceOneAsync(CollectionFilter{T}, T, CollectionReplaceOneOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="ReplaceOneAsync(CollectionFilter{T}, T, CollectionReplaceOneOptions{T})"/>
    public UpdateResult ReplaceOne(CollectionFilter<T> filter, T replacement, CollectionReplaceOneOptions<T> options = null)
    {
        return ReplaceOneAsync(filter, replacement, options, runSynchronously: true).ResultSync();
    }

    /// <summary>
    /// Replace a document in the collection that matches the provided filter with the provided replacement.
    /// This is similar to <see cref="FindOneAndReplaceAsync(CollectionFilter{T}, T, CollectionFindOneAndReplaceOptions{T})"/> but does not return the replaced document.
    /// </summary>
    /// <param name="filter">The filter to match documents.</param>
    /// <param name="replacement">The replacement document.</param>
    /// <param name="options">Options for the replace operation.</param>
    /// <returns>The result of the replace operation.</returns>
    public Task<UpdateResult> ReplaceOneAsync(CollectionFilter<T> filter, T replacement, CollectionReplaceOneOptions<T> options = null)
    {
        return ReplaceOneAsync(filter, replacement, options, runSynchronously: false);
    }

    private async Task<UpdateResult> ReplaceOneAsync(CollectionFilter<T> filter, T replacement, CollectionReplaceOneOptions<T> options, bool runSynchronously)
    {
        options ??= new();
        
        var replaceOptions = new CollectionFindOneAndReplaceOptions<T>
        {
            Sort = options.Sort,
            Upsert = options.Upsert,
            Projection = new ExclusiveProjectionBuilder<T>().Exclude("*")
        };
        
        var response = await CreateCommand("findOneAndReplace")
            .WithPayload(replaceOptions.ToPayload(filter, replacement))
            .AddCommandOptions(options)
            .RunAsyncReturnStatus<UpdateResult>(runSynchronously)
            .ConfigureAwait(false);
        
        return response.Result;
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndDeleteAsync(CollectionFilter{T}, CollectionFindOneAndDeleteOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndDeleteAsync(CollectionFilter{T}, CollectionFindOneAndDeleteOptions{T})"/>
    public T FindOneAndDelete(CollectionFilter<T> filter, CollectionFindOneAndDeleteOptions<T> options = null)
    {
        return FindOneAndDelete<T>(filter, options);
    }

    /// <summary>
    /// Synchronous version of <see cref="FindOneAndDeleteAsync{TResult}(CollectionFilter{T}, CollectionFindOneAndDeleteOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="FindOneAndDeleteAsync{TResult}(CollectionFilter{T}, CollectionFindOneAndDeleteOptions{T})"/>
    public TResult FindOneAndDelete<TResult>(CollectionFilter<T> filter, CollectionFindOneAndDeleteOptions<T> options = null)
    {
        return FindOneAndDeleteAsync<TResult>(filter, options, runSynchronously: true).ResultSync();
    }

    /// <summary>
    /// Find a document and delete it from the collection.
    /// This is similar to <see cref="DeleteOneAsync(CollectionFilter{T}, CollectionDeleteOneOptions{T})"/> but returns the deleted document.
    /// </summary>
    /// <param name="filter">The filter to match documents.</param>
    /// <param name="options">Options for the find and delete operation.</param>
    /// <returns>The deleted document, or null if not found</returns>
    public Task<T> FindOneAndDeleteAsync(CollectionFilter<T> filter, CollectionFindOneAndDeleteOptions<T> options = null)
    {
        return FindOneAndDeleteAsync<T>(filter, options);
    }

    /// <inheritdoc cref="FindOneAndDeleteAsync(CollectionFilter{T}, CollectionFindOneAndDeleteOptions{T})"/>
    /// <remarks>
    /// The FindOneAndDeleteAsync alternatives that accept a TResult type parameter allow for deserializing the resulting document
    /// as a different type (most commonly used when using projection to return a subset of fields)
    /// </remarks>
    public Task<TResult> FindOneAndDeleteAsync<TResult>(CollectionFilter<T> filter, CollectionFindOneAndDeleteOptions<T> options = null)
    {
        return FindOneAndDeleteAsync<TResult>(filter, options, runSynchronously: false);
    }

    private async Task<TResult> FindOneAndDeleteAsync<TResult>(CollectionFilter<T> filter, CollectionFindOneAndDeleteOptions<T> options, bool runSynchronously)
    {
        options ??= new();
        
        var response = await CreateCommand("findOneAndDelete")
            .WithPayload(options.ToPayload(filter))
            .AddCommandOptions(options)
            .RunAsyncReturnDocumentData<DocumentResult<TResult>, TResult, FindStatusResult>(runSynchronously)
            .ConfigureAwait(false);
        
        return response.Data.Document;
    }

    /// <summary>
    /// Synchronous version of <see cref="DeleteOneAsync(CollectionFilter{T}, CollectionDeleteOneOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="DeleteOneAsync(CollectionFilter{T}, CollectionDeleteOneOptions{T})"/>
    public DeleteResult DeleteOne(CollectionFilter<T> filter, CollectionDeleteOneOptions<T> options = null)
    {
        return DeleteOneAsync(filter, options, runSynchronously: true).ResultSync();
    }

    /// <summary>
    /// Delete a document from the collection.
    /// This is similar to <see cref="FindOneAndDeleteAsync(CollectionFilter{T}, CollectionFindOneAndDeleteOptions{T})"/> but does not return the deleted document.
    /// </summary>
    /// <param name="filter">The filter to match documents.</param>
    /// <param name="options">Options for the delete operation.</param>
    /// <returns>The result of the delete operation.</returns>
    public Task<DeleteResult> DeleteOneAsync(CollectionFilter<T> filter, CollectionDeleteOneOptions<T> options = null)
    {
        return DeleteOneAsync(filter, options, runSynchronously: false);
    }

    private async Task<DeleteResult> DeleteOneAsync(CollectionFilter<T> filter, CollectionDeleteOneOptions<T> options, bool runSynchronously)
    {
        options ??= new();
        
        var response = await CreateCommand("deleteOne")
            .WithPayload(options.ToPayload(filter))
            .AddCommandOptions(options)
            .RunAsyncReturnStatus<DeleteResult>(runSynchronously)
            .ConfigureAwait(false);
        
        return response.Result;
    }

    /// <summary>
    /// Synchronous version of <see cref="DeleteManyAsync(CollectionFilter{T}, CollectionDeleteManyOptions)"/>
    /// </summary>
    /// <inheritdoc cref="DeleteManyAsync(CollectionFilter{T}, CollectionDeleteManyOptions)"/>
    public DeleteResult DeleteMany(CollectionFilter<T> filter, CollectionDeleteManyOptions options = null)
    {
        return DeleteManyAsync(filter, options, runSynchronously: true).ResultSync();
    }

    /// <summary>
    /// Delete all documents matching the filter from the collection.
    /// </summary>
    /// <param name="filter">The filter to match documents.</param>
    /// <param name="options">Options for the delete operation.</param>
    /// <returns>The result of the delete operation.</returns>
    /// <remarks>
    /// Deleting all documents in a collection is not recommended for large collections. However, if needed
    /// you can pass null as the filter to delete all documents in the collection.
    /// <example>
    /// <code>
    /// var deleteResult = await collection.DeleteManyAsync(null);
    /// </code>
    /// </example>
    /// </remarks>
    public Task<DeleteResult> DeleteManyAsync(CollectionFilter<T> filter, CollectionDeleteManyOptions options = null)
    {
        return DeleteManyAsync(filter, options, runSynchronously: false);
    }

    private async Task<DeleteResult> DeleteManyAsync(CollectionFilter<T> filter, CollectionDeleteManyOptions options, bool runSynchronously)
    {
        options ??= new();
        
        var deleteResult = new DeleteResult();
        var keepProcessing = true;
        
        var (timeout, cts) = BulkOperationHelper.InitTimeout(GetOptionsTree(), options);

        using (cts)
        {
            try
            {
                while (keepProcessing)
                {
                    var response = await CreateCommand("deleteMany")
                        .WithPayload(options.ToPayload(filter))
                        .AddCommandOptions(options)
                        .RunAsyncReturnStatus<DeleteResult>(runSynchronously)
                        .ConfigureAwait(false);
                        
                    deleteResult.DeletedCount += response.Result.DeletedCount;
                    keepProcessing = response.Result.MoreData;
                }
            }
            catch (OperationCanceledException)
            {
                var innerException = new TimeoutException($"Bulk operation timed out after {timeout.TotalSeconds} seconds. Consider increasing the timeout using the CollectionDeleteManyOptions.TimeoutOptions.BulkOperationTimeout parameter.");
                throw new BulkOperationException<DeleteResult>(innerException, deleteResult);
            }
            catch (Exception ex)
            {
                throw new BulkOperationException<DeleteResult>(ex, deleteResult);
            }
        }

        return deleteResult;
    }

    /// <summary>
    /// Synchronous version of <see cref="UpdateOneAsync(CollectionFilter{T}, UpdateBuilder{T}, CollectionUpdateOneOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="UpdateOneAsync(CollectionFilter{T}, UpdateBuilder{T}, CollectionUpdateOneOptions{T})"/>
    public UpdateResult UpdateOne(CollectionFilter<T> filter, UpdateBuilder<T> update, CollectionUpdateOneOptions<T> options = null)
    {
        return UpdateOneAsync(filter, update, options, runSynchronously: true).ResultSync();
    }

    /// <summary>
    /// Update a single document in the collection using the provided filter and update builder.
    /// 
    /// This is similar to <see cref="FindOneAndUpdateAsync(CollectionFilter{T}, UpdateBuilder{T}, CollectionFindOneAndUpdateOptions{T})"/> but does not return the updated document.
    /// </summary>
    /// <param name="filter">The filter to match documents.</param>
    /// <param name="update">The update operations to apply.</param>
    /// <param name="options">Options for the update operation.</param>
    /// <returns>The result of the update operation.</returns>
    public Task<UpdateResult> UpdateOneAsync(CollectionFilter<T> filter, UpdateBuilder<T> update, CollectionUpdateOneOptions<T> options = null)
    {
        return UpdateOneAsync(filter, update, options, runSynchronously: false);
    }

    private async Task<UpdateResult> UpdateOneAsync(CollectionFilter<T> filter, UpdateBuilder<T> update, CollectionUpdateOneOptions<T> options, bool runSynchronously)
    {
        Guard.NotNull(update, nameof(update));
        
        options ??= new();
        
        var response = await CreateCommand("updateOne")
            .WithPayload(options.ToPayload(filter, update))
            .AddCommandOptions(options)
            .RunAsyncReturnStatus<UpdateResult>(runSynchronously)
            .ConfigureAwait(false);
        
        return response.Result;
    }

    /// <summary>
    /// Synchronous version of <see cref="UpdateManyAsync(CollectionFilter{T}, UpdateBuilder{T}, CollectionUpdateManyOptions)"/>
    /// </summary>
    /// <inheritdoc cref="UpdateManyAsync(CollectionFilter{T}, UpdateBuilder{T}, CollectionUpdateManyOptions)"/>
    public UpdateResult UpdateMany(CollectionFilter<T> filter, UpdateBuilder<T> update, CollectionUpdateManyOptions options = null)
    {
        return UpdateManyAsync(filter, update, options, runSynchronously: true).ResultSync();
    }

    /// <summary>
    /// Update all documents matching the filter by applying the provided updates.
    /// </summary>
    /// <param name="filter">The filter to match documents.</param>
    /// <param name="update">The update operations to apply.</param>
    /// <param name="options">Options for the update operation.</param>
    /// <returns>The result of the update operation.</returns>
    public Task<UpdateResult> UpdateManyAsync(CollectionFilter<T> filter, UpdateBuilder<T> update, CollectionUpdateManyOptions options = null)
    {
        return UpdateManyAsync(filter, update, options, runSynchronously: false);
    }

    private async Task<UpdateResult> UpdateManyAsync(CollectionFilter<T> filter, UpdateBuilder<T> update, CollectionUpdateManyOptions options, bool runSynchronously)
    {
        Guard.NotNull(update, nameof(update));
        
        options ??= new();
        
        var keepProcessing = true;
        var updateResult = new UpdateResult();
        string nextPageState = null;
        
        var (timeout, cts) = BulkOperationHelper.InitTimeout(GetOptionsTree(), options);

        using (cts)
        {
            try
            {
                while (keepProcessing)
                {
                    var response = await CreateCommand("updateMany")
                        .WithPayload(options.ToPayload(filter, update, nextPageState))
                        .AddCommandOptions(options)
                        .RunAsyncReturnStatus<PagedUpdateResult>(runSynchronously)
                        .ConfigureAwait(false);
                        
                    updateResult.MatchedCount += response.Result.MatchedCount;
                    updateResult.ModifiedCount += response.Result.ModifiedCount;
                    updateResult.UpsertedId = response.Result.UpsertedId;
                    nextPageState = response.Result.NextPageState;
                    
                    if (string.IsNullOrEmpty(nextPageState))
                    {
                        keepProcessing = false;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                var innerException = new TimeoutException($"Bulk operation timed out after {timeout.TotalSeconds} seconds. Consider increasing the timeout using the CollectionUpdateManyOptions.TimeoutOptions.BulkOperationTimeout parameter.");
                throw new BulkOperationException<UpdateResult>(innerException, updateResult);
            }
            catch (Exception ex)
            {
                throw new BulkOperationException<UpdateResult>(ex, updateResult);
            }
        }
        
        return updateResult;
    }

    /// <summary>
    /// Synchronous version of <see cref="CountDocumentsAsync(CollectionFilter{T}, int, CollectionCountDocumentsOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CountDocumentsAsync(CollectionFilter{T}, int, CollectionCountDocumentsOptions)"/>
    public int CountDocuments(CollectionFilter<T> filter, int maxDocumentsToCount, CollectionCountDocumentsOptions options = null)
    {
        return CountDocumentsAsync(filter, maxDocumentsToCount, options, true).ResultSync();
    }

    /// <summary>
    /// Count the documents matching a specified filter, up to a maximum count.
    /// </summary>
    /// <param name="filter"></param>
    /// <param name="maxDocumentsToCount"></param>
    /// <param name="options"></param>
    public Task<int> CountDocumentsAsync(CollectionFilter<T> filter, int maxDocumentsToCount, CollectionCountDocumentsOptions options = null)
    {
        return CountDocumentsAsync(filter, maxDocumentsToCount, options, false);
    }

    private async Task<int> CountDocumentsAsync(CollectionFilter<T> filter, int maxDocumentsToCount, CollectionCountDocumentsOptions options, bool runSynchronously)
    {
        if (maxDocumentsToCount < 1)
        {
            throw new ArgumentException($"maxDocumentsToCount must be >= 1 (got {maxDocumentsToCount})", nameof(maxDocumentsToCount));
        }
        
        options ??= new();
        
        var response = await CreateCommand("countDocuments")
            .WithPayload(options.ToPayload(filter))
            .AddCommandOptions(options)
            .RunAsyncReturnStatus<DocumentsCountResult>(runSynchronously)
            .ConfigureAwait(false);
        
        if (response.Result.Count >= maxDocumentsToCount || response.Result.MoreData)
        {
            throw new DocumentCountExceedsMaxException();
        }

        return response.Result.Count;
    }
    /// <summary>
    /// Synchronous version of <see cref="EstimateDocumentCountAsync(CollectionEstimateDocumentCountOptions)"/>
    /// </summary>
    /// <inheritdoc cref="EstimateDocumentCountAsync(CollectionEstimateDocumentCountOptions)"/>
    public int EstimateDocumentCount(CollectionEstimateDocumentCountOptions options = null)
    {
        return EstimateDocumentCountAsync(options, true).ResultSync();
    }

    /// <summary>
    /// Estimate the number of documents in the collection.
    /// </summary>
    /// <param name="options">Options for the estimate operation</param>
    /// <returns></returns>
    public Task<int> EstimateDocumentCountAsync(CollectionEstimateDocumentCountOptions options = null)
    {
        return EstimateDocumentCountAsync(options, false);
    }

    private async Task<int> EstimateDocumentCountAsync(CollectionEstimateDocumentCountOptions options, bool runSynchronously)
    {
        var response = await CreateCommand("estimatedDocumentCount")
            .WithPayload(new {})
            .AddCommandOptions(options)
            .RunAsyncReturnStatus<EstimatedDocumentsCountResult>(runSynchronously)
            .ConfigureAwait(false);

        return response.Result.Count;
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

    private List<CommandOptions> GetOptionsTree()
    {
        var optionsTree = _commandOptions == null
            ? _database.OptionsTree
            : _database.OptionsTree.Concat(new[] { _commandOptions });

        if (typeof(T) == typeof(Document))
        {
            optionsTree = optionsTree.Concat(new[]
            {
                new CommandOptions { SerializeIEEE754SpecialValues = false }
            });
        }

        return optionsTree.ToList();
    }

    internal Command CreateCommand(string name)
    {
        var optionsTree = GetOptionsTree().ToArray();
        return new Command(name, _database.Client, optionsTree, new DatabaseCommandUrlBuilder(_database, CollectionName));
    }
}
