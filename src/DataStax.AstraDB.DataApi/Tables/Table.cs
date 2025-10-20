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

namespace DataStax.AstraDB.DataApi.Tables;


/// <summary>
/// This is the main entry point for interacting with a table in the Astra DB Data API.
/// </summary>
/// <typeparam name="T">The type to use for rows in the table (when not specified, defaults to <see cref="Row"/> </typeparam>
public class Table<T> : IQueryRunner<T, SortBuilder<T>> where T : class
{
    private readonly string _tableName;
    private readonly Database _database;
    private readonly CommandOptions _commandOptions;

    internal Table(string tableName, Database database, CommandOptions commandOptions)
    {
        _tableName = tableName;
        _database = database;
        _commandOptions = commandOptions;
    }

    /// <summary>
    /// Synchronous version of <see cref="ListIndexMetadataAsync()"/>
    /// </summary>
    /// <returns></returns>
    public ListTableIndexMetadataResult ListIndexMetadata()
    {
        return ListIndexMetadata(null);
    }

    /// <summary>
    /// Synchronous version of <see cref="ListIndexMetadataAsync(CommandOptions)"/>
    /// </summary>
    /// <param name="commandOptions"></param>
    /// <returns></returns>
    public ListTableIndexMetadataResult ListIndexMetadata(CommandOptions commandOptions)
    {
        return ListIndexMetadataAsync(commandOptions, true).ResultSync();
    }

    /// <summary>
    /// Get a list of indexes for the table.
    /// </summary>
    /// <returns></returns>
    public Task<ListTableIndexMetadataResult> ListIndexMetadataAsync()
    {
        return ListIndexMetadataAsync(null);
    }

    /// <summary>
    /// Get a list of indexes for the table.
    /// </summary>
    /// <param name="commandOptions"></param>
    /// <returns></returns>
    public Task<ListTableIndexMetadataResult> ListIndexMetadataAsync(CommandOptions commandOptions)
    {
        return ListIndexMetadataAsync(commandOptions, false);
    }

    private async Task<ListTableIndexMetadataResult> ListIndexMetadataAsync(CommandOptions commandOptions, bool runSynchronously)
    {
        var payload = new
        {
            options = new
            {
                explain = true,
            }
        };
        var command = CreateCommand("listIndexes").WithPayload(payload).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnStatus<ListTableIndexMetadataResult>(runSynchronously).ConfigureAwait(false);
        return response.Result;
    }

    /// <summary>
    /// Synchronous version of <see cref="ListIndexNamesAsync()"/>
    /// </summary>
    /// <inheritdoc cref="ListIndexNamesAsync()"/>
    public List<string> ListIndexNames()
    {
        return ListIndexNames(null);
    }

    /// <summary>
    /// Synchronous version of <see cref="ListIndexNamesAsync(CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="ListIndexNamesAsync(CommandOptions)"/>
    public List<string> ListIndexNames(CommandOptions commandOptions)
    {
        return ListIndexNamesAsync(commandOptions, true).ResultSync();
    }

    /// <summary>
    /// Get a list of index names for the table.
    /// </summary>
    /// <returns></returns>
    public Task<List<string>> ListIndexNamesAsync()
    {
        return ListIndexNamesAsync(null);
    }

    /// <summary>
    /// Get a list of index names for the table.
    /// </summary>
    /// <param name="commandOptions"></param>
    /// <returns></returns>
    public Task<List<string>> ListIndexNamesAsync(CommandOptions commandOptions)
    {
        return ListIndexNamesAsync(commandOptions, false);
    }

    private async Task<List<string>> ListIndexNamesAsync(CommandOptions commandOptions, bool runSynchronously)
    {
        var payload = new
        {
            options = new
            {
                explain = false,
            }
        };
        var command = CreateCommand("listIndexes").WithPayload(payload).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnStatus<ListTableIndexNamesResult>(runSynchronously).ConfigureAwait(false);
        return response.Result.IndexNames;
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateIndexAsync(TableIndex)"/>
    /// </summary>
    /// <inheritdoc cref="CreateIndexAsync(TableIndex)"/>
    public void CreateIndex(TableIndex index)
    {
        CreateIndex(index, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateIndexAsync(TableIndex, CreateIndexCommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateIndexAsync(TableIndex, CreateIndexCommandOptions)"/>
    public void CreateIndex(TableIndex index, CreateIndexCommandOptions commandOptions)
    {
        CreateIndexAsync(index, commandOptions, true).ResultSync();
    }

    /// <summary>
    /// Creates an index on the table.
    /// </summary>
    /// <param name="index">The index specifications</param>
    public Task CreateIndexAsync(TableIndex index)
    {
        return CreateIndexAsync(index, null, false);
    }

    /// <inheritdoc cref="CreateIndexAsync(TableIndex)"/>
    /// <param name="commandOptions"></param>
    public Task CreateIndexAsync(TableIndex index, CreateIndexCommandOptions commandOptions)
    {
        return CreateIndexAsync(index, commandOptions, false);
    }

    private async Task CreateIndexAsync(TableIndex index, CreateIndexCommandOptions commandOptions, bool runSynchronously)
    {
        var indexResponse = await ListIndexMetadataAsync(commandOptions, runSynchronously);
        var exists = indexResponse?.Indexes?.Any(i => i.Name == index.IndexName) == true;

        if (exists)
        {
            if (commandOptions != null && commandOptions.SkipIfExists)
            {
                return;
            }
            throw new InvalidOperationException($"Index '{index.IndexName}' already exists on table '{this._tableName}'.");
        }

        var command = CreateCommand("createIndex").WithPayload(index).AddCommandOptions(commandOptions);
        await command.RunAsyncReturnStatus<Dictionary<string, int>>(runSynchronously).ConfigureAwait(false);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateVectorIndexAsync(TableVectorIndex)"/>
    /// </summary>
    /// <inheritdoc cref="CreateVectorIndexAsync(TableVectorIndex)"/>
    public void CreateVectorIndex(TableVectorIndex index)
    {
        CreateVectorIndex(index, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateVectorIndexAsync(TableVectorIndex, CreateIndexCommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateVectorIndexAsync(TableVectorIndex, CreateIndexCommandOptions)"/>
    public void CreateVectorIndex(TableVectorIndex index, CreateIndexCommandOptions commandOptions)
    {
        CreateVectorIndexAsync(index, commandOptions, true).ResultSync();
    }

    /// <summary>
    /// Creates a vector index on the table.
    /// </summary>
    /// <param name="index"></param>
    public Task CreateVectorIndexAsync(TableVectorIndex index)
    {
        return CreateVectorIndexAsync(index, null, false);
    }

    /// <inheritdoc cref="CreateVectorIndexAsync(TableVectorIndex)"/>
    /// <param name="commandOptions"></param>
    public Task CreateVectorIndexAsync(TableVectorIndex index, CreateIndexCommandOptions commandOptions)
    {
        return CreateVectorIndexAsync(index, commandOptions, false);
    }

    private async Task CreateVectorIndexAsync(TableVectorIndex index, CreateIndexCommandOptions commandOptions, bool runSynchronously)
    {
        var command = CreateCommand("createVectorIndex").WithPayload(index).AddCommandOptions(commandOptions);
        await command.RunAsyncReturnStatus<Dictionary<string, int>>(runSynchronously).ConfigureAwait(false);
    }

    /// <summary>
    /// This is a synchronous version of <see cref="InsertManyAsync(IEnumerable{T}, InsertManyOptions, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="InsertManyAsync(IEnumerable{T}, InsertManyOptions, CommandOptions)"/>
    public TableInsertManyResult InsertMany(IEnumerable<T> rows)
    {
        return InsertMany(rows, null as CommandOptions);
    }

    /// <summary>
    /// Synchronous version of <see cref="InsertManyAsync(IEnumerable{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="InsertManyAsync(IEnumerable{T}, CommandOptions)"/>
    public TableInsertManyResult InsertMany(IEnumerable<T> rows, CommandOptions commandOptions)
    {
        return InsertManyAsync(rows, null, commandOptions, true).ResultSync();
    }

    /// <summary>
    /// Synchronous version of <see cref="InsertManyAsync(IEnumerable{T}, InsertManyOptions)"/>
    /// </summary>
    /// <inheritdoc cref="InsertManyAsync(IEnumerable{T}, InsertManyOptions)"/>
    public TableInsertManyResult InsertMany(IEnumerable<T> rows, InsertManyOptions insertOptions)
    {
        return InsertManyAsync(rows, insertOptions, null, true).ResultSync();
    }

    /// <summary>
    /// Insert multiple rows into the table.
    /// </summary>
    /// <param name="rows"></param>
    /// <returns></returns>
    /// <remarks>
    /// If you need to control concurrency, chunk size, or whether the insert is ordered or not, use the <see cref="InsertManyAsync(IEnumerable{T}, InsertManyOptions)"/> overload.
    /// To additionally control timesouts, use the <see cref="InsertManyAsync(IEnumerable{T}, InsertManyOptions, CommandOptions)"/> overload.
    /// </remarks>
    /// <throws cref="ArgumentException">Thrown if the rows collection is null or empty.</throws>
    /// <throws cref="BulkOperationException{TableInsertManyResult}">Thrown if an error occurs during the bulk operation, with partial results returned in the <see cref="BulkOperationException{TableInsertManyResult}.PartialResult"/> property.</throws>
    public Task<TableInsertManyResult> InsertManyAsync(IEnumerable<T> rows)
    {
        return InsertManyAsync(rows, null, null, false);
    }

    /// <inheritdoc cref="InsertManyAsync(IEnumerable{T})"/>
    /// <param name="commandOptions"></param>
    public Task<TableInsertManyResult> InsertManyAsync(IEnumerable<T> rows, CommandOptions commandOptions)
    {
        return InsertManyAsync(rows, null, commandOptions, false);
    }

    /// <inheritdoc cref="InsertManyAsync(IEnumerable{T})"/>
    /// <param name="insertOptions"></param>
    public Task<TableInsertManyResult> InsertManyAsync(IEnumerable<T> rows, InsertManyOptions insertOptions)
    {
        return InsertManyAsync(rows, insertOptions, null, false);
    }

    /// <inheritdoc cref="InsertManyAsync(IEnumerable{T}, CommandOptions)"/>
    /// <param name="insertOptions"></param>
    public Task<TableInsertManyResult> InsertManyAsync(IEnumerable<T> rows, InsertManyOptions insertOptions, CommandOptions commandOptions)
    {
        return InsertManyAsync(rows, insertOptions, commandOptions, false);
    }

    private async Task<TableInsertManyResult> InsertManyAsync(IEnumerable<T> rows, InsertManyOptions insertOptions, CommandOptions commandOptions, bool runSynchronously)
    {
        Guard.NotNullOrEmpty(rows, nameof(rows));

        if (insertOptions == null) insertOptions = new InsertManyOptions();
        if (insertOptions.Concurrency > 1 && insertOptions.InsertInOrder)
        {
            throw new ArgumentException("Cannot run ordered insert_many concurrently.");
        }

        var result = new TableInsertManyResult();
        var tasks = new List<Task>();
        var semaphore = new SemaphoreSlim(insertOptions.Concurrency);
        var (timeout, cts) = BulkOperationHelper.InitTimeout(GetOptionsTree(), ref commandOptions);

        using (cts)
        {
            var bulkOperationTimeoutToken = cts.Token;
            try
            {
                var chunks = rows.CreateBatch(insertOptions.ChunkSize);

                foreach (var chunk in chunks)
                {
                    tasks.Add(Task.Run(async () =>
                    {
                        await semaphore.WaitAsync(bulkOperationTimeoutToken);
                        try
                        {
                            var runResult = await RunInsertManyAsync(chunk, insertOptions.InsertInOrder, insertOptions.ReturnDocumentResponses, commandOptions, runSynchronously).ConfigureAwait(false);
                            lock (result.InsertedIds)
                            {
                                result.PrimaryKeys = runResult.PrimaryKeys;
                                result.InsertedIds.AddRange(runResult.InsertedIds);
                                result.DocumentResponses.AddRange(runResult.DocumentResponses);
                            }
                        }
                        finally
                        {
                            semaphore.Release();
                        }
                    }, bulkOperationTimeoutToken));
                }

                await Task.WhenAll(tasks).WithCancellation(bulkOperationTimeoutToken);
            }
            catch (OperationCanceledException)
            {
                var innerException = new TimeoutException($"InsertMany operation timed out after {timeout.TotalSeconds} seconds. Consider increasing the timeout using the CommandOptions.TimeoutOptions.BulkOperationTimeout parameter.");
                throw new BulkOperationException<TableInsertManyResult>(innerException, result);
            }
            catch (Exception ex)
            {
                throw new BulkOperationException<TableInsertManyResult>(ex, result);
            }

            return result;
        }
    }

    private async Task<TableInsertManyResult> RunInsertManyAsync(IEnumerable<T> rows, bool insertOrdered, bool returnDocumentResponses, CommandOptions commandOptions, bool runSynchronously)
    {
        var payload = new
        {
            documents = rows,
            options = new
            {
                ordered = insertOrdered,
                returnDocumentResponses
            }
        };
        commandOptions = SetRowSerializationOptions<T>(commandOptions, true);
        var command = CreateCommand("insertMany").WithPayload(payload).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnStatus<TableInsertManyResult>(runSynchronously).ConfigureAwait(false);
        return response.Result;
    }

    /// <summary>
    /// Synchronous version of <see cref="InsertOneAsync(T)"/>
    /// </summary>
    /// <inheritdoc cref="InsertOneAsync(T)"/>
    public TableInsertOneResult InsertOne(T row)
    {
        return InsertOne(row, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="InsertOneAsync(T, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="InsertOneAsync(T, CommandOptions)"/>
    public TableInsertOneResult InsertOne(T row, CommandOptions commandOptions)
    {
        return InsertOneAsync(row, commandOptions, true).ResultSync();
    }

    /// <summary>
    /// Insert a single row into the table.
    /// </summary>
    /// <param name="row"></param>
    /// <returns></returns>
    public Task<TableInsertOneResult> InsertOneAsync(T row)
    {
        return InsertOneAsync(row, null);
    }

    /// <inheritdoc cref="InsertOneAsync(T)"/>
    /// <param name="commandOptions"></param>
    public Task<TableInsertOneResult> InsertOneAsync(T row, CommandOptions commandOptions)
    {
        return InsertOneAsync(row, commandOptions, false);
    }

    private async Task<TableInsertOneResult> InsertOneAsync(T row, CommandOptions commandOptions, bool runSynchronously)
    {
        var payload = new
        {
            document = row,
        };
        commandOptions = SetRowSerializationOptions<T>(commandOptions, true);
        var command = CreateCommand("insertOne").WithPayload(payload).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnStatus<TableInsertOneResult>(runSynchronously).ConfigureAwait(false);
        return response.Result;
    }

    /// <summary>
    /// Find rows in the table.
    /// 
    /// The Find() methods return a <see cref="FindEnumerator{T,T,SortBuilder{T}}"/> object that can be used to further structure the query
    /// by adding Sort, Projection, Skip, Limit, etc. to affect the final results.
    /// 
    /// The <see cref="FindEnumerator{T,T,SortBuilder{T}}"/> object can be directly enumerated both synchronously and asynchronously.
    /// Secondarily, the results can be paged through manually by using the results of <see cref="FindEnumerator{T,T,SortBuilder{T}}.ToCursor()"/>.
    /// </summary>
    /// <returns></returns>
    /// <example>
    /// Synchronous Enumeration:
    /// <code>
    /// var FindEnumerator = table.Find();
    /// foreach (var row in FindEnumerator)
    /// {
    ///     // Process row
    /// }
    /// </code>
    /// </example>
    /// <example>
    /// Asynchronous Enumeration:
    /// <code>
    /// var results = table.Find();
    /// await foreach (var row in results)
    /// {
    ///     // Process row
    /// }
    /// </code>
    /// </example>
    /// <remarks>
    /// Timeouts passed in the <see cref="CommandOptions"/> (<see cref="CommandOptions.TimeoutOptions.ConnectionTimeout"/>
    /// and <see cref="CommandOptions.TimeoutOptions.RequestTimeout"/>) will be used for each batched request to the API,
    /// however <see cref="CommandOptions.TimeoutOptions.BulkOperationCancellationToken"/> settings are ignored due to the nature of Enueration.
    /// If you need to enforce a timeout for the entire operation, you can pass a <see cref="CancellationToken"/> to GetAsyncEnumerator.
    /// </remarks>
    public FindEnumerator<T, T, SortBuilder<T>> Find()
    {
        return Find(null, null);
    }

    /// <inheritdoc cref="Find()"/>
    /// <param name="filter">The filter(s) to apply to the query.</param>
    /// <returns></returns>
    /// <example>
    /// <code>
    /// var filterBuilder = Builders<BookRow>.Filter;
    /// var filter = filterBuilder.Gt(x => x.NumberOfPages, 430);
    /// var matchingBooks = table.Find(filter).ToList();
    /// await foreach (var bookRow in matchingBooks)
    /// {
    ///     //handle each row
    /// }
    /// </code>
    /// </example>
    public FindEnumerator<T, T, SortBuilder<T>> Find(Filter<T> filter)
    {
        return Find(filter, null);
    }

    /// <inheritdoc cref="Find(Filter{T})"/>
    /// <param name="commandOptions"></param>
    public FindEnumerator<T, T, SortBuilder<T>> Find(Filter<T> filter, CommandOptions commandOptions)
    {
        return Find<T>(filter, commandOptions);
    }

    /// <inheritdoc cref="Find(Filter{T},CommandOptions)"/>
    /// <typeparam name="TResult"></typeparam>
    /// <returns></returns>
    /// <remarks>
    /// This overload of Find() allows you to specify a different result class type <typeparamref name="TResult"/>
    /// which the resultant rows will be deserialized into. This is generally used along with .Project() to limit the fields returned
    /// </remarks>
    public FindEnumerator<T, TResult, SortBuilder<T>> Find<TResult>(Filter<T> filter, CommandOptions commandOptions) where TResult : class
    {
        var findOptions = new TableFindManyOptions<T>
        {
            Filter = filter
        };
        return new FindEnumerator<T, TResult, SortBuilder<T>>(this, findOptions, commandOptions);
    }

    internal async Task<ApiResponseWithData<ApiFindResult<TResult>, FindStatusResult>> RunFindManyAsync<TResult>(Filter<T> filter, IFindManyOptions<T, SortBuilder<T>> findOptions, CommandOptions commandOptions, bool runSynchronously)
        where TResult : class
    {
        findOptions.Filter = filter;
        commandOptions = SetRowSerializationOptions<TResult>(commandOptions, false);
        var command = CreateCommand("find").WithPayload(findOptions).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnData<ApiFindResult<TResult>, FindStatusResult>(runSynchronously).ConfigureAwait(false);
        return response;
    }

    /// <inheritdoc cref="FindOneAsync()"/>
    /// Synchronous version of <see cref="FindOneAsync()"/>
    public T FindOne()
    {
        return FindOne(null, null, null);
    }

    /// <inheritdoc cref="FindOneAsync(Filter{T})"/>
    /// Synchronous version of <see cref="FindOneAsync(Filter{T})"/>
    public T FindOne(Filter<T> filter)
    {
        return FindOne(filter, null, null);
    }

    /// <inheritdoc cref="FindOneAsync(Filter{T}, CommandOptions)"/>
    /// Synchronous version of <see cref="FindOneAsync(Filter{T}, CommandOptions)"/>
    public T FindOne(Filter<T> filter, CommandOptions commandOptions)
    {
        return FindOne(filter, null, commandOptions);
    }

    /// <inheritdoc cref="FindOneAsync(TableFindOptions{T})"/>
    /// Synchronous version of <see cref="FindOneAsync(TableFindOptions{T})"/>
    public T FindOne(TableFindOptions<T> findOptions)
    {
        return FindOne<T>(null, findOptions, null);
    }

    /// <inheritdoc cref="FindOneAsync(Filter{T}, TableFindOptions{T})"/>
    /// Synchronous version of <see cref="FindOneAsync(Filter{T}, TableFindOptions{T})"/>
    public T FindOne(Filter<T> filter, TableFindOptions<T> findOptions)
    {
        return FindOne<T>(filter, findOptions, null);
    }

    /// <inheritdoc cref="FindOneAsync(Filter{T}, TableFindOptions{T}, CommandOptions)"/>
    /// Synchronous version of <see cref="FindOneAsync(Filter{T}, TableFindOptions{T}, CommandOptions)"/> 
    public T FindOne(Filter<T> filter, TableFindOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOne<T>(filter, findOptions, commandOptions);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}()"/>
    /// Synchronous version of <see cref="FindOneAsync{TResult}()"/>
    public TResult FindOne<TResult>() where TResult : class
    {
        return FindOne<TResult>(null, null, null);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}(Filter{T})"/>
    /// Synchronous version of <see cref="FindOneAsync{TResult}(Filter{T})"/>
    public TResult FindOne<TResult>(Filter<T> filter) where TResult : class
    {
        return FindOne<TResult>(filter, null, null);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}(Filter{T}, CommandOptions)"/>
    /// Synchronous version of <see cref="FindOneAsync{TResult}(Filter{T}, CommandOptions)"/>
    public TResult FindOne<TResult>(Filter<T> filter, CommandOptions commandOptions) where TResult : class
    {
        return FindOne<TResult>(filter, null, commandOptions);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}(Filter{T}, TableFindOptions{T})"/>
    /// Synchronous version of <see cref="FindOneAsync{TResult}(Filter{T}, TableFindOptions{T})"/>
    public TResult FindOne<TResult>(Filter<T> filter, TableFindOptions<T> findOptions) where TResult : class
    {
        return FindOne<TResult>(filter, findOptions, null);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}(Filter{T}, TableFindOptions{T}, CommandOptions)"/>
    /// Synchronous version of <see cref="FindOneAsync{TResult}(Filter{T}, TableFindOptions{T}, CommandOptions)"/>
    public TResult FindOne<TResult>(Filter<T> filter, TableFindOptions<T> findOptions, CommandOptions commandOptions) where TResult : class
    {
        return FindOneAsync<TResult>(filter, findOptions, commandOptions, true).ResultSync();
    }

    /// <summary>
    /// Find a single row in the table.
    /// </summary>
    /// <returns></returns>
    public Task<T> FindOneAsync()
    {
        return FindOneAsync(null, null, null);
    }

    /// <summary>
    /// Find a single row in the table that matches the specified filter.
    /// </summary>
    /// <param name="filter"></param>
    /// <returns></returns>
    public Task<T> FindOneAsync(Filter<T> filter)
    {
        return FindOneAsync(filter, null, null);
    }

    /// <inheritdoc cref="FindOneAsync(Filter{T})"/>
    /// <param name="commandOptions"></param>
    public Task<T> FindOneAsync(Filter<T> filter, CommandOptions commandOptions)
    {
        return FindOneAsync(filter, null, commandOptions);
    }

    /// <summary>
    /// Find a single row in the table using the specified find options.
    /// </summary>
    /// <param name="findOptions">Specify Sort options for the find operation.</param>
    /// <returns></returns>
    public Task<T> FindOneAsync(TableFindOptions<T> findOptions)
    {
        return FindOneAsync<T>(null, findOptions, null);
    }

    /// <inheritdoc cref="FindOneAsync(Filter{T})"/>
    /// <param name="findOptions">Specify Sort options for the find operation.</param>
    public Task<T> FindOneAsync(Filter<T> filter, TableFindOptions<T> findOptions)
    {
        return FindOneAsync<T>(filter, findOptions, null);
    }

    /// <inheritdoc cref="FindOneAsync(Filter{T}, TableFindOptions{T})"/>
    /// <param name="commandOptions"></param>
    public Task<T> FindOneAsync(Filter<T> filter, TableFindOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOneAsync<T>(filter, findOptions, commandOptions);
    }

    /// <summary>
    /// Find a single row in the table, specifying a different result row class type <typeparamref name="TResult"/>
    ///  (useful when you want to project only certain fields).
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    /// <returns></returns>
    public Task<TResult> FindOneAsync<TResult>() where TResult : class
    {
        return FindOneAsync<TResult>(null, null, null);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}()"/>
    /// <param name="filter"></param>
    /// <returns></returns>
    public Task<TResult> FindOneAsync<TResult>(Filter<T> filter) where TResult : class
    {
        return FindOneAsync<TResult>(filter, null, null);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}(Filter{T})"/>
    /// <param name="commandOptions"></param>
    public Task<TResult> FindOneAsync<TResult>(Filter<T> filter, CommandOptions commandOptions) where TResult : class
    {
        return FindOneAsync<TResult>(filter, null, commandOptions);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}(Filter{T})"/>
    /// <param name="findOptions">Specify Sort options for the find operation.</param>
    public Task<TResult> FindOneAsync<TResult>(Filter<T> filter, TableFindOptions<T> findOptions) where TResult : class
    {
        return FindOneAsync<TResult>(filter, findOptions, null);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}(Filter{T}, TableFindOptions{T})"/>
    /// <param name="commandOptions"></param>
    public Task<TResult> FindOneAsync<TResult>(Filter<T> filter, TableFindOptions<T> findOptions, CommandOptions commandOptions) where TResult : class
    {
        return FindOneAsync<TResult>(filter, findOptions, commandOptions, false);
    }

    internal async Task<TResult> FindOneAsync<TResult>(Filter<T> filter, TableFindOptions<T> findOptions, CommandOptions commandOptions, bool runSynchronously)
        where TResult : class
    {
        findOptions ??= new TableFindOptions<T>();
        findOptions.Filter = filter;
        commandOptions = SetRowSerializationOptions<TResult>(commandOptions, false);
        var command = CreateCommand("findOne").WithPayload(findOptions).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnData<DocumentResult<TResult>, FindStatusResult>(runSynchronously).ConfigureAwait(false);
        return response.Data.Document;
    }

    /// <summary>
    /// Synchronous version of <see cref="DropAsync()"/>
    /// </summary>
    /// <inheritdoc cref="DropAsync()"/>
    public void Drop()
    {
        DropAsync().ResultSync();
    }

    /// <summary>
    /// Drops the table.
    /// </summary>
    public Task DropAsync()
    {
        return _database.DropTableAsync(_tableName);
    }

    private CommandOptions SetRowSerializationOptions<TResult>(CommandOptions commandOptions, bool isInsert)
        where TResult : class
    {
        commandOptions ??= new CommandOptions();
        commandOptions.SerializeGuidAsDollarUuid = false;
        commandOptions.SerializeDateAsDollarDate = false;
        if (typeof(TResult) == typeof(Row))
        {
            return commandOptions;
        }
        if (isInsert)
        {
            commandOptions.InputConverter = new RowConverter<T>();
        }
        else
        {
            commandOptions.OutputConverter = new RowConverter<TResult>();
        }
        return commandOptions;
    }

    /// <summary>
    /// Synchronous version of <see cref="UpdateOneAsync(Filter{T}, UpdateBuilder{T})"/>
    /// </summary>
    /// <inheritdoc cref="UpdateOneAsync(Filter{T}, UpdateBuilder{T})"/>
    public void UpdateOne(Filter<T> filter, UpdateBuilder<T> update)
    {
        UpdateOne(filter, update, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="UpdateOneAsync(Filter{T}, UpdateBuilder{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="UpdateOneAsync(Filter{T}, UpdateBuilder{T}, CommandOptions)"/>
    public void UpdateOne(Filter<T> filter, UpdateBuilder<T> update, CommandOptions commandOptions)
    {
        UpdateOneAsync(filter, update, commandOptions, true).ResultSync();
    }

    /// <summary>
    /// Update a single row in the table using the provided filter and update builder.
    /// 
    /// </summary>
    /// <param name="filter"></param>
    /// <param name="update"></param>
    /// <returns></returns>
    public Task UpdateOneAsync(Filter<T> filter, UpdateBuilder<T> update)
    {
        return UpdateOneAsync(filter, update, null);
    }

    /// <inheritdoc cref="UpdateOneAsync(Filter{T}, UpdateBuilder{T})"/>
    /// <param name="commandOptions"></param>
    public Task UpdateOneAsync(Filter<T> filter, UpdateBuilder<T> update, CommandOptions commandOptions)
    {
        return UpdateOneAsync(filter, update, commandOptions, false);
    }

    internal async Task UpdateOneAsync(Filter<T> filter, UpdateBuilder<T> update, CommandOptions commandOptions, bool runSynchronously)
    {
        var updateOptions = new UpdateOneOptions<T>
        {
            Filter = filter,
            Update = update
        };
        var command = CreateCommand("updateOne").WithPayload(updateOptions).AddCommandOptions(commandOptions);
        await command.RunAsyncReturnStatus<UpdateResult>(runSynchronously).ConfigureAwait(false);
    }

    /// <summary>
    /// This is a synchronous version of <see cref="DeleteOneAsync(DeleteOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="DeleteOneAsync(TableDeleteOptions{T})"/>
    public DeleteResult DeleteOne(TableDeleteOptions<T> deleteOptions)
    {
        return DeleteOne(null, deleteOptions, null);
    }

    /// <summary>
    /// This is a synchronous version of <see cref="DeleteOneAsync(Filter{T})"/>
    /// </summary>
    /// <inheritdoc cref="DeleteOneAsync(Filter{T})"/>
    public DeleteResult DeleteOne(Filter<T> filter)
    {
        return DeleteOne(filter, null, null);
    }

    /// <summary>
    /// This is a synchronous version of <see cref="DeleteOneAsync(Filter{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="DeleteOneAsync(Filter{T}, CommandOptions)"/>
    public DeleteResult DeleteOne(Filter<T> filter, CommandOptions commandOptions)
    {
        return DeleteOne(filter, null, commandOptions);
    }

    /// <summary>
    /// This is a synchronous version of <see cref="DeleteOneAsync(TableDeleteOptions{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="DeleteOneAsync(TableDeleteOptions{T}, CommandOptions)"/>
    public DeleteResult DeleteOne(TableDeleteOptions<T> deleteOptions, CommandOptions commandOptions)
    {
        return DeleteOne(null, deleteOptions, commandOptions);
    }

    /// <summary>
    /// This is a synchronous version of <see cref="DeleteOneAsync(Filter{T}, TableDeleteOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="DeleteOneAsync(Filter{T}, TableDeleteOptions{T})"/>
    public DeleteResult DeleteOne(Filter<T> filter, TableDeleteOptions<T> deleteOptions)
    {
        return DeleteOne(filter, deleteOptions, null);
    }

    /// <summary>
    /// This is a synchronous version of <see cref="DeleteOneAsync(Filter{T}, TableDeleteOptions{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="DeleteOneAsync(Filter{T}, TableDeleteOptions{T}, CommandOptions)"/>
    public DeleteResult DeleteOne(Filter<T> filter, TableDeleteOptions<T> deleteOptions, CommandOptions commandOptions)
    {
        var response = DeleteOneAsync(filter, deleteOptions, commandOptions, true).ResultSync();
        return response;
    }

    /// <summary>
    /// Delete a row from the table.
    /// </summary>
    /// <param name="deleteOptions"></param>
    /// <returns></returns>
    public Task<DeleteResult> DeleteOneAsync(TableDeleteOptions<T> deleteOptions)
    {
        return DeleteOneAsync(null, deleteOptions, null);
    }

    /// <summary>
    /// Delete a row from the table.
    /// </summary>
    /// <param name="filter"></param>
    /// <returns></returns>
    public Task<DeleteResult> DeleteOneAsync(Filter<T> filter)
    {
        return DeleteOneAsync(filter, null, null);
    }

    /// <inheritdoc cref="DeleteOneAsync(Filter{T})"/>
    /// <param name="commandOptions"></param>
    public Task<DeleteResult> DeleteOneAsync(Filter<T> filter, CommandOptions commandOptions)
    {
        return DeleteOneAsync(filter, null, commandOptions);
    }

    /// <inheritdoc cref="DeleteOneAsync(TableDeleteOptions{T})"/>
    /// <param name="commandOptions"></param>
    public Task<DeleteResult> DeleteOneAsync(TableDeleteOptions<T> deleteOptions, CommandOptions commandOptions)
    {
        return DeleteOneAsync(null, deleteOptions, commandOptions);
    }

    /// <inheritdoc cref="DeleteOneAsync(Filter{T})"/>
    /// <param name="deleteOptions"></param>
    public Task<DeleteResult> DeleteOneAsync(Filter<T> filter, TableDeleteOptions<T> deleteOptions)
    {
        return DeleteOneAsync(filter, deleteOptions, null);
    }

    /// <inheritdoc cref="DeleteOneAsync(Filter{T}, TableDeleteOptions{T})"/>
    /// <param name="commandOptions"></param>
    public Task<DeleteResult> DeleteOneAsync(Filter<T> filter, TableDeleteOptions<T> deleteOptions, CommandOptions commandOptions)
    {
        return DeleteOneAsync(filter, deleteOptions, commandOptions, false);
    }

    internal async Task<DeleteResult> DeleteOneAsync(Filter<T> filter, TableDeleteOptions<T> deleteOptions, CommandOptions commandOptions, bool runSynchronously)
    {
        deleteOptions ??= new TableDeleteOptions<T>();
        deleteOptions.Filter = filter;
        var command = CreateCommand("deleteOne").WithPayload(deleteOptions).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnStatus<DeleteResult>(runSynchronously).ConfigureAwait(false);
        return response.Result;
    }

    /// <inheritdoc cref="DeleteManyAsync(Filter{T})"/>
    /// Synchronous version of <see cref="DeleteManyAsync(Filter{T})"/>
    public void DeleteMany(Filter<T> filter)
    {
        DeleteMany(filter, null);
    }

    /// <inheritdoc cref="DeleteManyAsync(Filter{T}, CommandOptions)"/>
    /// Synchronous version of <see cref="DeleteManyAsync(Filter{T}, CommandOptions)"/>
    public void DeleteMany(Filter<T> filter, CommandOptions commandOptions)
    {
        DeleteManyAsync(filter, commandOptions, true).ResultSync();
    }

    /// <summary>
    /// Delete all documents matching the filter from the table.
    /// </summary>
    /// <param name="filter"></param>
    /// <returns></returns>
    public Task DeleteManyAsync(Filter<T> filter)
    {
        return DeleteManyAsync(filter, null);
    }

    /// <inheritdoc cref="DeleteManyAsync(Filter{T})"/>
    /// <param name="commandOptions"></param>
    public Task DeleteManyAsync(Filter<T> filter, CommandOptions commandOptions)
    {
        return DeleteManyAsync(filter, commandOptions, false);
    }

    internal async Task DeleteManyAsync(Filter<T> filter, CommandOptions commandOptions, bool runSynchronously)
    {
        var deleteOptions = new DeleteManyOptions<T>
        {
            Filter = filter
        };

        var keepProcessing = true;
        var deleteResult = new DeleteResult();
        var (timeout, cts) = BulkOperationHelper.InitTimeout(GetOptionsTree(), ref commandOptions);

        using (cts)
        {
            var bulkOperationTimeoutToken = cts.Token;
            try
            {
                while (keepProcessing)
                {
                    var command = CreateCommand("deleteMany").WithPayload(deleteOptions).AddCommandOptions(commandOptions);
                    var response = await command.RunAsyncReturnStatus<DeleteResult>(runSynchronously).ConfigureAwait(false);
                    deleteResult.DeletedCount += response.Result.DeletedCount;
                    keepProcessing = response.Result.MoreData;
                }
            }
            catch (OperationCanceledException)
            {
                var innerException = new TimeoutException($"DeleteMany operation timed out after {timeout.TotalSeconds} seconds. Consider increasing the timeout using the CommandOptions.TimeoutOptions.BulkOperationTimeout parameter.");
                throw new BulkOperationException<DeleteResult>(innerException, deleteResult);
            }
            catch (Exception ex)
            {
                throw new BulkOperationException<DeleteResult>(ex, deleteResult);
            }
        }
    }

    /// <summary>
    /// Synchronous version of <see cref="DeleteAllAsync()"/>
    /// </summary>
    /// <inheritdoc cref="DeleteAllAsync()"/>
    public void DeleteAll()
    {
        DeleteAllAsync().ResultSync();
    }

    /// <summary>
    /// Delete all rows from the table.
    /// </summary>
    public Task DeleteAllAsync()
    {
        return DeleteManyAsync(null, null);
    }

    /// <summary>
    /// This is a synchronous version of <see cref="AlterAsync(IAlterTableOperation)"/>.
    /// </summary>
    /// <inheritdoc cref="AlterAsync(IAlterTableOperation)"/>
    public Table<T> Alter(IAlterTableOperation operation)
    {
        return Alter(operation, null);
    }

    /// <summary>
    /// This is a synchronous version of <see cref="AlterAsync(IAlterTableOperation, CommandOptions)"/>.
    /// </summary>
    /// <inheritdoc cref="AlterAsync(IAlterTableOperation, CommandOptions)"/>
    public Table<T> Alter(IAlterTableOperation operation, CommandOptions commandOptions)
    {
        var response = AlterAsync<T>(operation, commandOptions, true).ResultSync();
        return response;
    }

    /// <summary>
    /// Alters a table using the specified operation.
    /// </summary>
    /// <param name="operation">The alteration operation to apply.</param>
    /// <returns>The status result of the alterTable command.</returns>
    public Task<Table<T>> AlterAsync(IAlterTableOperation operation)
    {
        return AlterAsync<T>(operation, null, false);
    }

    /// <inheritdoc cref="AlterAsync(IAlterTableOperation)"/>
    /// <param name="operation">The alteration operation to apply.</param>
    /// <param name="commandOptions">Options to customize the command execution.</param>
    public Task<Table<T>> AlterAsync(IAlterTableOperation operation, CommandOptions commandOptions)
    {
        return AlterAsync<T>(operation, commandOptions, false);
    }

    /// <summary>
    /// This is a synchronous version of <see cref="AlterAsync(IAlterTableOperation)"/>.
    /// </summary>
    /// <inheritdoc cref="AlterAsync(IAlterTableOperation)"/>
    public Table<TRowAfterAlter> Alter<TRowAfterAlter>(IAlterTableOperation operation)
        where TRowAfterAlter : class
    {
        return Alter<TRowAfterAlter>(operation, null);
    }

    /// <summary>
    /// This is a synchronous version of <see cref="AlterAsync(IAlterTableOperation, CommandOptions)"/>.
    /// </summary>
    /// <inheritdoc cref="AlterAsync(IAlterTableOperation, CommandOptions)"/>
    public Table<TRowAfterAlter> Alter<TRowAfterAlter>(IAlterTableOperation operation, CommandOptions commandOptions)
        where TRowAfterAlter : class
    {
        var response = AlterAsync<TRowAfterAlter>(operation, commandOptions, true).ResultSync();
        return response;
    }

    /// <summary>
    /// Alters a table using the specified operation.
    /// </summary>
    /// <param name="operation">The alteration operation to apply.</param>
    /// <returns>The status result of the alterTable command.</returns>
    public Task<Table<TRowAfterAlter>> AlterAsync<TRowAfterAlter>(IAlterTableOperation operation)
        where TRowAfterAlter : class
    {
        return AlterAsync<TRowAfterAlter>(operation, null, false);
    }

    /// <inheritdoc cref="AlterAsync(IAlterTableOperation)"/>
    /// <param name="operation">The alteration operation to apply.</param>
    /// <param name="commandOptions">Options to customize the command execution.</param>
    public Task<Table<TRowAfterAlter>> AlterAsync<TRowAfterAlter>(IAlterTableOperation operation, CommandOptions commandOptions)
        where TRowAfterAlter : class
    {
        return AlterAsync<TRowAfterAlter>(operation, commandOptions, false);
    }

    internal async Task<Table<TRowAfterAlter>> AlterAsync<TRowAfterAlter>(IAlterTableOperation operation, CommandOptions commandOptions, bool runSynchronously)
        where TRowAfterAlter : class
    {
        var payload = new
        {
            operation = operation.ToJsonFragment()
        };

        var command = CreateCommand("alterTable")
            .WithPayload(payload)
            .AddCommandOptions(commandOptions);

        await command.RunAsyncReturnStatus<Dictionary<string, int>>(runSynchronously).ConfigureAwait(false);

        return new Table<TRowAfterAlter>(_tableName, _database, _commandOptions);

    }

    private List<CommandOptions> GetOptionsTree()
    {
        var optionsTree = _commandOptions == null ? _database.OptionsTree : _database.OptionsTree.Concat(new[] { _commandOptions });
        return optionsTree.ToList();
    }

    internal Command CreateCommand(string name)
    {
        var optionsTree = GetOptionsTree().ToArray();
        return new Command(name, _database.Client, optionsTree, new DatabaseCommandUrlBuilder(_database, _tableName));
    }

    Task<ApiResponseWithData<ApiFindResult<TProjected>, FindStatusResult>> IQueryRunner<T, SortBuilder<T>>.RunFindManyAsync<TProjected>(Filter<T> filter, IFindManyOptions<T, SortBuilder<T>> findOptions, CommandOptions commandOptions, bool runSynchronously)
        where TProjected : class
    {
        return RunFindManyAsync<TProjected>(filter, findOptions, commandOptions, runSynchronously);
    }
}
