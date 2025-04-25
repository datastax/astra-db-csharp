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


public class Table<T> : IQueryRunner<T> where T : class
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

    public Task<TableInsertManyResult> InsertManyAsync(IEnumerable<T> rows)
    {
        return InsertManyAsync(rows, null, null, false);
    }

    private async Task<TableInsertManyResult> InsertManyAsync(IEnumerable<T> rows, InsertManyOptions insertOptions, CommandOptions commandOptions, bool runSynchronously)
    {
        Guard.NotNullOrEmpty(rows, nameof(rows));

        if (insertOptions == null) insertOptions = new InsertManyOptions();
        if (insertOptions.Concurrency > 1 && insertOptions.InsertInOrder)
        {
            throw new ArgumentException("Cannot run ordered insert_many concurrently.");
        }

        // foreach (var doc in documents)
        // {
        //     InsertValidator.Validate(doc);
        // }

        var start = DateTime.Now;

        var result = new TableInsertManyResult();
        var tasks = new List<Task>();
        var semaphore = new SemaphoreSlim(insertOptions.Concurrency);

        var chunks = rows.CreateBatch(insertOptions.ChunkSize);

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
                        result.PrimaryKeys = runResult.PrimaryKeys;
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

    private async Task<TableInsertManyResult> RunInsertManyAsync(IEnumerable<T> rows, bool insertOrdered, CommandOptions commandOptions, bool runSynchronously)
    {
        var payload = new
        {
            documents = rows,
            options = new
            {
                ordered = insertOrdered,
                returnDocumentResponses = false
            }
        };
        commandOptions = SetRowSerializationOptions(commandOptions);
        var command = CreateCommand("insertMany").WithPayload(payload).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnStatus<TableInsertManyResult>(runSynchronously).ConfigureAwait(false);
        return response.Result;
    }

    private CommandOptions SetRowSerializationOptions(CommandOptions commandOptions)
    {
        commandOptions ??= new CommandOptions();
        commandOptions.SerializeGuidAsDollarUuid = false;
        commandOptions.SerializeDateAsDollarDate = false;
        commandOptions.InputConverter = new RowConverter<T>();
        commandOptions.OutputConverter = new RowConverter<T>();
        return commandOptions;
    }

    public ResultSet<T, T> Find(Filter<T> filter, FindOptions<T> findOptions, CommandOptions commandOptions)
    {
        return new ResultSet<T, T>(this, filter, findOptions, commandOptions);
    }

    public ResultSet<T, TResult> Find<TResult>(Filter<T> filter, FindOptions<T> findOptions, CommandOptions commandOptions) where TResult : class
    {
        return new ResultSet<T, TResult>(this, filter, findOptions, commandOptions);
    }

    internal async Task<ApiResponseWithData<DocumentsResult<TResult>, FindStatusResult>> RunFindManyAsync<TResult>(Filter<T> filter, FindOptions<T> findOptions, CommandOptions commandOptions, bool runSynchronously)
    {
        findOptions.Filter = filter;
        commandOptions = SetRowSerializationOptions(commandOptions);
        var command = CreateCommand("find").WithPayload(findOptions).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnData<DocumentsResult<TResult>, FindStatusResult>(runSynchronously).ConfigureAwait(false);
        return response;
    }

    internal Command CreateCommand(string name)
    {
        var optionsTree = _commandOptions == null ? _database.OptionsTree : _database.OptionsTree.Concat(new[] { _commandOptions }).ToArray();
        return new Command(name, _database.Client, optionsTree, new DatabaseCommandUrlBuilder(_database, _tableName));
    }

    Task<ApiResponseWithData<DocumentsResult<TProjected>, FindStatusResult>> IQueryRunner<T>.RunFindManyAsync<TProjected>(Filter<T> filter, FindOptions<T> findOptions, CommandOptions commandOptions, bool runSynchronously)
    {
        return RunFindManyAsync<TProjected>(filter, findOptions, commandOptions, runSynchronously);
    }

}
