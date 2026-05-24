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
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Tables;


/// <summary>
/// This is the main entry point for interacting with a table in the Astra DB Data API.
/// </summary>
/// <typeparam name="T">The type to use for rows in the table (when not specified, defaults to <see cref="Row"/> </typeparam>
public class Table<T> where T : class
{
    private readonly Database _database;
    private readonly CommandOptions _commandOptions;

    /// <summary>
    /// Access the name of the table
    /// </summary>
    public string TableName { get; }

    internal Table(string tableName, Database database, CommandOptions commandOptions)
    {
        TableName = tableName;
        _database = database;
        _commandOptions = commandOptions;
    }

    /// <summary>
    /// Synchronous version of <see cref="ListIndexesAsync()"/>
    /// </summary>
    /// <returns></returns>
    public List<TableIndexMetadata> ListIndexes()
    {
        return ListIndexes(null);
    }

    /// <summary>
    /// Synchronous version of <see cref="ListIndexesAsync(CommandOptions)"/>
    /// </summary>
    /// <param name="commandOptions"></param>
    /// <returns></returns>
    public List<TableIndexMetadata> ListIndexes(CommandOptions commandOptions)
    {
        return ListIndexesAsync(commandOptions, true).ResultSync();
    }

    /// <summary>
    /// Get a list of indexes for the table.
    /// </summary>
    /// <returns></returns>
    public Task<List<TableIndexMetadata>> ListIndexesAsync()
    {
        return ListIndexesAsync(null);
    }

    /// <summary>
    /// Get a list of indexes for the table.
    /// </summary>
    /// <param name="commandOptions"></param>
    /// <returns></returns>
    public Task<List<TableIndexMetadata>> ListIndexesAsync(CommandOptions commandOptions)
    {
        return ListIndexesAsync(commandOptions, false);
    }

    private async Task<List<TableIndexMetadata>> ListIndexesAsync(CommandOptions commandOptions, bool runSynchronously)
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
        return response.Result.Indexes;
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
    /// Synchronous version of <see cref="CreateIndexAsync(string, string)"/>
    /// </summary>
    /// <inheritdoc cref="CreateIndexAsync(string, string)"/>
    public void CreateIndex<TColumn>(string indexName, Expression<Func<T, TColumn>> column)
    {
        CreateIndex(indexName, column.GetMemberNameTree(), null, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateIndexAsync{TColumn}(string, Expression{Func{T, TColumn}})"/>
    /// </summary>
    /// <inheritdoc cref="CreateIndexAsync{TColumn}(string, Expression{Func{T, TColumn}})"/>
    public void CreateIndex(string indexName, string columnName)
    {
        CreateIndex(indexName, columnName, null, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateIndexAsync{TColumn}(string, Expression{Func{T, TColumn}}, CreateIndexCommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateIndexAsync{TColumn}(string, Expression{Func{T, TColumn}}, CreateIndexCommandOptions)"/>
    public void CreateIndex<TColumn>(string indexName, Expression<Func<T, TColumn>> column, CreateIndexCommandOptions commandOptions)
    {
        CreateIndex(indexName, column.GetMemberNameTree(), null, commandOptions);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateIndexAsync(string, string, CreateIndexCommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateIndexAsync(string, string, CreateIndexCommandOptions)"/>
    public void CreateIndex(string indexName, string columnName, CreateIndexCommandOptions commandOptions)
    {
        CreateIndex(indexName, columnName, null, commandOptions);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateIndexAsync{TColumn}(string, Expression{Func{T, TColumn}}, TableIndexDefinition)"/>
    /// </summary>
    /// <inheritdoc cref="CreateIndexAsync{TColumn}(string, Expression{Func{T, TColumn}}, TableIndexDefinition)"/>
    public void CreateIndex<TColumn>(string indexName, Expression<Func<T, TColumn>> column, TableIndexDefinition indexDefinition)
    {
        CreateIndex(indexName, column.GetMemberNameTree(), indexDefinition, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateIndexAsync(string, string, TableIndexDefinition)"/>
    /// </summary>
    /// <inheritdoc cref="CreateIndexAsync(string, string, TableIndexDefinition)"/>
    public void CreateIndex(string indexName, string columnName, TableIndexDefinition indexDefinition)
    {
        CreateIndex(indexName, columnName, indexDefinition, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateIndexAsync{TColumn}(string, Expression{Func{T, TColumn}}, TableIndexDefinition, CreateIndexCommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateIndexAsync{TColumn}(string, Expression{Func{T, TColumn}}, TableIndexDefinition, CreateIndexCommandOptions)"/>
    public void CreateIndex<TColumn>(string indexName, Expression<Func<T, TColumn>> column, TableIndexDefinition indexDefinition, CreateIndexCommandOptions commandOptions)
    {
        CreateIndex(indexName, column.GetMemberNameTree(), indexDefinition, commandOptions);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateIndexAsync(string, string, TableIndexDefinition, CreateIndexCommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateIndexAsync(string, string, TableIndexDefinition, CreateIndexCommandOptions)"/>
    public void CreateIndex(string indexName, string columnName, TableIndexDefinition indexDefinition, CreateIndexCommandOptions commandOptions)
    {
        if (indexDefinition == null)
        {
            indexDefinition = new TableIndexDefinition();
        }
        CreateGenericIndexAsync(indexName, columnName, indexDefinition, commandOptions, false).ResultSync();
    }

    /// <summary>
    /// Create an index on the specified column
    /// </summary>
    /// <typeparam name="TColumn">The type of the column to index</typeparam>
    /// <param name="indexName">The index name</param>
    /// <param name="column">The column to index</param>
    /// <returns></returns>
    public Task CreateIndexAsync<TColumn>(string indexName, Expression<Func<T, TColumn>> column)
    {
        return CreateIndexAsync(indexName, column.GetMemberNameTree(), null, null);
    }

    /// <summary>
    /// Create an index on the specified column
    /// </summary>
    /// <param name="indexName">The index name</param>
    /// <param name="columnName">The name of the column to index</param>
    /// <returns></returns>
    public Task CreateIndexAsync(string indexName, string columnName)
    {
        return CreateIndexAsync(indexName, columnName, null, null);
    }

    /// <inheritdoc cref="CreateIndexAsync{TColumn}(string, Expression{Func{T, TColumn}})"/>
    /// <param name="indexName">The index name</param>
    /// <param name="column">The column to index</param>
    /// <param name="commandOptions"></param>
    public Task CreateIndexAsync<TColumn>(string indexName, Expression<Func<T, TColumn>> column, CreateIndexCommandOptions commandOptions)
    {
        return CreateIndexAsync(indexName, column.GetMemberNameTree(), null, commandOptions);
    }

    /// <inheritdoc cref="CreateIndexAsync(string, string)"/>
    /// <param name="indexName">The index name</param>
    /// <param name="columnName">The name of the column to index</param>
    /// <param name="commandOptions"></param>
    public Task CreateIndexAsync(string indexName, string columnName, CreateIndexCommandOptions commandOptions)
    {
        return CreateIndexAsync(indexName, columnName, null, commandOptions);
    }

    /// <inheritdoc cref="CreateIndexAsync{TColumn}(string, Expression{Func{T, TColumn}})"/>
    /// <param name="indexName">The index name</param>
    /// <param name="column">The column to index</param>
    /// <param name="indexDefinition">Use <see cref="Builders.TableIndex"/> to create the appropriate index definition.</param>
    public Task CreateIndexAsync<TColumn>(string indexName, Expression<Func<T, TColumn>> column, TableIndexDefinition indexDefinition)
    {
        return CreateIndexAsync(indexName, column.GetMemberNameTree(), indexDefinition, null);
    }

    /// <inheritdoc cref="CreateIndexAsync(string, string)"/>
    /// <param name="indexName">The index name</param>
    /// <param name="columnName">The name of the column to index</param>
    /// <param name="indexDefinition">Use <see cref="Builders.TableIndex"/> to create the appropriate index definition.</param>
    public Task CreateIndexAsync(string indexName, string columnName, TableIndexDefinition indexDefinition)
    {
        return CreateIndexAsync(indexName, columnName, indexDefinition, null);
    }

    /// <inheritdoc cref="CreateIndexAsync{TColumn}(string, Expression{Func{T, TColumn}}, CreateIndexCommandOptions)"/>
    /// <param name="indexName">The index name</param>
    /// <param name="column">The column to index</param>
    /// <param name="indexDefinition">Use <see cref="Builders.TableIndex"/> to create the appropriate index definition.</param>
    /// <param name="commandOptions"></param>
    public Task CreateIndexAsync<TColumn>(string indexName, Expression<Func<T, TColumn>> column, TableIndexDefinition indexDefinition, CreateIndexCommandOptions commandOptions)
    {
        return CreateIndexAsync(indexName, column.GetMemberNameTree(), indexDefinition, commandOptions);
    }

    /// <inheritdoc cref="CreateIndexAsync(string, string, CreateIndexCommandOptions)"/>
    /// <param name="indexName">The index name</param>
    /// <param name="columnName">The name of the column to index</param>
    /// <param name="indexDefinition">Use <see cref="Builders.TableIndex"/> to create the appropriate index definition.</param>
    /// <param name="commandOptions"></param>
    public Task CreateIndexAsync(string indexName, string columnName, TableIndexDefinition indexDefinition, CreateIndexCommandOptions commandOptions)
    {
        if (indexDefinition == null)
        {
            indexDefinition = new TableIndexDefinition();
        }
        return CreateGenericIndexAsync(indexName, columnName, indexDefinition, commandOptions, true);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateVectorIndexAsync(string, string)"/>
    /// </summary>
    /// <inheritdoc cref="CreateVectorIndexAsync(string, string)"/>
    public void CreateVectorIndex<TColumn>(string indexName, Expression<Func<T, TColumn>> column)
    {
        CreateVectorIndex(indexName, column.GetMemberNameTree(), null, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateVectorIndexAsync{TColumn}(string, Expression{Func{T, TColumn}})"/>
    /// </summary>
    /// <inheritdoc cref="CreateVectorIndexAsync{TColumn}(string, Expression{Func{T, TColumn}})"/>
    public void CreateVectorIndex(string indexName, string columnName)
    {
        CreateVectorIndex(indexName, columnName, null, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateVectorIndexAsync{TColumn}(string, Expression{Func{T, TColumn}}, CreateIndexCommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateVectorIndexAsync{TColumn}(string, Expression{Func{T, TColumn}}, CreateIndexCommandOptions)"/>
    public void CreateVectorIndex<TColumn>(string indexName, Expression<Func<T, TColumn>> column, CreateIndexCommandOptions commandOptions)
    {
        CreateVectorIndex(indexName, column.GetMemberNameTree(), null, commandOptions);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateVectorIndexAsync(string, string, CreateIndexCommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateVectorIndexAsync(string, string, CreateIndexCommandOptions)"/>
    public void CreateVectorIndex(string indexName, string columnName, CreateIndexCommandOptions commandOptions)
    {
        CreateVectorIndex(indexName, columnName, null, commandOptions);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateVectorIndexAsync{TColumn}(string, Expression{Func{T, TColumn}}, TableVectorIndexDefinition)"/>
    /// </summary>
    /// <inheritdoc cref="CreateVectorIndexAsync{TColumn}(string, Expression{Func{T, TColumn}}, TableVectorIndexDefinition)"/>
    public void CreateVectorIndex<TColumn>(string indexName, Expression<Func<T, TColumn>> column, TableVectorIndexDefinition indexDefinition)
    {
        CreateVectorIndex(indexName, column.GetMemberNameTree(), indexDefinition, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateVectorIndexAsync(string, string, TableVectorIndexDefinition)"/>
    /// </summary>
    /// <inheritdoc cref="CreateVectorIndexAsync(string, string, TableVectorIndexDefinition)"/>
    public void CreateVectorIndex(string indexName, string columnName, TableVectorIndexDefinition indexDefinition)
    {
        CreateVectorIndex(indexName, columnName, indexDefinition, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateVectorIndexAsync{TColumn}(string, Expression{Func{T, TColumn}}, TableVectorIndexDefinition, CreateIndexCommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateVectorIndexAsync{TColumn}(string, Expression{Func{T, TColumn}}, TableVectorIndexDefinition, CreateIndexCommandOptions)"/>
    public void CreateVectorIndex<TColumn>(string indexName, Expression<Func<T, TColumn>> column, TableVectorIndexDefinition indexDefinition, CreateIndexCommandOptions commandOptions)
    {
        CreateVectorIndex(indexName, column.GetMemberNameTree(), indexDefinition, commandOptions);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateVectorIndexAsync(string, string, TableVectorIndexDefinition, CreateIndexCommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateVectorIndexAsync(string, string, TableVectorIndexDefinition, CreateIndexCommandOptions)"/>
    public void CreateVectorIndex(string indexName, string columnName, TableVectorIndexDefinition indexDefinition, CreateIndexCommandOptions commandOptions)
    {
        if (indexDefinition == null)
        {
            indexDefinition = new TableVectorIndexDefinition();
        }
        CreateGenericIndexAsync(indexName, columnName, indexDefinition, commandOptions, false).ResultSync();
    }

    /// <summary>
    /// Create a vector index on the specified column
    /// </summary>
    /// <typeparam name="TColumn">The type of the column to index</typeparam>
    /// <param name="indexName">The index name</param>
    /// <param name="column">The vector column to index</param>
    /// <returns></returns>
    public Task CreateVectorIndexAsync<TColumn>(string indexName, Expression<Func<T, TColumn>> column)
    {
        return CreateVectorIndexAsync(indexName, column.GetMemberNameTree(), null, null);
    }

    /// <summary>
    /// Create a vector index on the specified column
    /// </summary>
    /// <param name="indexName">The index name</param>
    /// <param name="columnName">The name of the vector column to index</param>
    /// <returns></returns>
    public Task CreateVectorIndexAsync(string indexName, string columnName)
    {
        return CreateVectorIndexAsync(indexName, columnName, null, null);
    }

    /// <inheritdoc cref="CreateVectorIndexAsync{TColumn}(string, Expression{Func{T, TColumn}})"/>
    /// <param name="indexName">The index name</param>
    /// <param name="column">The vector column to index</param>
    /// <param name="commandOptions"></param>
    public Task CreateVectorIndexAsync<TColumn>(string indexName, Expression<Func<T, TColumn>> column, CreateIndexCommandOptions commandOptions)
    {
        return CreateVectorIndexAsync(indexName, column.GetMemberNameTree(), null, commandOptions);
    }

    /// <inheritdoc cref="CreateVectorIndexAsync(string, string)"/>
    /// <param name="indexName">The index name</param>
    /// <param name="columnName">The name of the vector column to index</param>
    /// <param name="commandOptions"></param>
    public Task CreateVectorIndexAsync(string indexName, string columnName, CreateIndexCommandOptions commandOptions)
    {
        return CreateVectorIndexAsync(indexName, columnName, null, commandOptions);
    }

    /// <inheritdoc cref="CreateVectorIndexAsync{TColumn}(string, Expression{Func{T, TColumn}})"/>
    /// <param name="indexName">The index name</param>
    /// <param name="column">The vector column to index</param>
    /// <param name="indexDefinition">Use <see cref="Builders.TableIndex"/> to create the appropriate index definition.</param>
    public Task CreateVectorIndexAsync<TColumn>(string indexName, Expression<Func<T, TColumn>> column, TableVectorIndexDefinition indexDefinition)
    {
        return CreateVectorIndexAsync(indexName, column.GetMemberNameTree(), indexDefinition, null);
    }

    /// <inheritdoc cref="CreateVectorIndexAsync(string, string)"/>
    /// <param name="indexName">The index name</param>
    /// <param name="columnName">The name of the vector column to index</param>
    /// <param name="indexDefinition">Use <see cref="Builders.TableIndex"/> to create the appropriate index definition.</param>
    public Task CreateVectorIndexAsync(string indexName, string columnName, TableVectorIndexDefinition indexDefinition)
    {
        return CreateVectorIndexAsync(indexName, columnName, indexDefinition, null);
    }

    /// <inheritdoc cref="CreateVectorIndexAsync{TColumn}(string, Expression{Func{T, TColumn}}, CreateIndexCommandOptions)"/>
    /// <param name="indexName">The index name</param>
    /// <param name="column">The vector column to index</param>
    /// <param name="indexDefinition">Use <see cref="Builders.TableIndex"/> to create the appropriate index definition.</param>
    /// <param name="commandOptions"></param>
    public Task CreateVectorIndexAsync<TColumn>(string indexName, Expression<Func<T, TColumn>> column, TableVectorIndexDefinition indexDefinition, CreateIndexCommandOptions commandOptions)
    {
        return CreateVectorIndexAsync(indexName, column.GetMemberNameTree(), indexDefinition, commandOptions);
    }

    /// <inheritdoc cref="CreateVectorIndexAsync(string, string, CreateIndexCommandOptions)"/>
    /// <param name="indexName">The index name</param>
    /// <param name="columnName">The name of the vector column to index</param>
    /// <param name="indexDefinition">Use <see cref="Builders.TableIndex"/> to create the appropriate index definition.</param>
    /// <param name="commandOptions"></param>
    public Task CreateVectorIndexAsync(string indexName, string columnName, TableVectorIndexDefinition indexDefinition, CreateIndexCommandOptions commandOptions)
    {
        if (indexDefinition == null)
        {
            indexDefinition = new TableVectorIndexDefinition();
        }
        return CreateGenericIndexAsync(indexName, columnName, indexDefinition, commandOptions, true);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateTextIndexAsync(string, string)"/>
    /// </summary>
    /// <inheritdoc cref="CreateTextIndexAsync(string, string)"/>
    public void CreateTextIndex<TColumn>(string indexName, Expression<Func<T, TColumn>> column)
    {
        CreateTextIndex(indexName, column.GetMemberNameTree(), null, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateTextIndexAsync{TColumn}(string, Expression{Func{T, TColumn}})"/>
    /// </summary>
    /// <inheritdoc cref="CreateTextIndexAsync{TColumn}(string, Expression{Func{T, TColumn}})"/>
    public void CreateTextIndex(string indexName, string columnName)
    {
        CreateTextIndex(indexName, columnName, null, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateTextIndexAsync{TColumn}(string, Expression{Func{T, TColumn}}, CreateIndexCommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateTextIndexAsync{TColumn}(string, Expression{Func{T, TColumn}}, CreateIndexCommandOptions)"/>
    public void CreateTextIndex<TColumn>(string indexName, Expression<Func<T, TColumn>> column, CreateIndexCommandOptions commandOptions)
    {
        CreateTextIndex(indexName, column.GetMemberNameTree(), null, commandOptions);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateTextIndexAsync(string, string, CreateIndexCommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateTextIndexAsync(string, string, CreateIndexCommandOptions)"/>
    public void CreateTextIndex(string indexName, string columnName, CreateIndexCommandOptions commandOptions)
    {
        CreateTextIndex(indexName, columnName, null, commandOptions);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateTextIndexAsync{TColumn}(string, Expression{Func{T, TColumn}}, TableTextIndexDefinition)"/>
    /// </summary>
    /// <inheritdoc cref="CreateTextIndexAsync{TColumn}(string, Expression{Func{T, TColumn}}, TableTextIndexDefinition)"/>
    public void CreateTextIndex<TColumn>(string indexName, Expression<Func<T, TColumn>> column, TableTextIndexDefinition indexDefinition)
    {
        CreateTextIndex(indexName, column.GetMemberNameTree(), indexDefinition, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateTextIndexAsync(string, string, TableTextIndexDefinition)"/>
    /// </summary>
    /// <inheritdoc cref="CreateTextIndexAsync(string, string, TableTextIndexDefinition)"/>
    public void CreateTextIndex(string indexName, string columnName, TableTextIndexDefinition indexDefinition)
    {
        CreateTextIndex(indexName, columnName, indexDefinition, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateTextIndexAsync{TColumn}(string, Expression{Func{T, TColumn}}, TableTextIndexDefinition, CreateIndexCommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateTextIndexAsync{TColumn}(string, Expression{Func{T, TColumn}}, TableTextIndexDefinition, CreateIndexCommandOptions)"/>
    public void CreateTextIndex<TColumn>(string indexName, Expression<Func<T, TColumn>> column, TableTextIndexDefinition indexDefinition, CreateIndexCommandOptions commandOptions)
    {
        CreateTextIndex(indexName, column.GetMemberNameTree(), indexDefinition, commandOptions);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateTextIndexAsync(string, string, TableTextIndexDefinition, CreateIndexCommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateTextIndexAsync(string, string, TableTextIndexDefinition, CreateIndexCommandOptions)"/>
    public void CreateTextIndex(string indexName, string columnName, TableTextIndexDefinition indexDefinition, CreateIndexCommandOptions commandOptions)
    {
        if (indexDefinition == null)
        {
            indexDefinition = new TableTextIndexDefinition();
        }
        CreateGenericIndexAsync(indexName, columnName, indexDefinition, commandOptions, false).ResultSync();
    }

    /// <summary>
    /// Create a text index on the specified column
    /// </summary>
    /// <typeparam name="TColumn">The type of the column to index</typeparam>
    /// <param name="indexName">The index name</param>
    /// <param name="column">The text column to index</param>
    /// <returns></returns>
    public Task CreateTextIndexAsync<TColumn>(string indexName, Expression<Func<T, TColumn>> column)
    {
        return CreateTextIndexAsync(indexName, column.GetMemberNameTree(), null, null);
    }

    /// <summary>
    /// Create a text index on the specified column
    /// </summary>
    /// <param name="indexName">The index name</param>
    /// <param name="columnName">The name of the text column to index</param>
    /// <returns></returns>
    public Task CreateTextIndexAsync(string indexName, string columnName)
    {
        return CreateTextIndexAsync(indexName, columnName, null, null);
    }

    /// <inheritdoc cref="CreateTextIndexAsync{TColumn}(string, Expression{Func{T, TColumn}})"/>
    /// <param name="indexName">The index name</param>
    /// <param name="column">The text column to index</param>
    /// <param name="commandOptions"></param>
    public Task CreateTextIndexAsync<TColumn>(string indexName, Expression<Func<T, TColumn>> column, CreateIndexCommandOptions commandOptions)
    {
        return CreateTextIndexAsync(indexName, column.GetMemberNameTree(), null, commandOptions);
    }

    /// <inheritdoc cref="CreateTextIndexAsync(string, string)"/>
    /// <param name="indexName">The index name</param>
    /// <param name="columnName">The name of the text column to index</param>
    /// <param name="commandOptions"></param>
    public Task CreateTextIndexAsync(string indexName, string columnName, CreateIndexCommandOptions commandOptions)
    {
        return CreateTextIndexAsync(indexName, columnName, null, commandOptions);
    }

    /// <inheritdoc cref="CreateTextIndexAsync{TColumn}(string, Expression{Func{T, TColumn}})"/>
    /// <param name="indexName">The index name</param>
    /// <param name="column">The text column to index</param>
    /// <param name="indexDefinition">Use <see cref="Builders.TableIndex"/> to create the appropriate index definition.</param>
    public Task CreateTextIndexAsync<TColumn>(string indexName, Expression<Func<T, TColumn>> column, TableTextIndexDefinition indexDefinition)
    {
        return CreateTextIndexAsync(indexName, column.GetMemberNameTree(), indexDefinition, null);
    }

    /// <inheritdoc cref="CreateTextIndexAsync(string, string)"/>
    /// <param name="indexName">The index name</param>
    /// <param name="columnName">The name of the text column to index</param>
    /// <param name="indexDefinition">Use <see cref="Builders.TableIndex"/> to create the appropriate index definition.</param>
    public Task CreateTextIndexAsync(string indexName, string columnName, TableTextIndexDefinition indexDefinition)
    {
        return CreateTextIndexAsync(indexName, columnName, indexDefinition, null);
    }

    /// <inheritdoc cref="CreateTextIndexAsync{TColumn}(string, Expression{Func{T, TColumn}}, CreateIndexCommandOptions)"/>
    /// <param name="indexName">The index name</param>
    /// <param name="column">The text column to index</param>
    /// <param name="indexDefinition">Use <see cref="Builders.TableIndex"/> to create the appropriate index definition.</param>
    /// <param name="commandOptions"></param>
    public Task CreateTextIndexAsync<TColumn>(string indexName, Expression<Func<T, TColumn>> column, TableTextIndexDefinition indexDefinition, CreateIndexCommandOptions commandOptions)
    {
        return CreateTextIndexAsync(indexName, column.GetMemberNameTree(), indexDefinition, commandOptions);
    }

    /// <inheritdoc cref="CreateTextIndexAsync(string, string, CreateIndexCommandOptions)"/>
    /// <param name="indexName">The index name</param>
    /// <param name="columnName">The name of the text column to index</param>
    /// <param name="indexDefinition">Use <see cref="Builders.TableIndex"/> to create the appropriate index definition.</param>
    /// <param name="commandOptions"></param>
    public Task CreateTextIndexAsync(string indexName, string columnName, TableTextIndexDefinition indexDefinition, CreateIndexCommandOptions commandOptions)
    {
        if (indexDefinition == null)
        {
            indexDefinition = new TableTextIndexDefinition();
        }
        return CreateGenericIndexAsync(indexName, columnName, indexDefinition, commandOptions, true);
    }

    private async Task CreateGenericIndexAsync(string indexName, string columnName, TableBaseIndexDefinition indexDefinition, CreateIndexCommandOptions commandOptions, bool runSynchronously)
    {
        indexDefinition.ColumnName = columnName;

        var index = new TableIndex
        {
            IndexName = indexName,
            Definition = indexDefinition
        };
        if (commandOptions != null)
        {
            index.Options = new TableIndexCreationOptions { IfNotExists = commandOptions.IfNotExists };
        }
        var command = CreateCommand(indexDefinition.IndexCreationCommandName).WithPayload(index).AddCommandOptions(commandOptions);

        await command.RunAsyncReturnStatus<Dictionary<string, int>>(runSynchronously).ConfigureAwait(false);
    }

    /// <summary>
    /// Synchronous version of <see cref="InsertOneAsync(T, TableInsertOneOptions)"/>
    /// </summary>
    /// <inheritdoc cref="InsertOneAsync(T, TableInsertOneOptions)"/>
    public TableInsertOneResult InsertOne(T row, TableInsertOneOptions options = null)
    {
        return InsertOneAsync(row, options, true).ResultSync();
    }

    /// <summary>
    /// Insert a row into the table.
    /// </summary>
    /// <param name="row">The row to insert.</param>
    /// <param name="options">Options for the insert operation.</param>
    public Task<TableInsertOneResult> InsertOneAsync(T row, TableInsertOneOptions options = null)
    {
        return InsertOneAsync(row, options, false);
    }

    private async Task<TableInsertOneResult> InsertOneAsync(T row, TableInsertOneOptions options, bool runSynchronously)
    {
        Guard.NotNull(row, nameof(row));

        options = options?.ShallowCopy() ?? new();
        SetRowSerializationOptions<T>(options, true);
        
        var response = await CreateCommand("insertOne")
            .WithPayload(options.ToPayload(row))
            .AddCommandOptions(options)
            .RunAsyncReturnStatus<TableInsertManyResult>(runSynchronously)
            .ConfigureAwait(false);
        
        return new TableInsertOneResult
        {
            InsertedIdTuple = (response.Result.InsertedIdTuples.Count > 0) 
                ? response.Result.InsertedIdTuples[0]
                : null,
        };
    }

    /// <summary>
    /// Synchronous version of <see cref="InsertManyAsync(List{T}, TableInsertManyOptions)"/>
    /// </summary>
    /// <inheritdoc cref="InsertManyAsync(List{T}, TableInsertManyOptions)"/>
    public TableInsertManyResult InsertMany(List<T> rows, TableInsertManyOptions options = null)
    {
        return InsertManyAsync(rows, options, runSynchronously: true).ResultSync();
    }

    /// <summary>
    /// Insert multiple rows into the table.
    /// </summary>
    /// <param name="rows">The list of rows to insert.</param>
    /// <param name="options">Allows specifying the insertion chunk size, ordered/unordered mode, concurrency, as well as other generic command-execution options.</param>
    public Task<TableInsertManyResult> InsertManyAsync(List<T> rows, TableInsertManyOptions options = null)
    {
        return InsertManyAsync(rows, options, runSynchronously: false);
    }

    private async Task<TableInsertManyResult> InsertManyAsync(List<T> rows, TableInsertManyOptions options, bool runSynchronously)
    {
        Guard.NotNull(rows, nameof(rows));

        options = options?.ShallowClone() ?? new();
        SetRowSerializationOptions<T>(options, true);
        
        if (options.Concurrency > 1 && options.Ordered)
        {
            throw new ArgumentException("Cannot run ordered insert_many concurrently.");
        }

        var result = new TableInsertManyResult();
        var tasks = new List<Task>();
        var semaphore = new SemaphoreSlim(options.Concurrency);
        var (timeout, cts) = BulkOperationHelper.InitTimeout(GetOptionsTree(), options);

        using (cts)
        {
            var bulkOperationTimeoutToken = cts.Token;
            try
            {
                var chunks = rows.CreateBatch(options.ChunkSize);

                foreach (var chunk in chunks)
                {
                    tasks.Add(Task.Run(async () =>
                    {
                        await semaphore.WaitAsync(bulkOperationTimeoutToken);
                        try
                        {
                            var runResult = await RunInsertManyAsync(chunk, options, runSynchronously).ConfigureAwait(false);
                            lock (result.InsertedIdTuples)
                            {
                                result.InsertedIdTuples.AddRange(runResult.InsertedIdTuples);
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
                var innerException = new TimeoutException($"InsertMany operation timed out after {timeout.TotalSeconds} seconds. Consider increasing the timeout using the TableInsertManyOptions.TimeoutOptions.BulkOperationTimeout parameter.");
                throw new BulkOperationException<TableInsertManyResult>(innerException, result);
            }
            catch (Exception ex)
            {
                throw new BulkOperationException<TableInsertManyResult>(ex, result);
            }

            return result;
        }
    }

    private async Task<TableInsertManyResult> RunInsertManyAsync(IEnumerable<T> rows, TableInsertManyOptions options, bool runSynchronously)
    {
        var response = await CreateCommand("insertMany")
            .WithPayload(options.ToPayload(rows))
            .AddCommandOptions(options)
            .RunAsyncReturnStatus<TableInsertManyResult>(runSynchronously)
            .ConfigureAwait(false);

        return response.Result;
    }
    
    /// <inheritdoc cref="FindOneAsync(TableFindOneOptions{T})"/>
    /// Synchronous version of <see cref="FindOneAsync(TableFindOneOptions{T})"/>
    public T FindOne(TableFindOneOptions<T> options = null)
    {
        return FindOne<T>(null, options);
    }

    /// <inheritdoc cref="FindOneAsync(TableFilter{T}, TableFindOneOptions{T})"/>
    /// Synchronous version of <see cref="FindOneAsync(TableFilter{T}, TableFindOneOptions{T})"/>
    public T FindOne(TableFilter<T> filter, TableFindOneOptions<T> options = null)
    {
        return FindOne<T>(filter, options);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}(TableFilter{T}, TableFindOneOptions{T})"/>
    /// Synchronous version of <see cref="FindOneAsync{TResult}(TableFilter{T}, TableFindOneOptions{T})"/>
    public TResult FindOne<TResult>(TableFindOneOptions<T> options = null) where TResult : class
    {
        return FindOne<TResult>(null, options);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}(TableFilter{T}, TableFindOneOptions{T})"/>
    /// Synchronous version of <see cref="FindOneAsync{TResult}(TableFilter{T}, TableFindOneOptions{T})"/>
    public TResult FindOne<TResult>(TableFilter<T> filter, TableFindOneOptions<T> options = null) where TResult : class
    {
        return FindOneAsync<TResult>(filter, options, runSynchronously: true).ResultSync();
    }

    /// <summary>
    /// Find a single row in the table using the specified find options.
    /// </summary>
    /// <param name="options">Specify Sort options for the find operation.</param>
    /// <returns></returns>
    public Task<T> FindOneAsync(TableFindOneOptions<T> options = null)
    {
        return FindOneAsync<T>(null, options);
    }

    /// <inheritdoc cref="FindOneAsync(TableFindOneOptions{T})"/>
    /// <param name="filter"></param>
    /// <param name="options">Specify Sort options for the find operation.</param> = null
    public Task<T> FindOneAsync(TableFilter<T> filter, TableFindOneOptions<T> options = null)
    {
        return FindOneAsync<T>(filter, options);
    }

    /// <summary>
    /// Find a single row in the table, specifying a different result row class type <typeparamref name="TResult"/>
    ///  (useful when you want to project only certain fields).
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    /// <returns></returns>
    public Task<TResult> FindOneAsync<TResult>(TableFindOneOptions<T> options = null) where TResult : class
    {
        return FindOneAsync<TResult>(null, options);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}(TableFindOneOptions{T})"/>
    /// <param name="filter"></param>
    /// <param name="options">Specify Sort options for the find operation.</param>
    public Task<TResult> FindOneAsync<TResult>(TableFilter<T> filter, TableFindOneOptions<T> options = null) where TResult : class
    {
        return FindOneAsync<TResult>(filter, options, runSynchronously: false);
    }

    private async Task<TResult> FindOneAsync<TResult>(TableFilter<T> filter, TableFindOneOptions<T> options, bool runSynchronously) where TResult : class
    {
        options = options?.ShallowClone() ?? new();
        SetRowSerializationOptions<TResult>(options, false);
        
        var response = await CreateCommand("findOne")
            .WithPayload(options.ToPayload(filter))
            .AddCommandOptions(options)
            .RunAsyncReturnData<DocumentResult<TResult>, TableFindStatusResult>(runSynchronously)
            .ConfigureAwait(false);
        
        if (typeof(Row).IsAssignableFrom(typeof(TResult)))
        {
            if (response is { Data.Document: not null })
            {
                ProcessUntypedRow(response.Data.Document as Row, response.Status.ProjectionSchema);
            }
        }
        return response.Data.Document;
    }
    
    /// <summary>
    /// Find rows in the table.
    /// 
    /// The Find() methods return a <see cref="Core.Enumeration.TableFindCursor{T,TResult}"/> object that can be used to further structure the query
    /// by adding Sort, Projection, Skip, Limit, etc. to affect the final results.
    /// 
    /// The <see cref="Core.Enumeration.TableFindCursor{T,TResult}"/> object can be directly enumerated both synchronously and asynchronously.
    /// </summary>
    /// <returns></returns>
    /// <example>
    /// Synchronous Enumeration:
    /// <code>
    /// var cursor = table.Find();
    /// foreach (var row in cursor)
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
    /// Timeouts passed in the <see cref="TableFindManyOptions{T}"/> (<see cref="TimeoutOptions.ConnectionTimeout"/>
    /// and <see cref="TimeoutOptions.RequestTimeout"/>) will be used for each batched request to the API,
    /// however <c>BulkOperationCancellationToken</c> settings are ignored due to the nature of Enumeration.
    /// If you need to enforce a timeout for the entire operation, you can pass a <see cref="CancellationToken"/> to GetAsyncEnumerator.
    /// </remarks>
    public TableFindCursor<T> Find(TableFindManyOptions<T> options = null)
    {
        return Find(null, options);
    }

    /// <inheritdoc cref="Find(TableFindManyOptions{T})"/>
    /// <param name="filter"></param>
    /// <param name="options"></param>
    public TableFindCursor<T> Find(TableFilter<T> filter, TableFindManyOptions<T> options = null)
    {
        return new(filter, options, RunFindManyAsync);
    }
    
    /// <inheritdoc cref="Find(TableFindManyOptions{T})"/>
    /// <remarks>
    /// The Find alternatives that accept a TResult type parameter allow for deserializing the row as a different type
    /// (most commonly used when using projection to return a subset of fields)
    /// </remarks>
    public TableFindCursor<T, TResult> Find<TResult>(TableFindManyOptions<T> options = null) where TResult : class
    {
        return Find<TResult>(null, options);
    }

    /// <inheritdoc cref="Find(TableFilter{T}, TableFindManyOptions{T})"/>
    /// <remarks>
    /// The Find alternatives that accept a TResult type parameter allow for deserializing the row as a different type
    /// (most commonly used when using projection to return a subset of fields)
    /// </remarks>
    public TableFindCursor<T, TResult> Find<TResult>(TableFilter<T> filter, TableFindManyOptions<T> options = null) where TResult : class
    {
        return new(filter, options, RunFindManyAsync);
    }

    private async Task<FindPage<TResult>> RunFindManyAsync<TResult>(TableFindCursor<T, TResult> cursor, string nextPageState, bool runSynchronously) where TResult : class
    {
        SetRowSerializationOptions<TResult>(cursor.FindOptions, false);

        var response = await CreateCommand("find")
            .WithPayload(cursor.FindOptions.ToPayload(cursor.CurrentFilter, nextPageState))
            .AddCommandOptions(cursor.FindOptions)
            .RunAsyncReturnData<APIFindResult<TResult>, TableFindStatusResult>(runSynchronously)
            .ConfigureAwait(false);

        if (typeof(Row).IsAssignableFrom(typeof(TResult)))
        {
            var columnsInResult = response.Status.ProjectionSchema;
            if (response.Data is { Items: not null })
            {
                foreach (var row in response.Data.Items)
                {
                    ProcessUntypedRow(row as Row, columnsInResult);
                }
            }
        }
        
        return new FindPage<TResult>(
            response.Data.NextPageState,
            response.Data.Items,
            response.Status?.SortVector
        );
    }

    private void ProcessUntypedRow(Row row, Dictionary<string, SchemaColumn> projectionSchema)
    {
        foreach (var column in projectionSchema)
        {
            var columnType = column.Value.Type;
            if (!row.ContainsKey(column.Key))
            {
                object value = null;
                switch (columnType)
                {
                    case "map":
                        value = new Dictionary<object, object>();
                        break;
                    case "set":
                        value = new HashSet<object>();
                        break;
                    case "list":
                        value = new List<object>();
                        break;
                }
                row[column.Key] = value;
            }
            else
            {
                row[column.Key] = ConvertRowValue(row[column.Key], columnType);
            }
        }
    }

    private static object ConvertRowValue(object value, string columnType)
    {
        if (value is not JsonElement element)
            return value;

        if (element.ValueKind == JsonValueKind.Null)
            return null;

        switch (columnType)
        {
            case "timeuuid":
                if (element.ValueKind == JsonValueKind.String && TimeUuid.TryParse(element.GetString(), out var t))
                    return t;
                break;
            case "uuid":
                if (element.ValueKind == JsonValueKind.String && Guid.TryParse(element.GetString(), out var g))
                    return g;
                break;
#if NET6_0_OR_GREATER
            case "date":
                if (element.ValueKind == JsonValueKind.String && DateOnly.TryParse(element.GetString(), CultureInfo.InvariantCulture, out var d))
                    return d;
                break;
            case "time":
                if (element.ValueKind == JsonValueKind.String && TimeOnly.TryParse(element.GetString(), CultureInfo.InvariantCulture, out var to))
                    return to;
                break;
#endif
            case "timestamp":
                if (element.ValueKind == JsonValueKind.String && DateTime.TryParse(element.GetString(), null, DateTimeStyles.RoundtripKind, out var dt))
                    return dt;
                break;
            case "duration":
                if (element.ValueKind == JsonValueKind.String)
                    return Duration.Parse(element.GetString());
                break;
            case "inet":
                if (element.ValueKind == JsonValueKind.String && IPAddress.TryParse(element.GetString(), out var ip))
                    return ip;
                break;
            case "blob":
                if (element.ValueKind == JsonValueKind.Object && element.TryGetProperty("$binary", out var binaryEl))
                    return Convert.FromBase64String(binaryEl.GetString());
                break;
            case "int":
                if (element.ValueKind == JsonValueKind.Number) return element.GetInt32();
                break;
            case "smallint":
                if (element.ValueKind == JsonValueKind.Number) return element.GetInt16();
                break;
            case "tinyint":
                if (element.ValueKind == JsonValueKind.Number) return element.GetSByte();
                break;
            case "bigint":
            case "counter":
                if (element.ValueKind == JsonValueKind.Number) return element.GetInt64();
                break;
            case "float":
                if (element.ValueKind == JsonValueKind.Number) return element.GetSingle();
                break;
            case "double":
                if (element.ValueKind == JsonValueKind.Number) return element.GetDouble();
                break;
            case "decimal":
            case "varint":
                if (element.ValueKind == JsonValueKind.Number) return element.GetDecimal();
                break;
            case "boolean":
                if (element.ValueKind == JsonValueKind.True || element.ValueKind == JsonValueKind.False)
                    return element.GetBoolean();
                break;
        }

        // Generic fallback for unrecognized types or types handled above that fell through
        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.TryGetInt64(out var l) ? l : (object)element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => value
        };
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
        return _database.DropTableAsync(TableName);
    }

    private static void SetRowSerializationOptions<TResult>(CommandOptions options, bool isInsert) where TResult : class
    {
        options.SerializeGuidAsDollarUuid = false;
        options.SerializeDateAsDollarDate = false;
        
        if (typeof(TResult) == typeof(Row))
        {
            if (isInsert && typeof(T) == typeof(Row))
            {
                options.InputConverter = new SimpleDictionaryConverter();
            }
            return;
        }
        
        if (isInsert)
        {
            options.InputConverter = new RowConverter<T>();
        }
        else
        {
            options.OutputConverter = new RowConverter<TResult>();
        }
    }

    /// <summary>
    /// Synchronous version of <see cref="UpdateOneAsync(TableFilter{T}, UpdateBuilder{T}, TableUpdateOneOptions)"/>
    /// </summary>
    /// <inheritdoc cref="UpdateOneAsync(TableFilter{T}, UpdateBuilder{T}, TableUpdateOneOptions)"/>
    public void UpdateOne(TableFilter<T> filter, UpdateBuilder<T> update, TableUpdateOneOptions options = null)
    {
        UpdateOneAsync(filter, update, options, runSynchronously: true).ResultSync();
    }

    /// <summary>
    /// Update a single row in the table using the provided filter and update builder.
    /// </summary>
    /// <param name="filter">The filter to match rows.</param>
    /// <param name="update">The update operations to apply.</param>
    /// <param name="options">Options for the update operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public Task UpdateOneAsync(TableFilter<T> filter, UpdateBuilder<T> update, TableUpdateOneOptions options = null)
    {
        return UpdateOneAsync(filter, update, options, runSynchronously: false);
    }

    private async Task UpdateOneAsync(TableFilter<T> filter, UpdateBuilder<T> update, TableUpdateOneOptions options, bool runSynchronously)
    {
        Guard.NotNull(filter, nameof(filter));
        Guard.NotNull(update, nameof(update));
        
        options ??= new();
        
        await CreateCommand("updateOne")
            .WithPayload(options.ToPayload(filter, update))
            .AddCommandOptions(options)
            .RunAsyncReturnStatus<UpdateResult>(runSynchronously)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Synchronous version of <see cref="DeleteOneAsync(TableFilter{T}, TableDeleteOneOptions)"/>
    /// </summary>
    /// <inheritdoc cref="DeleteOneAsync(TableFilter{T}, TableDeleteOneOptions)"/>
    public void DeleteOne(TableFilter<T> filter, TableDeleteOneOptions options = null)
    {
        DeleteOneAsync(filter, options, runSynchronously: true).ResultSync();
    }

    /// <summary>
    /// Delete a row from the table.
    /// </summary>
    /// <param name="filter">The filter to match rows.</param>
    /// <param name="options">Options for the delete operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public Task DeleteOneAsync(TableFilter<T> filter, TableDeleteOneOptions options = null)
    {
        return DeleteOneAsync(filter, options, runSynchronously: false);
    }

    private async Task DeleteOneAsync(TableFilter<T> filter, TableDeleteOneOptions options, bool runSynchronously)
    {
        Guard.NotNull(filter, nameof(filter));
        
        options ??= new();
        
        await CreateCommand("deleteOne")
            .WithPayload(options.ToPayload(filter))
            .AddCommandOptions(options)
            .RunAsyncReturnStatus<DeleteResult>(runSynchronously)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Synchronous version of <see cref="DeleteManyAsync(TableFilter{T}, TableDeleteManyOptions)"/>
    /// </summary>
    /// <inheritdoc cref="DeleteManyAsync(TableFilter{T}, TableDeleteManyOptions)"/>
    public void DeleteMany(TableFilter<T> filter, TableDeleteManyOptions options = null)
    {
        DeleteManyAsync(filter, options, runSynchronously: true).ResultSync();
    }

    /// <summary>
    /// Delete all rows matching the filter from the table.
    /// </summary>
    /// <param name="filter">The filter to match rows.</param>
    /// <param name="options">Options for the delete operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public Task DeleteManyAsync(TableFilter<T> filter, TableDeleteManyOptions options = null)
    {
        return DeleteManyAsync(filter, options, runSynchronously: false);
    }

    private async Task DeleteManyAsync(TableFilter<T> filter, TableDeleteManyOptions options, bool runSynchronously)
    {
        options ??= new();
        
        await CreateCommand("deleteMany")
            .WithPayload(options.ToPayload(filter))
            .AddCommandOptions(options)
            .RunAsyncReturnStatus<DeleteResult>(runSynchronously)
            .ConfigureAwait(false);
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

        return new Table<TRowAfterAlter>(TableName, _database, _commandOptions);

    }

    private List<CommandOptions> GetOptionsTree()
    {
        var optionsTree = _commandOptions == null ? _database.OptionsTree : _database.OptionsTree.Concat(new[] { _commandOptions });
        return optionsTree.ToList();
    }

    internal Command CreateCommand(string name)
    {
        var optionsTree = GetOptionsTree().ToArray();
        return new Command(name, _database.Client, optionsTree, new DatabaseCommandUrlBuilder(_database, TableName));
    }
}
