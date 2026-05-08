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

    //

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
    /// <param name="rows"></param>
    /// <param name="commandOptions"></param>
    public Task<TableInsertManyResult> InsertManyAsync(IEnumerable<T> rows, CommandOptions commandOptions)
    {
        return InsertManyAsync(rows, null, commandOptions, false);
    }

    /// <inheritdoc cref="InsertManyAsync(IEnumerable{T})"/>
    /// <param name="rows"></param>
    /// <param name="insertOptions"></param>
    public Task<TableInsertManyResult> InsertManyAsync(IEnumerable<T> rows, InsertManyOptions insertOptions)
    {
        return InsertManyAsync(rows, insertOptions, null, false);
    }

    /// <inheritdoc cref="InsertManyAsync(IEnumerable{T}, CommandOptions)"/>
    /// <param name="rows"></param>
    /// <param name="insertOptions"></param>
    /// <param name="commandOptions"></param>
    public Task<TableInsertManyResult> InsertManyAsync(IEnumerable<T> rows, InsertManyOptions insertOptions, CommandOptions commandOptions)
    {
        return InsertManyAsync(rows, insertOptions, commandOptions, false);
    }

    private async Task<TableInsertManyResult> InsertManyAsync(IEnumerable<T> rows, InsertManyOptions insertOptions, CommandOptions commandOptions, bool runSynchronously)
    {
        Guard.NotNullOrEmpty(rows, nameof(rows));

        if (insertOptions == null) insertOptions = new InsertManyOptions();
        if (insertOptions.Concurrency > 1 && insertOptions.Ordered)
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
                            var runResult = await RunInsertManyAsync(chunk, insertOptions.Ordered, commandOptions, runSynchronously).ConfigureAwait(false);
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

    private async Task<TableInsertManyResult> RunInsertManyAsync(IEnumerable<T> rows, bool insertOrdered, CommandOptions commandOptions, bool runSynchronously)
    {
        var payload = new
        {
            documents = rows,
            options = new
            {
                ordered = insertOrdered,
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
    /// <param name="row"></param>
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
        var response = await command.RunAsyncReturnStatus<TableInsertManyResult>(runSynchronously).ConfigureAwait(false);
        return new TableInsertOneResult
        {
            InsertedIdTuple = response.Result.InsertedIdTuples.Count > 0 ? response.Result.InsertedIdTuples[0] : null
        };
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
    /// Timeouts passed in the <see cref="CommandOptions"/> (<see cref="TimeoutOptions.ConnectionTimeout"/>
    /// and <see cref="TimeoutOptions.RequestTimeout"/>) will be used for each batched request to the API,
    /// however <c>BulkOperationCancellationToken</c> settings are ignored due to the nature of Enumeration.
    /// If you need to enforce a timeout for the entire operation, you can pass a <see cref="CancellationToken"/> to GetAsyncEnumerator.
    /// </remarks>
    public TableFindCursor<T> Find()
    {
        return Find(null, null);
    }

    /// <inheritdoc cref="Find()"/>
    /// <param name="filter">The filter(s) to apply to the query.</param>
    /// <returns></returns>
    /// <example>
    /// <code>
    /// var filterBuilder = Builders{BookRow}.Filter;
    /// var filter = filterBuilder.Gt(x => x.NumberOfPages, 430);
    /// var matchingBooks = table.Find(filter).ToList();
    /// await foreach (var bookRow in matchingBooks)
    /// {
    ///     //handle each row
    /// }
    /// </code>
    /// </example>
    public TableFindCursor<T> Find(TableFilter<T> filter)
    {
        return Find(filter, null);
    }

    /// <inheritdoc cref="Find()" path="/summary"/>
    /// <param name="findOptions"></param>
    public TableFindCursor<T> Find(TableFindManyOptions<T> findOptions)
    {
        return Find(null, findOptions);
    }

    /// <inheritdoc cref="Find(TableFilter{T})"/>
    /// <param name="filter"></param>
    /// <param name="findOptions"></param>
    public TableFindCursor<T> Find(TableFilter<T> filter, TableFindManyOptions<T> findOptions)
    {
        findOptions ??= new TableFindManyOptions<T>();
        var commandOptions = findOptions.commandOptions();
        return new(findOptions.WithFilterParam(filter), commandOptions, RunFindManyAsync);
    }

    /// <inheritdoc cref="Find()" path="/summary"/>
    /// <remarks>
    /// The Find alternatives that accept a TResult type parameter allow for deserializing the row as a different type
    /// (most commonly used when using projection to return a subset of fields)
    /// </remarks>
    public TableFindCursor<T, TResult> Find<TResult>() where TResult : class
    {
        return Find<TResult>(null, null);
    }

    /// <inheritdoc cref="Find(TableFilter{T})"/>
    /// <remarks>
    /// The Find alternatives that accept a TResult type parameter allow for deserializing the row as a different type
    /// (most commonly used when using projection to return a subset of fields)
    /// </remarks>
    public TableFindCursor<T, TResult> Find<TResult>(TableFilter<T> filter) where TResult : class
    {
        return Find<TResult>(filter, null);
    }

    /// <inheritdoc cref="Find(TableFindManyOptions{T})"/>
    /// <remarks>
    /// The Find alternatives that accept a TResult type parameter allow for deserializing the row as a different type
    /// (most commonly used when using projection to return a subset of fields)
    /// </remarks>
    public TableFindCursor<T, TResult> Find<TResult>(TableFindManyOptions<T> findOptions) where TResult : class
    {
        return Find<TResult>(null, findOptions);
    }

    /// <inheritdoc cref="Find(TableFilter{T}, TableFindManyOptions{T})"/>
    /// <remarks>
    /// The Find alternatives that accept a TResult type parameter allow for deserializing the row as a different type
    /// (most commonly used when using projection to return a subset of fields)
    /// </remarks>
    public TableFindCursor<T, TResult> Find<TResult>(TableFilter<T> filter, TableFindManyOptions<T> findOptions) where TResult : class
    {
        findOptions ??= new TableFindManyOptions<T>();
        var commandOptions = findOptions.commandOptions();
        return new(findOptions.WithFilterParam(filter), commandOptions, RunFindManyAsync);
    }

    internal async Task<FindPage<TResult>> RunFindManyAsync<TResult>(TableFindCursor<T, TResult> cursor, string nextPageState, bool runSynchronously) where TResult : class
    {
        var options = cursor.FindOptions.Clone();
        options.PageState = nextPageState;

        var payloadOptions = options.PayloadOptions();
        var commandOptions = SetRowSerializationOptions<TResult>(cursor.CommandOptions, false);
        var command = CreateCommand("find").WithPayload(payloadOptions).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnData<APIFindResult<TResult>, TableFindStatusResult>(runSynchronously).ConfigureAwait(false);
        
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

    /// <inheritdoc cref="FindOneAsync()"/>
    /// Synchronous version of <see cref="FindOneAsync()"/>
    public T FindOne()
    {
        return FindOne(null, null, null);
    }

    /// <inheritdoc cref="FindOneAsync(TableFilter{T})"/>
    /// Synchronous version of <see cref="FindOneAsync(TableFilter{T})"/>
    public T FindOne(TableFilter<T> filter)
    {
        return FindOne(filter, null, null);
    }

    /// <inheritdoc cref="FindOneAsync(TableFilter{T}, CommandOptions)"/>
    /// Synchronous version of <see cref="FindOneAsync(TableFilter{T}, CommandOptions)"/>
    public T FindOne(TableFilter<T> filter, CommandOptions commandOptions)
    {
        return FindOne(filter, null, commandOptions);
    }

    /// <inheritdoc cref="FindOneAsync(TableFindOneOptions{T})"/>
    /// Synchronous version of <see cref="FindOneAsync(TableFindOneOptions{T})"/>
    public T FindOne(TableFindOneOptions<T> findOptions)
    {
        return FindOne<T>(null, findOptions, null);
    }

    /// <inheritdoc cref="FindOneAsync(TableFilter{T}, TableFindOneOptions{T})"/>
    /// Synchronous version of <see cref="FindOneAsync(TableFilter{T}, TableFindOneOptions{T})"/>
    public T FindOne(TableFilter<T> filter, TableFindOneOptions<T> findOptions)
    {
        return FindOne<T>(filter, findOptions, null);
    }

    /// <inheritdoc cref="FindOneAsync(TableFilter{T}, TableFindOneOptions{T}, CommandOptions)"/>
    /// Synchronous version of <see cref="FindOneAsync(TableFilter{T}, TableFindOneOptions{T}, CommandOptions)"/> 
    public T FindOne(TableFilter<T> filter, TableFindOneOptions<T> findOptions, CommandOptions commandOptions)
    {
        return FindOne<T>(filter, findOptions, commandOptions);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}()"/>
    /// Synchronous version of <see cref="FindOneAsync{TResult}()"/>
    public TResult FindOne<TResult>() where TResult : class
    {
        return FindOne<TResult>(null, null, null);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}(TableFilter{T})"/>
    /// Synchronous version of <see cref="FindOneAsync{TResult}(TableFilter{T})"/>
    public TResult FindOne<TResult>(TableFilter<T> filter) where TResult : class
    {
        return FindOne<TResult>(filter, null, null);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}(TableFilter{T}, CommandOptions)"/>
    /// Synchronous version of <see cref="FindOneAsync{TResult}(TableFilter{T}, CommandOptions)"/>
    public TResult FindOne<TResult>(TableFilter<T> filter, CommandOptions commandOptions) where TResult : class
    {
        return FindOne<TResult>(filter, null, commandOptions);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}(TableFilter{T}, TableFindOneOptions{T})"/>
    /// Synchronous version of <see cref="FindOneAsync{TResult}(TableFilter{T}, TableFindOneOptions{T})"/>
    public TResult FindOne<TResult>(TableFilter<T> filter, TableFindOneOptions<T> findOptions) where TResult : class
    {
        return FindOne<TResult>(filter, findOptions, null);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}(TableFilter{T}, TableFindOneOptions{T}, CommandOptions)"/>
    /// Synchronous version of <see cref="FindOneAsync{TResult}(TableFilter{T}, TableFindOneOptions{T}, CommandOptions)"/>
    public TResult FindOne<TResult>(TableFilter<T> filter, TableFindOneOptions<T> findOptions, CommandOptions commandOptions) where TResult : class
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
    public Task<T> FindOneAsync(TableFilter<T> filter)
    {
        return FindOneAsync(filter, null, null);
    }

    /// <inheritdoc cref="FindOneAsync(TableFilter{T})"/>
    /// <param name="filter"></param>
    /// <param name="commandOptions"></param>
    public Task<T> FindOneAsync(TableFilter<T> filter, CommandOptions commandOptions)
    {
        return FindOneAsync(filter, null, commandOptions);
    }

    /// <summary>
    /// Find a single row in the table using the specified find options.
    /// </summary>
    /// <param name="findOptions">Specify Sort options for the find operation.</param>
    /// <returns></returns>
    public Task<T> FindOneAsync(TableFindOneOptions<T> findOptions)
    {
        return FindOneAsync<T>(null, findOptions, null);
    }

    /// <inheritdoc cref="FindOneAsync(TableFilter{T})"/>
    /// <param name="filter"></param>
    /// <param name="findOptions">Specify Sort options for the find operation.</param>
    public Task<T> FindOneAsync(TableFilter<T> filter, TableFindOneOptions<T> findOptions)
    {
        return FindOneAsync<T>(filter, findOptions, null);
    }

    /// <inheritdoc cref="FindOneAsync(TableFilter{T}, TableFindOneOptions{T})"/>
    /// <param name="filter"></param>
    /// <param name="findOptions"></param>
    /// <param name="commandOptions"></param>
    public Task<T> FindOneAsync(TableFilter<T> filter, TableFindOneOptions<T> findOptions, CommandOptions commandOptions)
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
    public Task<TResult> FindOneAsync<TResult>(TableFilter<T> filter) where TResult : class
    {
        return FindOneAsync<TResult>(filter, null, null);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}(TableFilter{T})"/>
    /// <param name="filter"></param>
    /// <param name="commandOptions"></param>
    public Task<TResult> FindOneAsync<TResult>(TableFilter<T> filter, CommandOptions commandOptions) where TResult : class
    {
        return FindOneAsync<TResult>(filter, null, commandOptions);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}(TableFilter{T})"/>
    /// <param name="filter"></param>
    /// <param name="findOptions">Specify Sort options for the find operation.</param>
    public Task<TResult> FindOneAsync<TResult>(TableFilter<T> filter, TableFindOneOptions<T> findOptions) where TResult : class
    {
        return FindOneAsync<TResult>(filter, findOptions, null);
    }

    /// <inheritdoc cref="FindOneAsync{TResult}(TableFilter{T}, TableFindOneOptions{T})"/>
    /// <param name="filter"></param>
    /// <param name="findOptions"></param>
    /// <param name="commandOptions"></param>
    public Task<TResult> FindOneAsync<TResult>(TableFilter<T> filter, TableFindOneOptions<T> findOptions, CommandOptions commandOptions) where TResult : class
    {
        return FindOneAsync<TResult>(filter, findOptions, commandOptions, false);
    }

    internal async Task<TResult> FindOneAsync<TResult>(TableFilter<T> filter, TableFindOneOptions<T> findOptions, CommandOptions commandOptions, bool runSynchronously) where TResult : class
    {
        findOptions = findOptions != null ? findOptions.Clone() : new TableFindOneOptions<T>();
        if (filter != null)
        {
            if (findOptions.Filter == null)
            {
                findOptions.Filter = filter;
            } else
            {
                throw new ArgumentException("Cannot pass a filter both within FindOptions and as stand-alone argument");
            }
        }
        commandOptions = SetRowSerializationOptions<TResult>(commandOptions, false);
        var command = CreateCommand("findOne").WithPayload(findOptions).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnData<DocumentResult<TResult>, TableFindStatusResult>(runSynchronously).ConfigureAwait(false);
        if (typeof(Row).IsAssignableFrom(typeof(TResult)))
        {
            if (response is { Data.Document: not null })
            {
                ProcessUntypedRow(response.Data.Document as Row, response.Status.ProjectionSchema);
            }
        }
        return response.Data.Document;
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
            // Register SimpleDictionaryConverter for untyped Row serialization
            if (isInsert && typeof(T) == typeof(Row))
            {
                commandOptions.InputConverter = new SimpleDictionaryConverter();
            }
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
    /// Synchronous version of <see cref="UpdateOneAsync(TableFilter{T}, UpdateBuilder{T})"/>
    /// </summary>
    /// <inheritdoc cref="UpdateOneAsync(TableFilter{T}, UpdateBuilder{T})"/>
    public void UpdateOne(TableFilter<T> filter, UpdateBuilder<T> update)
    {
        UpdateOne(filter, update, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="UpdateOneAsync(TableFilter{T}, UpdateBuilder{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="UpdateOneAsync(TableFilter{T}, UpdateBuilder{T}, CommandOptions)"/>
    public void UpdateOne(TableFilter<T> filter, UpdateBuilder<T> update, CommandOptions commandOptions)
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
    public Task UpdateOneAsync(TableFilter<T> filter, UpdateBuilder<T> update)
    {
        return UpdateOneAsync(filter, update, null);
    }

    /// <inheritdoc cref="UpdateOneAsync(TableFilter{T}, UpdateBuilder{T})"/>
    /// <param name="filter"></param>
    /// <param name="update"></param>
    /// <param name="commandOptions"></param>
    public Task UpdateOneAsync(TableFilter<T> filter, UpdateBuilder<T> update, CommandOptions commandOptions)
    {
        return UpdateOneAsync(filter, update, commandOptions, false);
    }

    internal async Task UpdateOneAsync(TableFilter<T> filter, UpdateBuilder<T> update, CommandOptions commandOptions, bool runSynchronously)
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
    /// This is a synchronous version of <see cref="DeleteOneAsync(TableDeleteOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="DeleteOneAsync(TableDeleteOptions{T})"/>
    public void DeleteOne(TableDeleteOptions<T> deleteOptions)
    {
        DeleteOne(null, deleteOptions, null);
    }

    /// <summary>
    /// This is a synchronous version of <see cref="DeleteOneAsync(TableFilter{T})"/>
    /// </summary>
    /// <inheritdoc cref="DeleteOneAsync(TableFilter{T})"/>
    public void DeleteOne(TableFilter<T> filter)
    {
        DeleteOne(filter, null, null);
    }

    /// <summary>
    /// This is a synchronous version of <see cref="DeleteOneAsync(TableFilter{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="DeleteOneAsync(TableFilter{T}, CommandOptions)"/>
    public void DeleteOne(TableFilter<T> filter, CommandOptions commandOptions)
    {
        DeleteOne(filter, null, commandOptions);
    }

    /// <summary>
    /// This is a synchronous version of <see cref="DeleteOneAsync(TableDeleteOptions{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="DeleteOneAsync(TableDeleteOptions{T}, CommandOptions)"/>
    public void DeleteOne(TableDeleteOptions<T> deleteOptions, CommandOptions commandOptions)
    {
        DeleteOne(null, deleteOptions, commandOptions);
    }

    /// <summary>
    /// This is a synchronous version of <see cref="DeleteOneAsync(TableFilter{T}, TableDeleteOptions{T})"/>
    /// </summary>
    /// <inheritdoc cref="DeleteOneAsync(TableFilter{T}, TableDeleteOptions{T})"/>
    public void DeleteOne(TableFilter<T> filter, TableDeleteOptions<T> deleteOptions)
    {
        DeleteOne(filter, deleteOptions, null);
    }

    /// <summary>
    /// This is a synchronous version of <see cref="DeleteOneAsync(TableFilter{T}, TableDeleteOptions{T}, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="DeleteOneAsync(TableFilter{T}, TableDeleteOptions{T}, CommandOptions)"/>
    public void DeleteOne(TableFilter<T> filter, TableDeleteOptions<T> deleteOptions, CommandOptions commandOptions)
    {
        DeleteOneAsync(filter, deleteOptions, commandOptions, true).ResultSync();
    }

    /// <summary>
    /// Delete a row from the table.
    /// </summary>
    /// <param name="deleteOptions"></param>
    /// <returns></returns>
    public Task DeleteOneAsync(TableDeleteOptions<T> deleteOptions)
    {
        return DeleteOneAsync(null, deleteOptions, null);
    }

    /// <summary>
    /// Delete a row from the table.
    /// </summary>
    /// <param name="filter"></param>
    /// <returns></returns>
    public Task DeleteOneAsync(TableFilter<T> filter)
    {
        return DeleteOneAsync(filter, null, null);
    }

    /// <inheritdoc cref="DeleteOneAsync(TableFilter{T})"/>
    /// <param name="filter"></param>
    /// <param name="commandOptions"></param>
    public Task DeleteOneAsync(TableFilter<T> filter, CommandOptions commandOptions)
    {
        return DeleteOneAsync(filter, null, commandOptions);
    }

    /// <inheritdoc cref="DeleteOneAsync(TableDeleteOptions{T})"/>
    /// <param name="deleteOptions"></param>
    /// <param name="commandOptions"></param>
    public Task DeleteOneAsync(TableDeleteOptions<T> deleteOptions, CommandOptions commandOptions)
    {
        return DeleteOneAsync(null, deleteOptions, commandOptions);
    }

    /// <inheritdoc cref="DeleteOneAsync(TableFilter{T})"/>
    /// <param name="filter"></param>
    /// <param name="deleteOptions"></param>
    public Task DeleteOneAsync(TableFilter<T> filter, TableDeleteOptions<T> deleteOptions)
    {
        return DeleteOneAsync(filter, deleteOptions, null);
    }

    /// <inheritdoc cref="DeleteOneAsync(TableFilter{T}, TableDeleteOptions{T})"/>
    /// <param name="filter"></param>
    /// <param name="deleteOptions"></param>
    /// <param name="commandOptions"></param>
    public Task DeleteOneAsync(TableFilter<T> filter, TableDeleteOptions<T> deleteOptions, CommandOptions commandOptions)
    {
        return DeleteOneAsync(filter, deleteOptions, commandOptions, false);
    }

    internal async Task DeleteOneAsync(TableFilter<T> filter, TableDeleteOptions<T> deleteOptions, CommandOptions commandOptions, bool runSynchronously)
    {
        deleteOptions ??= new TableDeleteOptions<T>();
        deleteOptions.Filter = filter;
        var command = CreateCommand("deleteOne").WithPayload(deleteOptions).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnStatus<DeleteResult>(runSynchronously).ConfigureAwait(false);
    }

    /// <inheritdoc cref="DeleteManyAsync(TableFilter{T})"/>
    /// Synchronous version of <see cref="DeleteManyAsync(TableFilter{T})"/>
    public void DeleteMany(TableFilter<T> filter)
    {
        DeleteMany(filter, null);
    }

    /// <inheritdoc cref="DeleteManyAsync(TableFilter{T}, CommandOptions)"/>
    /// Synchronous version of <see cref="DeleteManyAsync(TableFilter{T}, CommandOptions)"/>
    public void DeleteMany(TableFilter<T> filter, CommandOptions commandOptions)
    {
        DeleteManyAsync(filter, commandOptions, true).ResultSync();
    }

    /// <summary>
    /// Delete all documents matching the filter from the table.
    /// </summary>
    /// <param name="filter"></param>
    /// <returns></returns>
    public Task DeleteManyAsync(TableFilter<T> filter)
    {
        return DeleteManyAsync(filter, null);
    }

    /// <inheritdoc cref="DeleteManyAsync(TableFilter{T})"/>
    /// <param name="filter"></param>
    /// <param name="commandOptions"></param>
    public Task DeleteManyAsync(TableFilter<T> filter, CommandOptions commandOptions)
    {
        return DeleteManyAsync(filter, commandOptions, false);
    }

    internal async Task DeleteManyAsync(TableFilter<T> filter, CommandOptions commandOptions, bool runSynchronously)
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
}
