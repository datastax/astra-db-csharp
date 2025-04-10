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

namespace DataStax.AstraDB.DataApi.Core;

/// <summary>
/// Options for inserting multiple documents into a collection.
/// </summary>
public class InsertManyOptions
{
    /// <summary>
    /// Default batch size.
    /// </summary>
    public const int DefaultChunkSize = 50;

    /// <summary>
    /// Maximum concurrency.
    /// </summary>
    public const int MaxConcurrency = int.MaxValue;

    private bool _insertInOrder = false;
    /// <summary>
    /// Whether to insert documents in order or not.
    /// </summary>
    public bool InsertInOrder
    {
        get => _insertInOrder;
        set
        {
            if (value) Concurrency = 1;
            _insertInOrder = value;
        }
    }
    /// <summary>
    /// The number of parallel processes to use while inserting documents.
    /// Must be set to 1 for ordered inserts.
    /// </summary>
    public int Concurrency { get; set; } = MaxConcurrency;

    /// <summary>
    /// The number of documents to insert in each batch.
    /// </summary>
    public int ChunkSize { get; set; } = DefaultChunkSize;
}