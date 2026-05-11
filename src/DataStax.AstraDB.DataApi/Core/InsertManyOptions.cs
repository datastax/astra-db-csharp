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
public class InsertManyOptions : CommandOptions
{
    /// <summary>
    /// Default batch size.
    /// </summary>
    public const int DefaultChunkSize = 50;

    /// <summary>
    /// Default concurrency for unordered insertions.
    /// </summary>
    public const int DefaultConcurrency = 20;

    private bool _Ordered = false;
    /// <summary>
    /// Whether to insert documents in order or not.
    /// </summary>
    public bool Ordered
    {
        get => _Ordered;
        set
        {
            if (value) Concurrency = 1;
            _Ordered = value;
        }
    }
    /// <summary>
    /// The number of parallel processes to use while inserting documents.
    /// Must be set to 1 for ordered inserts.
    /// </summary>
    public int Concurrency { get; set; } = DefaultConcurrency;

    /// <summary>
    /// The number of documents to insert in each batch.
    /// </summary>
    public int ChunkSize { get; set; } = DefaultChunkSize;

    internal CommandOptions CommandOptions()
    {
        return new CommandOptions {
            Token = Token,
            RunMode = RunMode,
            Destination = Destination,
            HttpClientOptions = HttpClientOptions != null ? HttpClientOptions.Clone() : null,
            TimeoutOptions = TimeoutOptions != null ? TimeoutOptions.Clone() : null,
            APIVersion = APIVersion,
            CancellationToken = CancellationToken
        };
    }

    internal InsertManyOptions Clone()
    {
        return new InsertManyOptions
        {
            Ordered = Ordered,
            Concurrency = Concurrency,
            ChunkSize = ChunkSize,
            // CommandOptions properties:
            Token = Token,
            RunMode = RunMode,
            Destination = Destination,
            HttpClientOptions = HttpClientOptions != null ? HttpClientOptions.Clone() : null,
            TimeoutOptions = TimeoutOptions != null ? TimeoutOptions.Clone() : null,
            APIVersion = APIVersion,
            CancellationToken = CancellationToken
        };
    }

}
