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
/// Base class for insert-one operation options.
/// </summary>
public abstract class BaseInsertOneOptions<T> : CommandOptions where T : class
{
    internal object ToPayload(T document)
    {
        return new
        {
            document
        };
    }
}

/// <summary>
/// Options for inserting a single document into a collection.
/// </summary>
public sealed class CollectionInsertOneOptions<T> : BaseInsertOneOptions<T> where T : class
{
}

/// <summary>
/// Options for inserting a single row into a table.
/// </summary>
public sealed class TableInsertOneOptions<T> : BaseInsertOneOptions<T> where T : class
{
}
