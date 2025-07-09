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

using System;

namespace DataStax.AstraDB.DataApi.Core.Results;

/// <summary>
/// Exception thrown from bulk operaions (e.g. Collection.InsertManyAsync, Collection.UpdateManyAsync, etc.) when an error occurs.
/// If the operation was partially successful, the <see cref="PartialResult"/> property will contain the results of the operation that succeeded.
/// </summary>
public class BulkOperationException<T> : Exception
{
    public BulkOperationException(string message, T partialResult) : base(message)
    {
        PartialResult = partialResult;
    }
    public BulkOperationException(Exception causingException, T partialResult) : base(causingException.Message, causingException)
    {
        PartialResult = partialResult;
    }

    public T PartialResult { get; set; }
}