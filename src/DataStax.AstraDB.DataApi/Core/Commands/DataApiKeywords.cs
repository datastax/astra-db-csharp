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

namespace DataStax.AstraDB.DataApi.Core.Commands;

internal static class DataApiKeywords
{
    internal const string Id = "_id";
    internal const string All = "$all";
    internal const string Date = "$date";
    internal const string Uuid = "$uuid";
    internal const string ObjectId = "$objectId";
    internal const string Size = "$size";
    internal const string Exists = "$exists";
    internal const string Slice = "$slice";
    internal const string Similarity = "$similarity";
    internal const string Vector = "$vector";
    internal const string SortVector = "sortVector";
    internal const string Vectorize = "$vectorize";
}