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

public static class DataApiKeywords
{
    public const string Id = "_id";
    public const string All = "$all";
    public const string Date = "$date";
    public const string Uuid = "$uuid";
    public const string ObjectId = "$objectId";
    public const string Size = "$size";
    public const string Exists = "$exists";
    public const string Slice = "$slice";
    public const string Similarity = "$similarity";
    public const string Vector = "$vector";
    public const string SortVector = "sortVector";
    public const string Vectorize = "$vectorize";
}