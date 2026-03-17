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

namespace DataStax.AstraDB.DataApi.Tables;


/// <summary>
/// Options for dropping a table index.
/// </summary>
public class DropIndexCommandOptions : CommandOptions
{
    /// <summary>
    /// A value indicating whether to skip the drop operation if the index does not exist,
    /// avoiding an error.
    /// </summary>
    public bool IfExists { get; set; } = false;
}