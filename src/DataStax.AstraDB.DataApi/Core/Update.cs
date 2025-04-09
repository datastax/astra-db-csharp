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

using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Core;

internal class Update<T>
{
    internal string UpdateOperator { get; set; }
    internal string FieldName { get; set; }
    internal object FieldValue { get; set; }

    internal Update(string updateOperator, string fieldName, object value)
    {
        UpdateOperator = updateOperator;
        FieldName = fieldName;
        FieldValue = value;
    }

}

internal class AddToSetValue<T>
{
    [JsonPropertyName("$each")]
    public List<T> Each { get; set; }
}

internal class PushUpdateValue<T>
{
    [JsonPropertyName("$each")]
    public List<T> Each { get; set; }
    [JsonPropertyName("$position")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? Position { get; set; } = null;
}