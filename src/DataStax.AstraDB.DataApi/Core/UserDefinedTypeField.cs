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
/// Represents a field in a user-defined type (UDT), consisting of a name and a CQL type.
/// </summary>
public class UserDefinedTypeField
{
    /// <summary>The name of the UDT field.</summary>
    public string Name { get; set; }
    /// <summary>The CQL type of the UDT field.</summary>
    public string Type { get; set; }
}