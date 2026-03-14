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

using DataStax.AstraDB.DataApi.Utils;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Tables;

/// <summary>
/// Represents operations that can be applied to an existing User Defined Type.
/// </summary>
/// <remarks>
/// The following operations are available:
/// <list type="bullet">
///   <item><see cref="AlterTypeAddFields"/> - Adds new fields to a User Defined Type.</item>
///   <item><see cref="AlterTypeRenameFields"/> - Renames fields in a User Defined Type.</item>
/// </list>
/// </remarks>
public interface IAlterTypeOperation
{
  /// <summary>
  /// Gets the operation as a tuple (operation name, operation data).
  /// </summary>
  /// <returns>A tuple where Item1 is the operation name and Item2 is the operation data.</returns>
  (string, object) GetOperation();
}

/// <summary>
/// Represents an operation to add new fields to a User Defined Type.
/// </summary>
public class AlterTypeAddFields : IAlterTypeOperation
{
  /// <summary>
  /// Gets the fields to be added.
  /// </summary>
  public Dictionary<string, string> Fields { get; }

  /// <summary>
  /// Initializes a new instance with the specified fields.
  /// </summary>
  /// <param name="fields">The fields to add (field name -> field type).</param>
  public AlterTypeAddFields(Dictionary<string, DataApiType> fields)
  {
    Fields = fields.ToDictionary(e => e.Key, e => e.Value.Key);
  }

  /// <inheritdoc/>
  public (string, object) GetOperation() => (
    "add",
    new
    { 
      fields = Fields
    }
  );
}

/// <summary>
/// Represents an operation to rename fields in a User Defined Type.
/// </summary>
public class AlterTypeRenameFields : IAlterTypeOperation
{
  /// <summary>
  /// Gets the fields to be renamed.
  /// </summary>
  public Dictionary<string, string> Fields { get; }

  /// <summary>
  /// Initializes a new instance with the specified field renames.
  /// </summary>
  /// <param name="fields">The fields to rename (old name -> new name).</param>
  public AlterTypeRenameFields(Dictionary<string, string> fields)
  {
    Fields = fields;
  }

  /// <inheritdoc/>
  public (string, object) GetOperation() => (
    "rename",
    new
    { 
      fields = Fields
    }
  );
}
