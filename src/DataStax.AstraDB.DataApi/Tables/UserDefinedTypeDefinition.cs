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
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Tables;

/// <summary>
/// Options for creating a User Defined Type
/// </summary>
public class UserDefinedTypeDefinition
{
    [JsonPropertyName("fields")]
    [JsonInclude]
    private Dictionary<string, string> FieldTypes => Fields.ToDictionary(x => x.Key, x => x.Value.ToString().ToLowerInvariant());

    /// <summary>
    /// List of fields (field name, field type) for this User Defined Type
    /// </summary>
    public Dictionary<string, DataApiType> Fields { get; set; } = new Dictionary<string, DataApiType>();

}


internal class UserDefinedTypeRequest
{
    [JsonInclude]
    [JsonPropertyName("name")]
    internal string Name { get; set; }

    [JsonInclude]
    [JsonPropertyName("definition")]
    internal UserDefinedTypeDefinition TypeDefinition { get; set; }

    [JsonInclude]
    [JsonPropertyName("options")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    internal Dictionary<string, object> Options { get; set; }

    internal void SetSkipIfExists(bool skipIfExists)
    {
        var optionsKey = "ifNotExists";
        if (!skipIfExists)
        {
            if (Options != null)
            {
                Options.Remove(optionsKey);
            }
        }
        else
        {
            Options ??= new Dictionary<string, object>();
            Options[optionsKey] = skipIfExists;
        }
    }

    internal static UserDefinedTypeDefinition CreateDefinitionFromType<T>()
    {
        return CreateDefinitionFromType(typeof(T));
    }

    internal static UserDefinedTypeDefinition CreateDefinitionFromType(Type wrapperType)
    {
        UserDefinedTypeDefinition definition = new();

        foreach (var property in wrapperType.GetProperties())
        {
            var propertyType = property.GetType();
            var typeInfo = TypeUtilities.GetDataApiType(propertyType);
            definition.Fields.Add(GetColumnName(propertyType), typeInfo);
        }

        return definition;
    }

    internal static string GetUserDefinedTypeName<T>()
    {
        return GetUserDefinedTypeName(typeof(T));
    }

    internal static string GetUserDefinedTypeName(Type type)
    {
        var nameAttribute = type.GetCustomAttribute<UserDefinedTypeAttribute>();
        return GetUserDefinedTypeName(type, nameAttribute);
    }

    internal static string GetUserDefinedTypeName(Type type, UserDefinedTypeAttribute nameAttribute)
    {
        if (nameAttribute != null && nameAttribute.Name != null)
        {
            return nameAttribute.Name;
        }
        return type.Name;
    }

    internal static string GetColumnName(Type type)
    {
        var nameAttribute = type.GetCustomAttribute<ColumnNameAttribute>();
        if (nameAttribute != null)
        {
            return nameAttribute.Name;
        }
        return type.Name;
    }
}


