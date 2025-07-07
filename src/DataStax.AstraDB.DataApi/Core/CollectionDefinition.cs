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

using DataStax.AstraDB.DataApi.SerDes;
using System;
using System.Reflection;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Core;

/// <summary>
/// Options for a collection's behavior
/// </summary>
public class CollectionDefinition
{
    /// <summary>
    /// Settings for generating ids
    /// </summary>
    [JsonPropertyName("defaultId")]
    public DefaultIdOptions DefaultId { get; set; }

    /// <summary>
    /// Vector specifications for the collection
    /// </summary>
    [JsonPropertyName("vector")]
    public VectorOptions Vector { get; set; }

    /// <summary>
    /// Overrides for document indexing
    /// </summary>
    [JsonPropertyName("indexing")]
    public IndexingOptions Indexing { get; set; }

    /// <summary>
    /// Lexical analysis options for the collection
    /// </summary>
    [JsonPropertyName("lexical")]
    public LexicalOptions Lexical { get; set; }

    /// <summary>
    /// Reranking options for the collection
    /// </summary>
    [JsonPropertyName("rerank")]
    public RerankOptions Rerank { get; set; }

    internal static CollectionDefinition Create<T>()
    {
        return CheckAddDefinitionsFromAttributes<T>(new CollectionDefinition());
    }

    internal static CollectionDefinition CheckAddDefinitionsFromAttributes<T>(CollectionDefinition definition)
    {
        Type type = typeof(T);
        PropertyInfo idProperty = null;
        DocumentIdAttribute idAttribute = null;

        if (definition.DefaultId == null)
        {
            foreach (var property in type.GetProperties())
            {
                var attr = property.GetCustomAttribute<DocumentIdAttribute>();
                if (attr != null)
                {
                    idProperty = property;
                    idAttribute = attr;
                    break;
                }
            }

            if (idProperty != null)
            {
                if (idAttribute.DefaultIdType.HasValue)
                {
                    definition.DefaultId = new DefaultIdOptions() { Type = idAttribute.DefaultIdType.Value };
                }
            }
        }

        return definition;
    }
}




