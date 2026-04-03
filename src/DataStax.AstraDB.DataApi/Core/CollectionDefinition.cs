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
using System.Collections.Generic;
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

        foreach (var property in type.GetProperties())
        {
            var attr = property.GetCustomAttribute<DocumentIdAttribute>();
            if (attr != null)
            {
                idProperty = property;
                idAttribute = attr;
            }
        }

        var lexicalAttribute = type.GetCustomAttribute<LexicalOptionsAttribute>();
        var vectorAttribute = type.GetCustomAttribute<VectorOptionsAttribute>();
        var vectorizeAttribute = type.GetCustomAttribute<VectorizeOptionsAttribute>();

        if (definition.DefaultId == null && idProperty != null)
        {
            if (idAttribute.DefaultIdType.HasValue)
            {
                definition.DefaultId = new DefaultIdOptions() { Type = idAttribute.DefaultIdType.Value };
            }
        }

        if (definition.Lexical == null && lexicalAttribute != null)
        {
            definition.Lexical = new LexicalOptions()
            {
                Analyzer = new AnalyzerOptions()
                {
                    Tokenizer = new TokenizerOptions()
                    {
                        Name = lexicalAttribute.TokenizerName,
                        Arguments = lexicalAttribute.GetArguments()
                    },
                    Filters = lexicalAttribute.Filters != null ? new List<string>(lexicalAttribute.Filters) : new List<string>(),
                    CharacterFilters = lexicalAttribute.CharacterFilters != null ? new List<string>(lexicalAttribute.CharacterFilters) : new List<string>()
                }
            };
        }

        if (vectorAttribute != null)
        {
            if (definition.Vector == null)
            {
                definition.Vector = new VectorOptions();
            }
            if (vectorAttribute.Dimension != -1)
            {
                definition.Vector.Dimension = vectorAttribute.Dimension;
            }
            definition.Vector.Metric = vectorAttribute.Metric;
            if (!string.IsNullOrEmpty(vectorAttribute.SourceModel))
            {
                definition.Vector.SourceModel = vectorAttribute.SourceModel;
            }
        }

        if (vectorizeAttribute != null)
        {
            if (definition.Vector == null)
            {
                definition.Vector = new VectorOptions();
            }
            if (vectorizeAttribute.Dimension != -1)
            {
                definition.Vector.Dimension = vectorizeAttribute.Dimension;
            }
            definition.Vector.Metric = vectorizeAttribute.Metric;
            if (!string.IsNullOrEmpty(vectorizeAttribute.Provider))
            {
                definition.Vector.Service = new VectorServiceOptions()
                {
                    Provider = vectorizeAttribute.Provider,
                    ModelName = vectorizeAttribute.ModelName,
                    Authentication = vectorizeAttribute.GetAuthentication(),
                    Parameters = vectorizeAttribute.GetParameters()
                };
            }
        }


        return definition;
    }
}




