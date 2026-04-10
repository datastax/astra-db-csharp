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
/// An index builder for tables.
/// </summary>
public class TableIndexBuilder
{
    /// <summary>
    /// Create a default table index.
    /// </summary>
    /// <param name="caseSensitive">Whether the index should be case sensitive</param>
    /// <param name="normalize">Whether the index should normalize the text</param>
    /// <param name="ascii">Whether the index should use ASCII conversion</param>
    /// <returns>An index definition for use in a <see cref="Table"/> CreateIndex method call.</returns>
    public TableIndexDefinition Index(bool caseSensitive = true, bool normalize = false, bool ascii = false)
    {
        return new TableIndexDefinition
        {
            Options = new TableIndexOptions {
                CaseSensitive = caseSensitive,
                Normalize = normalize,
                Ascii = ascii
            }
        };
    }

    /// <summary>
    /// Create a table index for a map column.
    /// </summary>
    /// <param name="mapIndexType">A <see cref="MapIndexType"/> value specifying how the map is indexed (keys, values or entries).</param>
    /// <returns>An index definition for use in a <see cref="Table"/> CreateIndex method call.</returns>
    public TableIndexDefinition Map(MapIndexType mapIndexType)
    {
        return new TableMapIndexDefinition(mapIndexType);
    }

    /// <summary>
    /// Create a table index for a map column.
    /// </summary>
    /// <param name="mapIndexType">A <see cref="MapIndexType"/> value specifying how the map is indexed (keys, values or entries).</param>
    /// <param name="caseSensitive">Whether the index should be case sensitive</param>
    /// <param name="normalize">Whether the index should normalize the text</param>
    /// <param name="ascii">Whether the index should use ASCII conversion</param>
    /// <returns>An index definition for use in a <see cref="Table"/> CreateIndex method call.</returns>
    public TableIndexDefinition Map(MapIndexType mapIndexType, bool caseSensitive = true, bool normalize = false, bool ascii = false)
    {
        return new TableMapIndexDefinition (
            mapIndexType,
            new TableIndexOptions {
                CaseSensitive = caseSensitive,
                Normalize = normalize,
                Ascii = ascii
            }
        );
    }

    /// <summary>
    /// Create a table text index using the default analyzer.
    /// </summary>
    /// <returns>An index definition for use in a <see cref="Table"/> CreateIndex method call.</returns>
    public TableTextIndexDefinition Text()
    {
        return new TableTextIndexDefinition();
    }

    /// <summary>
    /// Create a table text index using a specific analyzer.
    /// See https://docs.datastax.com/en/astra-db-serverless/databases/analyzers.html#supported-built-in-analyzers
    /// </summary>
    /// <param name="analyzer">A <see cref="TextAnalyzer"/> value specifying a preset indexing method.</param>
    /// <returns>An index definition for use in a <see cref="Table"/> CreateIndex method call.</returns>
    public TableTextIndexDefinition Text(TextAnalyzer analyzer)
    {
        return new TableTextIndexDefinition()
        {
            Options = new TableTextIndexOptions {
                Analyzer = analyzer
            }
        };
    }

    /// <summary>
    /// Create a table text index using a specific analyzer by name, for example a language-specific analyzer.
    /// See https://docs.datastax.com/en/astra-db-serverless/databases/analyzers.html#supported-built-in-analyzers
    /// </summary>
    /// <param name="analyzer">A string value specifying a preset indexing method.</param>
    /// <returns>An index definition for use in a <see cref="Table"/> CreateIndex method call.</returns>
    public TableTextIndexDefinition Text(string analyzer)
    {
        return new TableTextIndexDefinition()
        {
            Options = new TableTextIndexOptions {
                Analyzer = analyzer
            }
        };
    }

    /// <summary>
    /// Create a table text index using custom analyzer options.
    /// See https://docs.datastax.com/en/astra-db-serverless/databases/analyzers.html#supported-built-in-analyzers
    /// </summary>
    /// <param name="analyzerOptions">An <see cref="AnalyzerOptions"/> object defining the analyzer options for the indexing.</param>
    /// <returns>An index definition for use in a <see cref="Table"/> CreateIndex method call.</returns>
    public TableTextIndexDefinition Text(AnalyzerOptions analyzerOptions)
    {
        return new TableTextIndexDefinition()
        {
            Options = new TableTextIndexOptions {
                Analyzer = analyzerOptions
            }
        };
    }

    /// <summary>
    /// Create a table text index using custom, free-form analyzer options.
    /// See https://docs.datastax.com/en/astra-db-serverless/databases/analyzers.html#supported-built-in-analyzers
    /// </summary>
    /// <param name="analyzer">A free-form object defining the analyzer options for the indexing.</param>
    /// <returns>An index definition for use in a <see cref="Table"/> CreateIndex method call.</returns>
    public TableTextIndexDefinition Text(object analyzer)
    {
        return new TableTextIndexDefinition()
        {
            Options = new TableTextIndexOptions {
                Analyzer = analyzer
            }
        };
    }

    /// <summary>
    /// Create a table vector index.
    /// </summary>
    /// <returns>An index definition for use in a <see cref="Table"/> CreateIndex method call.</returns>
    public TableVectorIndexDefinition Vector()
    {
        return Vector(null, null);
    }

    /// <summary>
    /// Create a table vector index.
    /// </summary>
    /// <param name="metric">Similarity metric to use for vector searches on this index</param>
    /// <returns>An index definition for use in a <see cref="Table"/> CreateIndex method call.</returns>
    public TableVectorIndexDefinition Vector(SimilarityMetric metric)
    {
        return Vector(metric, null);
    }

    /// <summary>
    /// Create a table vector index.
    /// </summary>
    /// <param name="sourceModel">Allows enabling certain vector optimizations on the index by specifying the source model for your vectors</param>
    /// <returns>An index definition for use in a <see cref="Table"/> CreateIndex method call.</returns>
    public TableVectorIndexDefinition Vector(string sourceModel)
    {
        return Vector(null, sourceModel);
    }

    /// <summary>
    /// Create a table vector index.
    /// </summary>
    /// <param name="metric">Similarity metric to use for vector searches on this index</param>
    /// <param name="sourceModel">Allows enabling certain vector optimizations on the index by specifying the source model for your vectors. Pass a null for server default.</param>
    /// <returns>An index definition for use in a <see cref="Table"/> CreateIndex method call.</returns>
    public TableVectorIndexDefinition Vector(SimilarityMetric? metric, string sourceModel)
    {
        return new TableVectorIndexDefinition
        {
            Options = new TableVectorIndexOptions {
                Metric = metric,
                SourceModel = sourceModel
            }
        };
    }

}
