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
    /// Create a default index.
    /// </summary>
    /// <returns></returns>
    public TableIndexDefinition Index(bool caseSensitive = true, bool normalize = false, bool ascii = false)
    {
        return new TableIndexDefinition
        {
            CaseSensitive = caseSensitive,
            Normalize = normalize,
            Ascii = ascii
        };
    }

    /// <summary>
    /// Create a text index using the default analyzer.
    /// </summary>
    /// <returns></returns>
    public TableIndexDefinition Text()
    {
        return new TableTextIndexDefinition();
    }

    /// <summary>
    /// Create a text index using a specific analyzer.
    /// </summary>
    /// <param name="analyzer"></param>
    /// <returns></returns>
    public TableIndexDefinition Text(TextAnalyzer analyzer)
    {
        return new TableTextIndexDefinition()
        {
            Analyzer = analyzer
        };
    }

    /// <summary>
    /// Create a text index using a specific analyzer by name, for example a language-specific analyzer.
    /// See https://docs.datastax.com/en/astra-db-serverless/databases/analyzers.html#supported-built-in-analyzers
    /// </summary>
    /// <param name="analyzer"></param>
    /// <returns></returns>
    public TableIndexDefinition Text(string analyzer)
    {
        return new TableTextIndexDefinition()
        {
            Analyzer = analyzer
        };
    }

    /// <summary>
    /// Create a text index using custom analyzer options.
    /// </summary>
    /// <param name="analyzerOptions"></param>
    /// <returns></returns>
    public TableIndexDefinition Text(AnalyzerOptions analyzerOptions)
    {
        return new TableTextIndexDefinition()
        {
            Analyzer = analyzerOptions
        };
    }

    /// <summary>
    /// Create a vector index.
    /// </summary>
    /// <param name="metric">Optional similarity metric to use for vector searches on this index</param>
    /// <param name="sourceModel">Allows enabling certain vector optimizations on the index by specifying the source model for your vectors</param>
    /// <returns></returns>
    public TableIndexDefinition Vector(SimilarityMetric metric = SimilarityMetric.Cosine, string sourceModel = "other")
    {
        return new TableVectorIndexDefinition
        {
            Metric = metric,
            SourceModel = sourceModel
        };
    }
}
