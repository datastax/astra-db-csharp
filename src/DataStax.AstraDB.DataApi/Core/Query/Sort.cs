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

using DataStax.AstraDB.DataApi.Core.Commands;
using DataStax.AstraDB.DataApi.Utils;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace DataStax.AstraDB.DataApi.Core.Query;

internal class Sort
{
    internal string Name { get; set; }
    internal object Value { get; set; }

    internal Sort Clone()
    {
        if (Value is float[] vector)
        {
            var vectorCopy = new float[vector.Length];
            Array.Copy(vector, vectorCopy, vector.Length);
            return new Sort(Name, vectorCopy);
        }
        else if (Value is Dictionary<string, object> dict)
        {
            var dictCopy = new Dictionary<string, object>();
            foreach (var kvp in dict)
            {
                if (kvp.Value is float[] vectorValue)
                {
                    var vectorCopy = new float[vectorValue.Length];
                    Array.Copy(vectorValue, vectorCopy, vectorValue.Length);
                    dictCopy[kvp.Key] = vectorCopy;
                }
                else
                {
                    dictCopy[kvp.Key] = kvp.Value;
                }
            }
            return new Sort(Name, dictCopy);
        }
        return new Sort(Name, Value);
    }

    internal Sort(string sortKey, object value)
    {
        Name = sortKey;
        Value = value;
    }

    internal static Sort Ascending(string field) => new(field, DataApiKeywords.SortAscending);

    internal static Sort Descending(string field) => new(field, DataApiKeywords.SortDescending);

    internal static Sort Vector(float[] vector) => new(DataApiKeywords.Vector, vector);

    internal static Sort Vectorize(string valueToVectorize) => new(DataApiKeywords.Vectorize, valueToVectorize);

    internal static Sort Hybrid(string combinedSearchString) => new(DataApiKeywords.Hybrid, combinedSearchString);

    internal static Sort Hybrid(string lexical, string vectorize) => new(DataApiKeywords.Hybrid, new Dictionary<string, object> { { DataApiKeywords.Lexical, lexical }, { DataApiKeywords.Vectorize, vectorize } });

    internal static Sort Hybrid(string lexical, float[] vector) => new(DataApiKeywords.Hybrid, new Dictionary<string, object> { { DataApiKeywords.Lexical, lexical }, { DataApiKeywords.Vector, vector } });

    internal static Sort Lexical(string value) => new(DataApiKeywords.Lexical, value);
}

internal class Sort<T> : Sort
{
    internal Sort(string sortKey, object value) : base(sortKey, value) { }

    internal static Sort Ascending<TField>(Expression<Func<T, TField>> expression)
    {
        return new Sort<T>(expression.GetMemberNameTree(), DataApiKeywords.SortAscending);
    }

    internal static Sort Descending<TField>(Expression<Func<T, TField>> expression)
    {
        return new Sort<T>(expression.GetMemberNameTree(), DataApiKeywords.SortDescending);
    }
}
