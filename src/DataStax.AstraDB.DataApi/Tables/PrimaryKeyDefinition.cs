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
using DataStax.AstraDB.DataApi.Core.Commands;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Tables;


/// <summary>
/// Defines the primary key structure of a table, including partition keys and clustering keys with their sort orders.
/// </summary>
public class PrimaryKeyDefinition
{
    /*
    "primaryKey": {
      "partitionBy": [
        "title", "rating"
      ],
      "partitionSort": {
        "number_of_pages": 1,
        "is_checked_out": -1
      }
    }
    */
    private List<string> _keys;
    /// <summary>
    /// The ordered list of partition key column names.
    /// </summary>
    [JsonPropertyName("partitionBy")]
    public List<string> Keys
    {
        get
        {
            if (_keys == null)
            {
                var keys = KeyList.Keys;
                if (!keys.IsValidKeyOrder())
                {
                    throw new InvalidOperationException("Primary Keys must be in a contiguous sequence starting from 1");
                }
                _keys = KeyList.OrderBy(pair => pair.Key - 1).Select(pair => pair.Value).ToList();
            }
            return _keys;
        }
        set { _keys = value; }
    }

    internal Dictionary<int, string> KeyList { get; set; } = new Dictionary<int, string>();

    private Dictionary<string, SortDirection> _sorts;
    /// <summary>
    /// The clustering key columns and their sort directions, ordered by clustering key position.
    /// </summary>
    [JsonIgnore]
    public Dictionary<string, SortDirection> Sorts
    {
        get
        {
            if (_sorts == null)
            {
                var keys = SortList.Keys;
                if (keys.Any() && !keys.IsValidKeyOrder())
                {
                    throw new InvalidOperationException("Primary Key Sort Orders must be in a contiguous sequence starting from 1");
                }
                _sorts = SortList.OrderBy(pair => pair.Key - 1).ToDictionary(pair => pair.Value.Key,
                    pair => pair.Value.Direction);
            }
            return _sorts;
        }
        set { _sorts = value; }
    }

    [JsonInclude]
    [JsonPropertyName("partitionSort")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    internal Dictionary<string, int> SortsSerialized
    {
        get
        {
            return Sorts.Select(pair => new { pair.Key, pair.Value }).ToDictionary(
                pair => pair.Key, pair => pair.Value == SortDirection.Ascending ? DataApiKeywords.SortAscending : DataApiKeywords.SortDescending);
        }
        set
        {
            Sorts = value.ToDictionary(
                pair => pair.Key, pair => pair.Value == DataApiKeywords.SortAscending ? SortDirection.Ascending : SortDirection.Descending);
        }
    }

    internal Dictionary<int, PrimaryKeySort> SortList { get; set; } = new Dictionary<int, PrimaryKeySort>();
}

/// <summary>
/// Defines the sort order for a single clustering key column.
/// </summary>
public class PrimaryKeySort
{
    /// <summary>
    /// The column name of the clustering key.
    /// </summary>
    public string Key { get; set; }

    /// <summary>
    /// The sort direction for this clustering key column.
    /// </summary>
    public SortDirection Direction { get; set; }

    /// <summary>
    /// Initializes a new instance of <see cref="PrimaryKeySort"/> with the specified key and sort direction.
    /// </summary>
    /// <param name="key">The column name of the clustering key.</param>
    /// <param name="direction">The sort direction for this clustering key column.</param>
    public PrimaryKeySort(string key, SortDirection direction)
    {
        Key = key;
        Direction = direction;
    }
}

/// <summary>
/// Extension methods for validating primary key ordering sequences.
/// </summary>
public static class PrimaryKeyExtensions
{
    /// <summary>
    /// Determines whether a sequence of integer key positions forms a valid contiguous sequence starting from 1.
    /// </summary>
    public static bool IsValidKeyOrder(this IEnumerable<int> numbers)
    {
        if (numbers == null || !numbers.Any())
        {
            return false;
        }

        if (numbers.Any(n => n < 1))
        {
            return false; // All numbers must be >= 1
        }

        if (numbers.Distinct().Count() != numbers.Count())
        {
            return false; // There are duplicates
        }

        if (numbers.Count() == 1 && numbers.First() == 1)
        {
            return true;
        }

        var sortedNumbers = numbers.OrderBy(n => n).ToList();

        if (sortedNumbers[0] != 1)
        {
            return false; // The contiguous sequence must start with 1
        }

        for (int i = 0; i < sortedNumbers.Count - 1; i++)
        {
            if (sortedNumbers[i + 1] != sortedNumbers[i] + 1)
            {
                return false; // Found a gap in the contiguous sequence starting from 1
            }
        }

        return true;
    }
}
