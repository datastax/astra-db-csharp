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

public class PrimaryKeySort
{
    public string Key { get; set; }
    public SortDirection Direction { get; set; }

    public PrimaryKeySort(string key, SortDirection direction)
    {
        Key = key;
        Direction = direction;
    }
}

public static class PrimaryKeyExtensions
{
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
