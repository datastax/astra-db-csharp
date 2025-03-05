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

using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Core.Query;

public class Projection
{
    public string Field { get; set; }
    public bool Present { get; set; }
    public int? SliceStart { get; set; }
    public int? SliceEnd { get; set; }

    internal object Value
    {
        get
        {
            // valid options
            // true
            // false
            // { "$slice": [4, 2] }
            // { "$slice": -2 }
            if (SliceStart.HasValue)
            {
                if (SliceEnd.HasValue)
                {
                    return new int[] { SliceStart.Value, SliceEnd.Value };
                }
                else
                {
                    return SliceStart.Value;
                }
            }
            return Present;
        }
    }
}