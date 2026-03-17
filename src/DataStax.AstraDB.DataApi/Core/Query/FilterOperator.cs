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

namespace DataStax.AstraDB.DataApi.Core.Query;

/// <summary>
/// String constants for Data API filter operators used in query filter expressions.
/// </summary>
public class FilterOperator
{
    /// <summary>Matches values greater than the specified value ($gt).</summary>
    public const string GreaterThan = "$gt";
    /// <summary>Matches values greater than or equal to the specified value ($gte).</summary>
    public const string GreaterThanOrEqualTo = "$gte";
    /// <summary>Matches values less than the specified value ($lt).</summary>
    public const string LessThan = "$lt";
    /// <summary>Matches values less than or equal to the specified value ($lte).</summary>
    public const string LessThanOrEqualTo = "$lte";
    /// <summary>Matches values equal to the specified value ($eq).</summary>
    public const string EqualsTo = "$eq";
    /// <summary>Matches values not equal to the specified value ($ne).</summary>
    public const string NotEqualsTo = "$ne";
    /// <summary>Matches values that are contained within the specified array ($in).</summary>
    public const string In = "$in";
    /// <summary>Matches values that are not contained within the specified array ($nin).</summary>
    public const string NotIn = "$nin";
    /// <summary>Matches documents where the field exists ($exists).</summary>
    public const string Exists = "$exists";
    /// <summary>Matches arrays that contain all of the specified elements ($all).</summary>
    public const string All = "$all";
    /// <summary>Matches arrays with the specified number of elements ($size).</summary>
    public const string Size = "$size";
    /// <summary>Matches documents where the array field contains the specified value ($contains).</summary>
    public const string Contains = "$contains";
    /// <summary>Matches documents where the field matches the specified sub-document ($match).</summary>
    public const string Match = "$match";
    internal const string Keys = "$keys";
    internal const string Values = "$values";
}
