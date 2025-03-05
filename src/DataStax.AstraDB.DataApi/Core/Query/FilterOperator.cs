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

public class FilterOperator
{
    public const string GreaterThan = "$gt";
    public const string GreaterThanOrEqualTo = "$gte";
    public const string LessThan = "$lt";
    public const string LessThanOrEqualTo = "$lte";
    public const string EqualsTo = "$eq";
    public const string NotEqualsTo = "$ne";
    public const string In = "$in";
    public const string NotIn = "$nin";
    public const string Exists = "$exists";
    public const string All = "$all";
    public const string Contains = "$contains";
    public const string ContainsKey = "$containsKey";
    public const string ContainsEntry = "$containsEntry";
}
