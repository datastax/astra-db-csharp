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

using System;

namespace DataStax.AstraDB.DataApi.Core.Query;

internal enum LogicalOperator : int
{
    And,
    Or,
    Not
}

internal static class LogicalOperatorExtensions
{
    internal static string ToApiString(this LogicalOperator value)
    {
        return value switch
        {
            LogicalOperator.And => "$and",
            LogicalOperator.Or => "$or",
            LogicalOperator.Not => "$not",
            _ => throw new ArgumentException("Invalid Logical Operator"),
        };
    }
}