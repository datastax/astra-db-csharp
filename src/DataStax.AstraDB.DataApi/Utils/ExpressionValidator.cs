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
using System.Linq.Expressions;
using System.Reflection;

namespace DataStax.AstraDB.DataApi.Utils;

public class ExpressionValidator
{
    public static bool DoesPropertyHaveAttribute<T, TField, TAttribute>(Expression<Func<T, TField>> expression)
        where TAttribute : Attribute
    {
        if (expression.Body is MemberExpression memberExpression &&
            memberExpression.Member is PropertyInfo propertyInfo)
        {
            return propertyInfo.GetCustomAttribute<TAttribute>() != null;
        }
        return false;
    }
}