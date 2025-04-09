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
using System.Collections;

namespace DataStax.AstraDB.DataApi.Utils;

internal static class Guard
{
    internal static void NotNullOrEmpty(string value, string paramName, string message = null)
    {
        if (string.IsNullOrEmpty(value))
        {
            throw new ArgumentNullException(message.OrIfEmpty("Value cannot be null or empty."), paramName);
        }
    }

    internal static void NotNullOrEmpty(IList value, string paramName, string message = null)
    {
        if (value == null || value.Count == 0)
        {
            throw new ArgumentNullException(message.OrIfEmpty("Value cannot be null or empty."), paramName);
        }
    }

    internal static void Equals<T>(T value, T valueTwo, string paramName, string message = null)
    {
        if (!value.Equals(valueTwo))
        {
            throw new ArgumentException(message.OrIfEmpty("Value cannot be null or empty."), paramName);
        }
    }

    internal static T NotNull<T>(T value, string paramName) where T : class
    {
        return value ?? throw new ArgumentNullException(paramName);
    }

    internal static void NotEmpty(Guid value, string paramName)
    {
        if (value == Guid.Empty)
        {
            throw new ArgumentException($"Guid cannot be empty for {paramName}");
        }
    }

    internal static void NotDefault<T>(T value, string paramName) where T : struct
    {
        if (value.Equals(default(T)))
        {
            throw new ArgumentException($"Value cannot be default value for type {typeof(T)}", paramName);
        }
    }
}