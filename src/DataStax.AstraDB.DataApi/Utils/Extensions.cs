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

<<<<<<<< HEAD:src/DataStax.AstraDB.DataApi/Utils/Extensions.cs
using System.Collections.Generic;

namespace DataStax.AstraDB.DataApi.Utils;
========
using DataStax.AstraDB.DataApi.Utils;

namespace DataStax.AstraDB.DataApi.Core;
>>>>>>>> 2334bb53b9027422e19c36f2165408c636ddc279:src/DataStax.AstraDB.DataApi/Core/DatabaseOptions.cs

public static class Extensions
{
<<<<<<<< HEAD:src/DataStax.AstraDB.DataApi/Utils/Extensions.cs
    internal static string OrIfEmpty(this string str, string alternate)
========
    //TODO: move to common area for defaults if used other than directly related to DatabaseOptions.
    public const string DefaultKeyspace = "default_keyspace";

    private string _currentKeyspace;

    public string CurrentKeyspace
>>>>>>>> 2334bb53b9027422e19c36f2165408c636ddc279:src/DataStax.AstraDB.DataApi/Core/DatabaseOptions.cs
    {
        return string.IsNullOrEmpty(str) ? alternate : str;
    }

    internal static T Merge<T>(this IEnumerable<T> list)
    {
        T result = default(T);
        foreach (T item in list)
        {
            result = EqualityComparer<T>.Default.Equals(item, default) ? result : item;
        }
        return result;
    }
}