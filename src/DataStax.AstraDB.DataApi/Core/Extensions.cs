
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

using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Core;

public static class CoreExtensions
{
    public static string ToUrlString(this ApiVersion apiVersion)
    {
        return apiVersion switch
        {
            ApiVersion.V1 => "v1",
            _ => "v1",
        };
    }

    public static TResult ResultSync<TResult>(this Task<TResult> task)
    {
        return task.GetAwaiter().GetResult();
    }

    public static void ResultSync(this Task task)
    {
        task.GetAwaiter().GetResult();
    }
}