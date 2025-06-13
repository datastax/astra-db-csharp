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
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Utils;

internal class Wait
{
    internal static async Task WaitForProcess(Func<Task<bool>> process, int maxWaitInSeconds = 600)
    {
        const int SLEEP_SECONDS = 5;

        int secondsWaited = 0;

        while (secondsWaited < maxWaitInSeconds)
        {
            var done = await process().ConfigureAwait(false);
            if (done)
            {
                return;
            }
            await Task.Delay(SLEEP_SECONDS * 1000).ConfigureAwait(false);
            secondsWaited += SLEEP_SECONDS;
        }

        throw new Exception();
    }
}