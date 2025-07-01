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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace DataStax.AstraDB.DataApi.Utils;

/// <summary>
/// Helper class for setting up bulk operation timeouts and cancellation tokens.
/// </summary>
internal static class BulkOperationHelper
{
    /// <summary>
    /// Sets up bulk operation timeout and cancellation token for the given command options.
    /// </summary>
    /// <param name="optionsTree">The tree of command options to merge.</param>
    /// <param name="commandOptions">The command options to configure with timeout.</param>
    /// <returns>A tuple containing the timeout duration and the cancellation token source.</returns>
    internal static (TimeSpan timeout, CancellationTokenSource cts) InitTimeout(
        List<CommandOptions> optionsTree,
        ref CommandOptions commandOptions)
    {
        if (commandOptions != null)
        {
            optionsTree.Add(commandOptions);
        }
        var mergedOptions = CommandOptions.Merge(optionsTree.ToArray());
        var timeout = new TimeoutManager().GetBulkOperationTimeout(mergedOptions);
        var cts = new CancellationTokenSource(timeout);
        var bulkOperationTimeoutToken = cts.Token;

        if (commandOptions == null)
        {
            commandOptions = new CommandOptions
            {
                BulkOperationCancellationToken = bulkOperationTimeoutToken
            };
        }
        else
        {
            commandOptions.BulkOperationCancellationToken = bulkOperationTimeoutToken;
        }

        return (timeout, cts);
    }

}