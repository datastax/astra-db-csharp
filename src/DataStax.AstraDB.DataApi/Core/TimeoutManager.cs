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

internal class TimeoutManager
{
    internal TimeSpan GetRequestTimeout(CommandOptions commandOptions)
    {
        if (commandOptions.TimeoutOptions?.RequestTimeout != null)
        {
            return commandOptions.TimeoutOptions.RequestTimeout.Value;
        }
        return GetDefaultRequestTimeout(commandOptions);
    }

    internal TimeSpan GetConnectionTimeout(CommandOptions commandOptions)
    {
        if (commandOptions.TimeoutOptions?.ConnectionTimeout != null)
        {
            return commandOptions.TimeoutOptions.ConnectionTimeout.Value;
        }
        return GetDefaultConnectionTimeout(commandOptions);
    }

    internal TimeSpan GetBulkOperationTimeout(CommandOptions commandOptions)
    {
        if (commandOptions.TimeoutOptions?.BulkOperationTimeout != null)
        {
            return commandOptions.TimeoutOptions.BulkOperationTimeout.Value;
        }
        return GetDefaultBulkOperationTimeout(commandOptions);
    }

    virtual internal TimeSpan GetDefaultRequestTimeout(CommandOptions commandOptions)
    {
        return commandOptions.TimeoutOptions.Defaults.RequestTimeout;
    }

    virtual internal TimeSpan GetDefaultConnectionTimeout(CommandOptions commandOptions)
    {
        return commandOptions.TimeoutOptions.Defaults.ConnectionTimeout;
    }

    virtual internal TimeSpan GetDefaultBulkOperationTimeout(CommandOptions commandOptions)
    {
        return commandOptions.TimeoutOptions.Defaults.BulkOperationTimeout;
    }
}

internal class CollectionAdminTimeoutManager : TimeoutManager
{
    override internal TimeSpan GetDefaultRequestTimeout(CommandOptions commandOptions)
    {
        return commandOptions.TimeoutOptions.Defaults.CollectionAdminTimeout;
    }
}

internal class TableAdminTimeoutManager : TimeoutManager
{
    override internal TimeSpan GetDefaultRequestTimeout(CommandOptions commandOptions)
    {
        return commandOptions.TimeoutOptions.Defaults.TableAdminTimeout;
    }
}

internal class DatabaseAdminTimeoutManager : TimeoutManager
{
    override internal TimeSpan GetDefaultRequestTimeout(CommandOptions commandOptions)
    {
        return commandOptions.TimeoutOptions.Defaults.DatabaseAdminTimeout;
    }
}

internal class KeyspaceAdminTimeoutManager : TimeoutManager
{
    override internal TimeSpan GetDefaultRequestTimeout(CommandOptions commandOptions)
    {
        return commandOptions.TimeoutOptions.Defaults.KeyspaceAdminTimeout;
    }
}