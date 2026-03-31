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
        return commandOptions.TimeoutOptions?.RequestTimeout ?? GetDefaultRequestTimeout(commandOptions);
    }

    internal TimeSpan GetConnectionTimeout(CommandOptions commandOptions)
    {
        return commandOptions.TimeoutOptions?.ConnectionTimeout ?? TimeoutOptions.DefaultConnectionTimeout;
    }

    internal TimeSpan GetBulkOperationTimeout(CommandOptions commandOptions)
    {
        return commandOptions.TimeoutOptions?.BulkOperationTimeout ?? TimeoutOptions.DefaultBulkOperationTimeout;
    }

    virtual internal TimeSpan GetDefaultRequestTimeout(CommandOptions commandOptions)
    {
        return TimeoutOptions.DefaultRequestTimeout;
    }
}

internal class CollectionAdminTimeoutManager : TimeoutManager
{
    override internal TimeSpan GetDefaultRequestTimeout(CommandOptions commandOptions)
    {
        return commandOptions.TimeoutOptions?.CollectionAdminTimeout ?? TimeoutOptions.DefaultCollectionAdminTimeout;
    }
}

internal class TableAdminTimeoutManager : TimeoutManager
{
    override internal TimeSpan GetDefaultRequestTimeout(CommandOptions commandOptions)
    {
        return commandOptions.TimeoutOptions?.TableAdminTimeout ?? TimeoutOptions.DefaultTableAdminTimeout;
    }
}

internal class DatabaseAdminTimeoutManager : TimeoutManager
{
    override internal TimeSpan GetDefaultRequestTimeout(CommandOptions commandOptions)
    {
        return commandOptions.TimeoutOptions?.DatabaseAdminTimeout ?? TimeoutOptions.DefaultDatabaseAdminTimeout;
    }
}

internal class KeyspaceAdminTimeoutManager : TimeoutManager
{
    override internal TimeSpan GetDefaultRequestTimeout(CommandOptions commandOptions)
    {
        return commandOptions.TimeoutOptions?.KeyspaceAdminTimeout ?? TimeoutOptions.DefaultKeyspaceAdminTimeout;
    }
}
