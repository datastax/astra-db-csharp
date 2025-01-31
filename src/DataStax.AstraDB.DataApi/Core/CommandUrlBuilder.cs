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

using DataStax.AstraDB.DataApi.Utils;

namespace DataStax.AstraDB.DataApi.Core;

abstract class CommandUrlBuilder
{
    internal abstract string BuildUrl();
}

internal class DatabaseCommandUrlBuilder : CommandUrlBuilder
{

    private readonly Database _database;
    private readonly CommandOptions[] _optionsTree;
    private readonly string _urlPostfix;

    //TODO: refactor once we get more usages
    internal DatabaseCommandUrlBuilder(Database database, CommandOptions[] optionsTree, string urlPostfix)
    {
        _database = database;
        _optionsTree = optionsTree;
        _urlPostfix = urlPostfix;
    }

    internal override string BuildUrl()
    {
        var options = CommandOptions.Merge(_optionsTree);
        //TODO: Is this how we want to get the keyspace? (I think not...)
        //TODO: factor in environment
        var url = $"{_database.ApiEndpoint}/api/json/{options.ApiVersion.Value.ToUrlString()}" +
            $"/{_database.DatabaseOptions.CurrentKeyspace}/{_urlPostfix}";
        return url;

        //  ADMIN URL https://api.astra.datastax.com/v2/databases/DB_ID
        //  DB URL: https://1ae8dd5d-19ce-452d-9df8-6e5b78b82ca7-us-east1.apps.astra.datastax.com

        //ASTRA_DB_API_ENDPOINT/api/json/v1/ASTRA_DB_KEYSPACE
        //"https://1ae8dd5d-19ce-452d-9df8-6e5b78b82ca7-us-east1.apps.astra.datastax.com"
        // PROD("https://api.astra.datastax.com/v2",
        //     ".apps.astra.datastax.com",
        //     ".api.streaming.datastax.com"),

        // /** Development Environment. */
        // DEV("https://api.dev.cloud.datastax.com/v2",
        //         ".apps.astra-dev.datastax.com",
        //         ".api.dev.streaming.datastax.com"),

        // /** Test Environment. */
        // TEST("https://api.test.cloud.datastax.com/v2",
        //         ".apps.astra-test.datastax.com",
        //         ".api.staging.streaming.datastax.com");
    }
}

internal class AdminCommandUrlBuilder : CommandUrlBuilder
{
    private readonly CommandOptions[] _optionsTree;
    private readonly string _urlPostfix;

    //TODO: refactor once we get more usages
    internal AdminCommandUrlBuilder(CommandOptions[] optionsTree, string urlPostfix)
    {
        _optionsTree = optionsTree;
        _urlPostfix = urlPostfix;
    }

    internal AdminCommandUrlBuilder(CommandOptions[] optionsTree) : this(optionsTree, null)
    {

    }

    internal override string BuildUrl()
    {
        var options = CommandOptions.Merge(_optionsTree);

        string url = null;
        switch (options.Environment)
        {
            case DBEnvironment.Production:
                url = "https://api.astra.datastax.com/v2";
                break;
            case DBEnvironment.Dev:
                url = "https://api.dev.cloud.datastax.com/v2";
                break;
            case DBEnvironment.Test:
                url = "https://api.test.cloud.datastax.com/v2";
                break;
        }
        if (!string.IsNullOrEmpty(_urlPostfix))
        {
            url += "/" + _urlPostfix;
        }
        return url;
    }
}