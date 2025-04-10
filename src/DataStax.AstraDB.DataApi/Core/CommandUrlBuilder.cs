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

using System.Linq;

namespace DataStax.AstraDB.DataApi.Core;

abstract class CommandUrlBuilder
{
    internal abstract string BuildUrl(CommandOptions options);
}

internal class DatabaseCommandUrlBuilder : CommandUrlBuilder
{

    private readonly Database _database;
    private readonly string _urlPostfix;

    internal DatabaseCommandUrlBuilder(Database database, string urlPostfix)
    {
        _database = database;
        _urlPostfix = urlPostfix;
    }

    internal override string BuildUrl(CommandOptions options)
    {
        var url = $"{_database.ApiEndpoint}/api/json/{options.ApiVersion.Value.ToUrlString()}" +
            $"/{options.Keyspace}/{_urlPostfix}";
        return url;
    }
}

internal class AdminCommandUrlBuilder : CommandUrlBuilder
{
    internal override string BuildUrl(CommandOptions options)
    {
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
        if (options.IncludeKeyspaceInUrl && !string.IsNullOrEmpty(options.Keyspace))
        {
            url = CombineUrlParts(url, options.Keyspace);
        }
        return url;
    }

    protected string CombineUrlParts(string baseUrl, params string[] parts)
    {
        var trimmedBaseUrl = baseUrl.TrimEnd('/');
        var combinedUrl = string.Join("/", parts.Select(part => part?.Trim('/')));
        return $"{trimmedBaseUrl}/{combinedUrl}";
    }
}

internal class EmbeddingCommandUrlBuilder : CommandUrlBuilder
{

    private readonly Database _database;

    internal EmbeddingCommandUrlBuilder(Database database)
    {
        _database = database;
    }

    internal override string BuildUrl(CommandOptions options)
    {
        var url = $"{_database.ApiEndpoint}/api/json/{options.ApiVersion.Value.ToUrlString()}";
        return url;
    }
}