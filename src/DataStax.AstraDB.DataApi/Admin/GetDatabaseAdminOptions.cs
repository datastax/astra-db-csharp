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

namespace DataStax.AstraDB.DataApi.Admin;

/// <summary>
/// Options used for AstraDatabasesAdmin's GetDatabaseAdmin methods.
/// </summary>
public class GetDatabaseAdminOptions : CommandOptions
{

    static internal GetDatabaseAdminOptions FromCommandOptions(CommandOptions options)
    {
        if (options == null) {
            return null;
        }
        // Until actual new fields are added w.r.t. CommandOptions, this is it:
        return new GetDatabaseAdminOptions
        {
            Environment = options.Environment,
            Keyspace = options.Keyspace,
            InputConverter = options.InputConverter,
            OutputConverter = options.OutputConverter,
            SerializeGuidAsDollarUuid = options.SerializeGuidAsDollarUuid,
            SerializeDateAsDollarDate = options.SerializeDateAsDollarDate,
            SerializeIEEE754SpecialValues = options.SerializeIEEE754SpecialValues,
            DeserializeToObjectDictionary = options.DeserializeToObjectDictionary,
            Token = options.Token,
            RunMode = options.RunMode,
            Destination = options.Destination,
            HttpClientOptions = options.HttpClientOptions,
            APICallers = options.APICallers,
            TimeoutOptions = options.TimeoutOptions,
            APIVersion = options.APIVersion,
            CancellationToken = options.CancellationToken,
            BulkOperationCancellationToken = options.BulkOperationCancellationToken,
            IncludeKeyspaceInUrl = options.IncludeKeyspaceInUrl,
            AdditionalHeaders = options.AdditionalHeaders
        };
    }
}
