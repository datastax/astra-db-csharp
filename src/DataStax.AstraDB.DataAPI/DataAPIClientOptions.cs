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

using DataStax.AstraDB.DataAPI.Core;
using DataStax.AstraDB.DataAPI.Utils;

namespace DataStax.AstraDB.DataAPI;

public class DataAPIClientOptions
{
    private Dictionary<string, string> _additionalHeaders = new();
    private DataAPIDestination _destination = DataAPIDestination.ASTRA;
    private HttpClientOptions _httpClientOptions = new();
    private TimeoutOptions _timeoutOptions = new();
    private ApiVersion _apiVersion = ApiVersion.V1;
    private DBEnvironment _environment = DBEnvironment.Production;
    private RunMode _runMode = RunMode.Normal;

    internal DBEnvironment Environment
    {
        get => _environment;
        set => _environment = value;
    }

    internal RunMode RunMode
    {
        get => _runMode;
        set => _runMode = value;
    }

    public Dictionary<string, string> AdditionalHeaders
    {
        get => _additionalHeaders;
        set
        {
            Guard.NotNull(value, nameof(value));
            _additionalHeaders = value;
        }
    }

    public DataAPIDestination Destination
    {
        get => _destination;
        set => _destination = value;
    }

    public HttpClientOptions HttpClientOptions
    {
        get => _httpClientOptions;
        set
        {
            Guard.NotNull(value, nameof(value));
            _httpClientOptions = value;
        }
    }

    public TimeoutOptions TimeoutOptions
    {
        get => _timeoutOptions;
        set
        {
            Guard.NotNull(value, nameof(value));
            _timeoutOptions = value;
        }
    }

    public ApiVersion ApiVersion
    {
        get => _apiVersion;
        set => _apiVersion = value;
    }
}
