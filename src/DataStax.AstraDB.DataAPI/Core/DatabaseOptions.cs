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

using DataStax.AstraDB.DataAPI.Utils;

namespace DataStax.AstraDB.DataAPI.Core;

public class DatabaseOptions
{
    public const string DefaultKeyspace = "default_keyspace";

    private string _currentKeyspace;

    public string CurrentKeyspace
    {
        get => _currentKeyspace;
        set
        {
            Guard.NotNullOrEmpty(value, nameof(value));
            _currentKeyspace = value;
        }
    }

    public DatabaseOptions()
        : this(DefaultKeyspace)
    {
    }

    public DatabaseOptions(string currentKeyspace)
    {
        _currentKeyspace = currentKeyspace;
    }
}
