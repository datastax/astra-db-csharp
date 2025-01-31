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

using System.Text.Json;

namespace DataStax.AstraDB.DataApi.Core.SerDes;

public class DatabaseSerializer : IDataSerializer
{
    private readonly JsonSerializerOptions _options;

    public DatabaseSerializer()
    {
        _options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            PropertyNameCaseInsensitive = true
        };
    }

    public string Serialize<T>(T obj)
    {
        return JsonSerializer.Serialize(obj, _options);
    }

    public T Deserialize<T>(string json)
    {
        return JsonSerializer.Deserialize<T>(json, _options);
    }

    public Dictionary<string, object> SerializeToMap<T>(T obj)
    {
        string json = Serialize(obj);
        return Deserialize<Dictionary<string, object>>(json);
    }

    public T DeserializeFromMap<T>(Dictionary<string, object> map)
    {
        string json = Serialize(map);
        return Deserialize<T>(json);
    }
}
