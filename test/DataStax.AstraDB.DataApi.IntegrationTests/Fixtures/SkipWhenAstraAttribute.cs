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

using Microsoft.Extensions.Configuration;
using System.Reflection;
using Xunit.v3;

namespace DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;

[AttributeUsage(AttributeTargets.Method | AttributeTargets.Class)]
public class SkipWhenAstraAttribute : BeforeAfterTestAttribute
{
    private static readonly Lazy<string> _destination = new(() =>
    {
        var config = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables(prefix: "ASTRA_DB_")
            .Build();
        return (config["DESTINATION"] ?? config["AstraDB:Destination"])?.ToLower();
    });

    public override void Before(MethodInfo methodUnderTest, IXunitTest test)
    {
        var destination = _destination.Value;
        if (string.IsNullOrEmpty(destination) || destination == "astra")
            throw new Exception($"{DynamicSkipToken.Value}Requires non-Astra destination (current: '{destination}')");
    }
}