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
using Xunit;

namespace DataStax.AstraDB.DataApi.UnitTests;


public class APICallersTests
{
    [Fact]
    public void CallerToString()
    {
        Assert.Null(new APICaller().ToString());
        Assert.Equal("n", new APICaller(){ Name = "n" }.ToString());
        Assert.Equal("v", new APICaller(){ Version = "v" }.ToString());
        Assert.Equal("n/v", new APICaller(){ Name = "n", Version = "v" }.ToString());
    }

    [Fact]
    public void CallersToString()
    {
        var callers = new List<APICaller>();

        callers.Add(new APICaller(){});
        callers.Add(new APICaller(){ Name = "n2" });
        callers.Add(new APICaller(){ Version = "v3" });
        callers.Add(new APICaller(){});
        callers.Add(new APICaller(){ Name = "n5", Version = "v5" });

        Assert.Equal(
            "n2 v3 n5/v5",
            APICaller.ToHeaderString(callers)
        );
    }
}
