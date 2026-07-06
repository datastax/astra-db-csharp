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

namespace DataStax.AstraDB.DataApi.Tests;

public class CommandOptionsTests
{
    [Fact]
    public void MergeTest()
    {
        var one = new CommandOptions();
        var two = new CommandOptions { Environment = DBEnvironment.Production };
        var three = new CommandOptions { Environment = DBEnvironment.Test };

        var result = CommandOptions.Merge(one, two, three);
        Assert.Equal(DBEnvironment.Test, result.Environment);

        result = CommandOptions.Merge(two, three, one);
        Assert.Equal(DBEnvironment.Test, result.Environment);

        result = CommandOptions.Merge(three, one, two);
        Assert.Equal(DBEnvironment.Production, result.Environment);

        result = CommandOptions.Merge(three, two, one);
        Assert.Equal(DBEnvironment.Production, result.Environment);
    }
}