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

using DataStax.AstraDB.DataApi.Admin;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Commands;
using Xunit;

namespace DataStax.AstraDB.DataApi.UnitTests;


public class PCUGroupDeserializationTests
{
    [Fact]
    public void FullPCUGroupDeserializationTest()
    {
        var json = """
            [
                {
                    "uuid": "9aaddbb5-4622-447c-9bc5-b036bb9d7ed8",
                    "orgId": "org-id",
                    "title": "some title",
                    "cloudProvider": "AWS",
                    "region": "us-east-1",
                    "instanceType": "standard",
                    "pcuType": {
                        "type": "standard",
                        "region": "eu-west-1",
                        "provider": "aws",
                        "details": {
                            "vCPU": 0,
                            "memory": "mstring",
                            "disk_cache": "dcstring"
                        }
                    },
                    "provisionType": "shared",
                    "min": 1,
                    "max": 1,
                    "reserved": 1,
                    "description": "some description",
                    "createdAt": "2021-06-01T12:00:00.000Z",
                    "updatedAt": "2021-06-01T12:00:00.000Z",
                    "createdBy": "user",
                    "updatedBy": "user",
                    "status": "INITIALIZING"
                }
            ]
        """;

        // same deserialization setup as in the devops for AstraDatabasesAdmin:
        var devOpsOptions = new CommandOptions { SerializeDateAsDollarDate = false };
        var command = new Command("test", new DataAPIClient(), new[] { devOpsOptions }, null);

        var groups = command.Deserialize<List<PCUGroup>>(json);
        Assert.Single(groups);

        var group = groups[0];
        Assert.IsType<PCUGroup>(group);
        Assert.Equal("org-id", group.OrgId);
        Assert.Equal(1, group.Min);
        Assert.Equal(new DateTime(2021, 6, 1, 12, 0, 0, DateTimeKind.Utc), group.CreatedAt);
        Assert.Equal(CloudProviderType.AWS, group.CloudProvider);

        var pcutype = group.PCUType;
        Assert.IsType<PCUType>(pcutype);
        Assert.Equal("eu-west-1", pcutype.Region);
        Assert.Equal(CloudProviderType.AWS, pcutype.CloudProvider);

        var details = pcutype.Details;
        Assert.IsType<PCUTypeDetails>(details);
        Assert.Equal(0, details.VCpu);
        Assert.Equal("mstring", details.Memory);
    }

    [Fact]
    public void IncompleteNoDetailsPCUGroupDeserializationTest()
    {
        var json = """
            [
                {
                    "uuid": "9aaddbb5-4622-447c-9bc5-b036bb9d7ed8",
                    "cloudProvider": "AWS",
                    "region": "us-east-1",
                    "pcuType": {
                        "type": "standard",
                        "region": "eu-west-1",
                        "provider": "aws"
                    }
                }
            ]
        """;

        // same deserialization setup as in the devops for AstraDatabasesAdmin:
        var devOpsOptions = new CommandOptions { SerializeDateAsDollarDate = false };
        var command = new Command("test", new DataAPIClient(), new[] { devOpsOptions }, null);

        var groups = command.Deserialize<List<PCUGroup>>(json);
        Assert.Single(groups);

        var pcutype = groups[0].PCUType;
        Assert.Null(pcutype.Details);
    }

    [Fact]
    public void IncompleteNoTypePCUGroupDeserializationTest()
    {
        var json = """
            [
                {
                    "uuid": "9aaddbb5-4622-447c-9bc5-b036bb9d7ed8",
                    "cloudProvider": "AWS",
                    "region": "us-east-1"
                }
            ]
        """;

        // same deserialization setup as in the devops for AstraDatabasesAdmin:
        var devOpsOptions = new CommandOptions { SerializeDateAsDollarDate = false };
        var command = new Command("test", new DataAPIClient(), new[] { devOpsOptions }, null);

        var groups = command.Deserialize<List<PCUGroup>>(json);
        Assert.Single(groups);

        var pcutype = groups[0].PCUType;
        Assert.Null(pcutype);
    }
}
