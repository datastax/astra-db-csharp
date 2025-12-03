using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Query;
using DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;
using DataStax.AstraDB.DataApi.Tables;
using Microsoft.VisualBasic;
using Newtonsoft.Json;
using System.Net;
using System.Text;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

[Collection("UserDefinedTypes")]
public class UserDefinedTypesTests
{
    UserDefinedTypesFixture fixture;

    public UserDefinedTypesTests(AssemblyFixture assemblyFixture, UserDefinedTypesFixture fixture)
    {
        this.fixture = fixture;
    }

    [Fact]
    public async Task UserDefinedTypes_Test()
    {
        const string typeName = "testUserDefinedType";
        try
        {
            await fixture.Database.CreateTypeAsync(typeName, new UserDefinedTypeDefinition
            {
                Fields = new Dictionary<string, DataApiType>
                {
                    ["id"] = DataApiType.Int(),
                    ["name"] = DataApiType.Text(),
                    ["removeMe"] = DataApiType.Boolean()
                }
            });

            var types = await fixture.Database.ListTypesAsync();
            Assert.Contains(types, t => t.Name.Equals(typeName, StringComparison.OrdinalIgnoreCase));
            Assert.True(types.First(t => t.Name.Equals(typeName, StringComparison.OrdinalIgnoreCase))
                .Definition.Fields.ContainsKey("removeMe"));

            var alterDefinition = new AlterUserDefinedTypeDefinition(typeName)
                .AddField("new_field", DataApiType.Text())
                .RenameField("removeMe", "okYouCanStay");

            await fixture.Database.AlterTypeAsync(alterDefinition);
            var updatedTypes = await fixture.Database.ListTypesAsync();
            var updatedType = updatedTypes.FirstOrDefault(t => t.Name.Equals(typeName, StringComparison.OrdinalIgnoreCase));
            Assert.NotNull(updatedType);
            Assert.True(updatedType.Definition.Fields.ContainsKey("new_field"));
            Assert.True(updatedType.Definition.Fields.ContainsKey("okYouCanStay"));
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
            throw;
        }
        finally
        {
            await fixture.Database.DropTypeAsync(typeName);
        }
    }


    [Fact]
    public async Task UserDefinedTypes_CreateFromClasses_Basic()
    {
        var tableName = "userDefinedTypesFromClassesBasic";
        try
        {
            List<UdtTestMinimal> items = new List<UdtTestMinimal>() {
                new()
                {
                    Id = 0,
                    Udt = new SimpleUdtTwo
                    {
                        Name = "Test 1",
                        Number = 101
                    },
                },
                new()
                {
                    Id = 1,
                    Udt = new SimpleUdtTwo
                    {
                        Name = "Test 2",
                        Number = 102
                    },
                },
                new()
                {
                    Id = 2,
                    Udt = new SimpleUdtTwo
                    {
                        Name = "Test 3",
                        Number = 103
                    },
                },
            };

            var table = await fixture.Database.CreateTableAsync<UdtTestMinimal>(tableName);
            var insertResult = await table.InsertManyAsync(items);
            Assert.Equal(items.Count, insertResult.InsertedIds.Count);
            var filter = Builders<UdtTestMinimal>.Filter.Eq(b => b.Udt.Name, "Test 3");

            //TODO: Can you filter on UDT fields?
            // var result = await table.FindOneAsync(filter);
            // Assert.NotNull(result);
            // Assert.Equal(2, result.Id);
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
            throw;
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
            await fixture.Database.DropTypeAsync<SimpleUdtTwo>();
        }
    }

    [Fact]
    public async Task UserDefinedTypes_CreateFromClasses()
    {
        var tableName = "userDefinedTypesFromClasses";
        try
        {
            List<UdtTest> items = new List<UdtTest>() {
                new()
                {
                    Id = 0,
                    Udt = new TypesTester
                    {
                        String = "Test 1",
                        //Inet = IPAddress.Parse("192.168.0.1"),
                        Int = int.MaxValue,
                        TinyInt = byte.MaxValue,
                        SmallInt = short.MaxValue,
                        BigInt = long.MaxValue,
                        Decimal = decimal.MaxValue,
                        Double = double.MaxValue,
                        Float = float.MaxValue,
                        Boolean = false,
                        UUID = Guid.NewGuid(),
                        Duration = Duration.Parse("12y3mo1d12h30m5s12ms7us1ns"),
                        Timestamp = DateTime.Now,
                        Date = DateOnly.FromDateTime(DateTime.Now),
                        Time = TimeOnly.FromDateTime(DateTime.Now),
                        MaybeTimestamp = null,
                        MaybeDate = DateOnly.FromDateTime(DateTime.Now),
                        MaybeTime = null,
                        TimestampWithKind = DateTime.SpecifyKind(DateTime.Now, DateTimeKind.Utc),
                    },
                    UdtList = new List<SimpleUdt>
                    {
                        new SimpleUdt
                        {
                            Name = "List Test 1",
                            Number = 1001
                        },
                        new SimpleUdt
                        {
                            Name = "List Test 2",
                            Number = 1002
                        }
                    },
                },
                new()
                {
                    Id = 1,
                    Udt = new TypesTester
                    {
                        String = "Test 2",
                        //Inet = IPAddress.Parse("192.168.0.2"),
                        Int = int.MaxValue,
                        TinyInt = byte.MaxValue,
                        SmallInt = short.MaxValue,
                        BigInt = long.MaxValue,
                        Decimal = decimal.MaxValue,
                        Double = double.MaxValue,
                        Float = float.MaxValue,
                        Boolean = false,
                        UUID = Guid.NewGuid(),
                        Duration = Duration.Parse("12y3mo1d12h30m5s12ms7us2ns"),
                        Timestamp = DateTime.Now,
                        Date = DateOnly.FromDateTime(DateTime.Now),
                        Time = TimeOnly.FromDateTime(DateTime.Now),
                        MaybeTimestamp = null,
                        MaybeDate = DateOnly.FromDateTime(DateTime.Now),
                        MaybeTime = null,
                        TimestampWithKind = DateTime.SpecifyKind(DateTime.Now, DateTimeKind.Utc),
                    },
                    UdtList = new List<SimpleUdt>
                    {
                        new SimpleUdt
                        {
                            Name = "List Test 2 dot 1",
                            Number = 2001
                        },
                        new SimpleUdt
                        {
                            Name = "List Test 2 dot 2",
                            Number = 2002
                        }
                    },
                },
                new()
                {
                    Id = 2,
                    Udt = new TypesTester
                    {
                        String = "Test 3",
                        //Inet = IPAddress.Parse("192.168.0.3"),
                        Int = int.MaxValue,
                        TinyInt = byte.MaxValue,
                        SmallInt = short.MaxValue,
                        BigInt = long.MaxValue,
                        Decimal = decimal.MaxValue,
                        Double = double.MaxValue,
                        Float = float.MaxValue,
                        Boolean = false,
                        UUID = Guid.NewGuid(),
                        Duration = Duration.Parse("12y3mo1d12h30m5s12ms7us2ns"),
                        Timestamp = DateTime.Now,
                        Date = DateOnly.FromDateTime(DateTime.Now),
                        Time = TimeOnly.FromDateTime(DateTime.Now),
                        MaybeTimestamp = null,
                        MaybeDate = DateOnly.FromDateTime(DateTime.Now),
                        MaybeTime = null,
                        TimestampWithKind = DateTime.SpecifyKind(DateTime.Now, DateTimeKind.Utc),
                    },
                    UdtList = new List<SimpleUdt>
                    {
                        new SimpleUdt
                        {
                            Name = "List Test 3 dot 1",
                            Number = 3001
                        },
                        new SimpleUdt
                        {
                            Name = "List Test 3 dot 2",
                            Number = 3002
                        }
                    },
                },
            };

            var table = await fixture.Database.CreateTableAsync<UdtTest>(tableName);
            var insertResult = await table.InsertManyAsync(items);
            Assert.Equal(items.Count, insertResult.InsertedIds.Count);
            var filter = Builders<UdtTest>.Filter.Eq(b => b.Udt.String, "Test 3");

            //TODO: Can you filter on UDT fields?
            // var result = await table.FindOneAsync(filter);
            // Assert.NotNull(result);
            // Assert.Equal(2, result.Id);
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
            throw;
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
            await fixture.Database.DropTypeAsync<TypesTester>();
            await fixture.Database.DropTypeAsync<SimpleUdt>();
        }
    }

}

