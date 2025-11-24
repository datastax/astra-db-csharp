using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Query;
using DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;
using DataStax.AstraDB.DataApi.Tables;
using Microsoft.VisualBasic;
using System.IO;
using System.Text.Json;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

[Collection("Database")]
public class AdditionalTableTests
{
    DatabaseFixture fixture;

    public AdditionalTableTests(AssemblyFixture assemblyFixture, DatabaseFixture fixture)
    {
        this.fixture = fixture;
    }

    [Fact]
    public async Task Test_Arrays()
    {
        var tableName = "tableFindOneWithArrays";
        try
        {
            List<ArrayTestRow> items = new List<ArrayTestRow>() {
                new()
                {
                    Id = 0,
                    StringArray = new string[] { "one", "two", "three" }
                },
                new()
                {
                    Id = 1,
                    StringArray = new string[] { "four", "five", "six" }
                },
                new()
                {
                    Id = 2,
                    StringArray = new string[] { "seven", "eight", "nine" }
                },
            };

            var table = await fixture.Database.CreateTableAsync<ArrayTestRow>(tableName);
            await table.CreateIndexAsync((b) => b.StringArray);
            var insertResult = await table.InsertManyAsync(items);
            Assert.Equal(items.Count, insertResult.InsertedIds.Count);
            var findOptions = new TableFindOptions<ArrayTestRow>()
            {
                Filter = Builders<ArrayTestRow>.Filter.In(x => x.StringArray, new string[] { "five" }),
            };

            var result = await table.FindOneAsync(findOptions);
            Assert.NotNull(result);
            Assert.Equal(1, result.Id);
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task Test_BookJsonData()
    {
        var tableName = "tableBookWithTestData";
        try
        {
            var table = await fixture.Database.CreateTableAsync<TestDataBook>(tableName);

            // Use AppContext.BaseDirectory so the test finds the file when run from the test output folder
            var dataFilePath = Path.Combine(AppContext.BaseDirectory ?? Directory.GetCurrentDirectory(), "book_test_data.json");
            // Read the JSON file and parse it into a JSON array
            string rawData = await File.ReadAllTextAsync(dataFilePath);

            var options = new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
            };
            var rows = JsonSerializer.Deserialize<List<TestDataBook>>(rawData, options);
            foreach (var row in rows)
            {
                row.SummaryGenresVector =
                  $"summary: {row.Summary ?? ""} | genres: {string.Join(", ", row.Genres)}";
                row.DueDate = row.DueDate == null ? null : DateTime.SpecifyKind(row.DueDate.Value, DateTimeKind.Utc);
            }

            // Insert the data
            var result = await table.InsertManyAsync(rows);

            Console.WriteLine($"Inserted {result.InsertedCount} rows");

            Assert.Equal(100, result.InsertedCount);
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task Test_DateTimeTypes()
    {
        var tableName = "tableTestDateTimeTypes";
        try
        {
            var table = await fixture.Database.CreateTableAsync<DateTypeTest>(tableName);

            List<DateTypeTest> rows = new List<DateTypeTest>();
            for (var i = 0; i < 5; i++)
            {
                rows.Add(new DateTypeTest()
                {
                    Id = i,
                    Timestamp = DateTime.SpecifyKind(DateTime.Now.AddDays(i), DateTimeKind.Unspecified),
                    Date = new DateOnly(2000, 1, i + 1),
                    Time = new TimeOnly(12, i),
                    TimestampWithKind = DateTime.SpecifyKind(DateTime.Now.AddDays(i), DateTimeKind.Local),
                });
            }
            for (var i = 5; i < 10; i++)
            {
                rows.Add(new DateTypeTest()
                {
                    Id = i,
                    Timestamp = DateTime.SpecifyKind(DateTime.Now.AddDays(i), DateTimeKind.Unspecified),
                    Date = new DateOnly(2000, 1, i + 1),
                    Time = new TimeOnly(12, i),
                    TimestampWithKind = DateTime.SpecifyKind(DateTime.Now.AddDays(i), DateTimeKind.Local),
                    MaybeDate = new DateOnly(2000, 1, i + 1),
                    MaybeTime = new TimeOnly(12, i),
                    MaybeTimestamp = DateTime.SpecifyKind(DateTime.Now.AddDays(i), DateTimeKind.Unspecified),
                });
            }

            // Insert the data
            var result = await table.InsertManyAsync(rows);

            Console.WriteLine($"Inserted {result.InsertedCount} rows");

            Assert.Equal(10, result.InsertedCount);
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex);
            throw;
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

}

