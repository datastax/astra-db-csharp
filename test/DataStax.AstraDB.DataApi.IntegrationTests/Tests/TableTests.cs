

using DataStax.AstraDB.DataApi.Tables;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

[Collection("Tables")]
public class TableTests
{
    TablesFixture fixture;

    public TableTests(TablesFixture fixture)
    {
        this.fixture = fixture;
    }

    [Fact]
    public async Task InsertRows()
    {
        var table = await fixture.Database.CreateTableAsync<RowBook>("insertRowsTest");
        var row1 = new RowBook()
        {
            Title = "Computed Wilderness",
            Author = "Ryan Eau",
            NumberOfPages = 432,
            DueDate = DateTime.Now - TimeSpan.FromDays(1),
            Genres = new HashSet<string> { "History", "Biography" }
        };
        var row2 = new RowBook()
        {
            Title = "Desert Peace",
            Author = "Walter Dray",
            NumberOfPages = 355,
            DueDate = DateTime.Now - TimeSpan.FromDays(2),
            Genres = new HashSet<string> { "Fiction" }
        };
        var rows = new List<RowBook> { row1, row2 };
        var result = await table.InsertManyAsync(rows);
        Assert.Equal(rows.Count, result.InsertedCount);
        // Assert.Equal(rows[0].Title, result.PrimaryKeys[0].Title);
        // Assert.Equal(rows[1].Title, result.PrimaryKeys[1].Title);
    }
}

/*
ow1 = new Row()
            .addText("title", "Computed Wilderness")
            .addText("author", "Ryan Eau")
            .addInt("numberOfPages", 432)
            .addDate("dueDate", DateTime.Now - TimeSpan.FromDays(1))
                .addSet("genres", Set.of("History", "Biography"));
        Row row2 =
            new Row()
                .addText("title", "Desert Peace")
                .addText("author", "Walter Dray")
                .addInt("numberOfPages", 355)
                .add
*/