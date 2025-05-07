using DataStax.AstraDB.DataApi.Tables;
using System.Data;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

[Collection("TableIndexes")]
public class TableIndexesTests
{
    private readonly TableIndexesFixture fixture;

    public TableIndexesTests(TableIndexesFixture fixture)
    {
        this.fixture = fixture;
    }

    [Fact]
    public async Task FixtureTableExists()
    {
        var tableInfos = await fixture.Database.ListTablesAsync();
        Assert.Contains("tableIndexesTest", tableInfos.Select(t => t.Name));
    }

    [Fact]
    public async Task ListIndexMetadata()
    {
        var table = fixture.Database.GetTable<RowEventByDay>("tableIndexesTest", null);
        await table.ListIndexMetadataAsync(null, false);
    }

    [Fact]
    public async Task CreateIndexOnCategoryColumn()
    {
        var table = fixture.Database.GetTable<RowEventByDay>("tableIndexesTest", null);

        var indexOptions = new TableIndexOptions
        {
            Name = "category_idx",
            Column = "category",
            Options = new IndexOptionFlags
            {
                Normalize = true,
                CaseSensitive = false
            }
        };

        await table.CreateIndexAsync(indexOptions, null, runSynchronously: false);

        var result = await table.ListIndexMetadataAsync(null, false);
        Assert.Contains(result.Indexes, i => i.Name == "category_idx");
    }

    [Fact]
    public async Task CreateIndexThrowsOnExtantIndex()
    {
        var table = fixture.Database.GetTable<RowEventByDay>("tableIndexesTest", null);

        var indexOptions = new TableIndexOptions
        {
            Name = "category_idx",
            Column = "category",
            Options = new IndexOptionFlags
            {
                Normalize = true,
                CaseSensitive = false
            }
        };

        // first creation (should succeed)
        await table.CreateIndexAsync(indexOptions);

        // second creation (should fail)
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            table.CreateIndexAsync(indexOptions));

        Assert.Contains("already exists", ex.Message);
    }
}
