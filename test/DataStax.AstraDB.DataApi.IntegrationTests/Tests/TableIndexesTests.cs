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
    public async Task CreateIndexTests()
    {
        var table = fixture.Database.GetTable<RowEventByDay>("tableIndexesTest", null);

        var indexOptions = new TableIndex
        {
            IndexName = "category_idx",
            Definition = new TableIndexDefinition<RowEventByDay, string>()
            {
                Column = (b) => b.Category,
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

        // second creation (should not fail when SkipIfExists is set)
        await table.CreateIndexAsync(indexOptions, new CreateIndexCommandOptions()
        {
            SkipIfExists = true
        });

        var result = await table.ListIndexMetadataAsync();
        Assert.Contains(result.Indexes, i => i.Name == "category_idx");
    }

}
