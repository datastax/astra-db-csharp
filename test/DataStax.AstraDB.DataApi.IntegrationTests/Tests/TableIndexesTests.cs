using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core.Commands;
using DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;
using DataStax.AstraDB.DataApi.Tables;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

[Collection("TableIndexes")]
public class TableIndexesTests
{
    private readonly TableIndexesFixture fixture;

    public TableIndexesTests(AssemblyFixture assemblyFixture, TableIndexesFixture fixture)
    {
        this.fixture = fixture;
    }

    [Fact]
    public async Task CreateIndexTests()
    {
        var table = fixture.Database.GetTable<RowEventByDay>("tableIndexesTest");

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

    [Fact]
    public async Task DropIndexTests()
    {
        var table = fixture.Database.GetTable<RowEventByDay>("tableIndexesTest");

        var indexName = "drop_idx";
        var indexOptions = new TableIndex
        {
            IndexName = indexName,
            Definition = new TableIndexDefinition<RowEventByDay, string>()
            {
                Column = (b) => b.Location
            }
        };
        await table.CreateIndexAsync(indexOptions);

        // drop should work
        await fixture.Database.DropTableIndexAsync(indexName);
        var result = await table.ListIndexMetadataAsync();
        Assert.DoesNotContain(result.Indexes, i => i.Name == indexName);

        // second drop (should fail)
        var ex = await Assert.ThrowsAsync<CommandException>(() =>
            fixture.Database.DropTableIndexAsync(indexName));

        // second drop (should not fail when SkipIfExists is set)
        await fixture.Database.DropTableIndexAsync(indexName, new DropIndexCommandOptions()
        {
            SkipIfNotExists = true
        });


    }

}
