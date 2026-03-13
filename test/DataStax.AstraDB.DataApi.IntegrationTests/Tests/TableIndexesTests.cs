using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
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
    public async Task CreateIndexTests_NoOptions()
    {
        var tableName = "tableIndexesTest_NoOptions";
        try
        {
            var table = await fixture.Database.CreateTableAsync<RowEventByDay>(tableName);

            // first creation (should succeed)
            await table.CreateIndexAsync("category_idx", (b) => b.Category);

            // second creation (should fail)
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                table.CreateIndexAsync("category_idx", (b) => b.Category));

            Assert.Contains("already exists", ex.Message);

            // second creation (should not fail when IfNotExists is set)
            await table.CreateIndexAsync("category_idx", (b) => b.Category, new CreateIndexCommandOptions()
            {
                IfNotExists = true
            });

            var result = await table.ListIndexesAsync();
            Assert.Contains(result.Indexes, i => i.Name == "category_idx");

            var insertResult = await TableIndexesFixture.AddTableRows(table);
            Assert.Equal(3, insertResult.InsertedCount);
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task CreateIndexTests_WithOptions()
    {
        var tableName = "tableIndexesTest_NoOptions";
        try
        {
            var table = await fixture.Database.CreateTableAsync<RowEventByDay>(tableName);

            // first creation (should succeed)
            await table.CreateIndexAsync("category_idx", (b) => b.Category,
                new TableIndexDefinition() { Ascii = true,  CaseSensitive = true, Normalize = true }
            );

            // second creation (should fail)
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                table.CreateIndexAsync("category_idx", (b) => b.Category));

            Assert.Contains("already exists", ex.Message);

            // second creation (should not fail when IfNotExists is set)
            await table.CreateIndexAsync("category_idx", (b) => b.Category, new CreateIndexCommandOptions()
            {
                IfNotExists = true
            });

            var result = await table.ListIndexesAsync();
            Assert.Contains(result.Indexes, i => i.Name == "category_idx");

            var insertResult = await TableIndexesFixture.AddTableRows(table);
            Assert.Equal(3, insertResult.InsertedCount);
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task CreateIndexTests_MapIndex_Entries()
    {
        var tableName = "tableIndexesTest_MapIndex_Entries";
        try
        {
            var table = await fixture.Database.CreateTableAsync<TableMapTest>(tableName);

            await table.CreateIndexAsync("map_idx", (b) => b.StringMap, Builders.TableIndex.Map(MapIndexType.Entries));

            var result = await table.ListIndexesAsync();
            Assert.Contains(result.Indexes, i => i.Name == "map_idx");

        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task CreateIndexTests_MapIndex_Keys()
    {
        var tableName = "tableIndexesTest_MapIndex_Keys";
        try
        {
            var table = await fixture.Database.CreateTableAsync<TableMapTest>(tableName);

            await table.CreateIndexAsync("map_idx", (b) => b.StringMap, Builders.TableIndex.Map(MapIndexType.Keys));

            var result = await table.ListIndexesAsync();
            Assert.Contains(result.Indexes, i => i.Name == "map_idx");

        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task CreateIndexTests_MapIndex_Values()
    {
        var tableName = "tableIndexesTest_MapIndex_Values";
        try
        {
            var table = await fixture.Database.CreateTableAsync<TableMapTest>(tableName);

            await table.CreateIndexAsync("map_idx", (b) => b.StringMap, Builders.TableIndex.Map(MapIndexType.Values));

            var result = await table.ListIndexesAsync();
            Assert.Contains(result.Indexes, i => i.Name == "map_idx");

        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task DropIndexTests()
    {
        var tableName = "dropIndexTest";
        try
        {
            var table = await fixture.Database.CreateTableAsync<RowEventByDay>(tableName);

            var indexName = "drop_idx";
            await table.CreateIndexAsync(indexName, (b) => b.Location);

            // drop should work
            await fixture.Database.DropTableIndexAsync(indexName);
            var result = await table.ListIndexesAsync();
            Assert.DoesNotContain(result.Indexes, i => i.Name == indexName);

            // second drop (should fail)
            var ex = await Assert.ThrowsAsync<CommandException>(() =>
                fixture.Database.DropTableIndexAsync(indexName));

            // second drop (should not fail when IfNotExists is set)
            await fixture.Database.DropTableIndexAsync(indexName, new DropIndexCommandOptions()
            {
                IfExists = true
            });
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }

    }

}
