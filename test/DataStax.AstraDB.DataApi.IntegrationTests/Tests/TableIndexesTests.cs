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
            string indexName = "category_idx";

            // first creation (should succeed)
            await table.CreateIndexAsync(indexName, (b) => b.Category);

            // second creation (should fail)
            var ex = await Assert.ThrowsAsync<CommandException>(() =>
                table.CreateIndexAsync(indexName, (b) => b.Category));

            Assert.Contains("already exists", ex.Message);

            // third creation (should fail when IfNotExists is false)
            var ex2 = await Assert.ThrowsAsync<CommandException>(() =>
                table.CreateIndexAsync(indexName, (b) => b.Category,
                    new CreateIndexCommandOptions(){IfNotExists = false}));

            Assert.Contains("already exists", ex2.Message);

            // fourth creation (should not fail when IfNotExists is true)
            await table.CreateIndexAsync(indexName, (b) => b.Category, new CreateIndexCommandOptions()
            {
                IfNotExists = true
            });

            var result = await table.ListIndexesAsync();
            Assert.Contains(result.Indexes, i => i.Name == indexName);

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
        var tableName = "tableIndexesTest_WithOptions";
        try
        {
            var table = await fixture.Database.CreateTableAsync<RowEventByDay>(tableName);
            string indexName = "category_idx";
            var indexDefinition = new TableIndexDefinition() { Ascii = true,  CaseSensitive = true, Normalize = true };

            // first creation (should succeed)
            await table.CreateIndexAsync(indexName, (b) => b.Category, indexDefinition);

            // second creation (should fail)
            var ex = await Assert.ThrowsAsync<CommandException>(() =>
                table.CreateIndexAsync(indexName, (b) => b.Category, indexDefinition));

            Assert.Contains("already exists", ex.Message);

            // third creation (should not fail when IfNotExists is set)
            await table.CreateIndexAsync(indexName, (b) => b.Category, indexDefinition, new CreateIndexCommandOptions()
            {
                IfNotExists = true
            });

            var result = await table.ListIndexesAsync();
            Assert.Contains(result.Indexes, i => i.Name == indexName);

            var insertResult = await TableIndexesFixture.AddTableRows(table);
            Assert.Equal(3, insertResult.InsertedCount);
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task CreateIndexTests_MapIndex_EntriesByDefault()
    {
        var tableName = "tableIndexesTest_MapIndex_EntriesByDefault";
        string indexName = "map_e_def_idx";

        try
        {
            var table = await fixture.Database.CreateTableAsync<TableMapTest>(tableName);

            await table.CreateIndexAsync(indexName, (b) => b.StringMap);

            var result = await table.ListIndexesAsync();
            Assert.Contains(result.Indexes, i => i.Name == indexName);

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
        string indexName = "map_e_idx";

        try
        {
            var table = await fixture.Database.CreateTableAsync<TableMapTest>(tableName);

            await table.CreateIndexAsync(indexName, (b) => b.StringMap, Builders.TableIndex.Map(MapIndexType.Entries));

            var result = await table.ListIndexesAsync();
            Assert.Contains(result.Indexes, i => i.Name == indexName);

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
        string indexName = "map_k_idx";

        try
        {
            var table = await fixture.Database.CreateTableAsync<TableMapTest>(tableName);

            await table.CreateIndexAsync(indexName, (b) => b.StringMap, Builders.TableIndex.Map(MapIndexType.Keys));

            var result = await table.ListIndexesAsync();
            Assert.Contains(result.Indexes, i => i.Name == indexName);

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
        string indexName = "map_v_idx";

        try
        {
            var table = await fixture.Database.CreateTableAsync<TableMapTest>(tableName);

            await table.CreateIndexAsync(indexName, (b) => b.StringMap, Builders.TableIndex.Map(MapIndexType.Values));

            var result = await table.ListIndexesAsync();
            Assert.Contains(result.Indexes, i => i.Name == indexName);

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

            Assert.Contains("attempted to drop", ex.Message);

            // third drop (should fail when IfNotExists is false)
            var ex2 = await Assert.ThrowsAsync<CommandException>(() =>
                fixture.Database.DropTableIndexAsync(indexName,
                    new DropIndexCommandOptions(){IfExists = false}));

            Assert.Contains("attempted to drop", ex2.Message);

            // fourth drop (should not fail when IfNotExists is true)
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
