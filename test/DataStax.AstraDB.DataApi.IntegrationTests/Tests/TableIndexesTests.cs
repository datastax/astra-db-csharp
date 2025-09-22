using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core.Commands;
using DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;
using DataStax.AstraDB.DataApi.Tables;
using System.Runtime.CompilerServices;
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
    public async Task CreateIndexTests_GeneratedIndexNames()
    {
        var tableName = "tableIndexesTest";
        try
        {
            var table = await fixture.Database.CreateTableAsync<RowEventByDay>(tableName);

            // first creation (should succeed)
            await table.CreateIndexAsync((b) => b.Category);

            // second creation (should fail)
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                table.CreateIndexAsync((b) => b.Category));

            Assert.Contains("already exists", ex.Message);

            // second creation (should not fail when SkipIfExists is set)
            await table.CreateIndexAsync((b) => b.Category, new CreateIndexCommandOptions()
            {
                SkipIfExists = true
            });

            var result = await table.ListIndexMetadataAsync();
            Assert.Contains(result.Indexes, i => i.Name == "category_idx");

            //ensure insert still works
            var insertResult = await TableIndexesFixture.AddTableRows(table);
            Assert.Equal(3, insertResult.InsertedCount);

        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task CreateIndexTests_NamedIndex()
    {
        var tableName = "tableIndexesTest_NamedIndex";
        try
        {
            var table = await fixture.Database.CreateTableAsync<RowEventByDay>(tableName);

            // first creation (should succeed)
            await table.CreateIndexAsync("category_idx", (b) => b.Category);

            // second creation (should fail)
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                table.CreateIndexAsync("category_idx", (b) => b.Category));

            Assert.Contains("already exists", ex.Message);

            // second creation (should not fail when SkipIfExists is set)
            await table.CreateIndexAsync("category_idx", (b) => b.Category, new CreateIndexCommandOptions()
            {
                SkipIfExists = true
            });

            var result = await table.ListIndexMetadataAsync();
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
    public async Task DropIndexTests_NamedIndex()
    {
        var tableName = "dropIndexTest";
        try
        {
            var table = await fixture.Database.CreateTableAsync<RowEventByDay>(tableName);

            var indexName = "drop_idx";

            await table.CreateIndexAsync(indexName, (b) => b.Location);

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
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }

    }

    [Fact]
    public async Task DropIndexTests_GeneratedIndexNames()
    {
        var tableName = "dropIndexTests_GeneratedIndexNames";
        try
        {
            var table = await fixture.Database.CreateTableAsync<RowEventByDay>(tableName);

            var indexName = "Location_idx";
            await table.CreateIndexAsync(indexName, (b) => b.Location);

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
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }

    }

}
