using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Commands;
using DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;
using DataStax.AstraDB.DataApi.Tables;

using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

// TODO: this whole file needs a stronger verification,
// i.e. reading index metadata in each test and checking details match expectation.
// For now, part of the testing is manual payload inspection in the logs.

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
            // compare: {"createIndex":{"name":"category_idx","definition":{"column":"category"}}}

            // second creation (should fail)
            var ex = await Assert.ThrowsAsync<CommandException>(() =>
                table.CreateIndexAsync(indexName, (b) => b.Category));

            Assert.Contains("already exists", ex.Message);

            // third creation (should fail when IfNotExists is false)
            var ex2 = await Assert.ThrowsAsync<CommandException>(() =>
                table.CreateIndexAsync(indexName, (b) => b.Category,
                    new CreateIndexCommandOptions(){IfNotExists = false}));
                // compare: {"createIndex":{"name":"category_idx","definition":{"column":"category"},"options":{"ifNotExists":false}}}

            Assert.Contains("already exists", ex2.Message);

            // fourth creation (should not fail when IfNotExists is true)
            await table.CreateIndexAsync(indexName, (b) => b.Category, new CreateIndexCommandOptions()
            {
                IfNotExists = true
            });
            // compare: {"createIndex":{"name":"category_idx","definition":{"column":"category"},"options":{"ifNotExists":true}}}

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
            // compare: {"createIndex":{"name":"category_idx","definition":{"column":"category","options":{"ascii":"true","caseSensitive":"true","normalize":"true"}}}}

            // second creation (should fail)
            var ex = await Assert.ThrowsAsync<CommandException>(() =>
                table.CreateIndexAsync(indexName, (b) => b.Category, indexDefinition));

            Assert.Contains("already exists", ex.Message);

            // third creation (should not fail when IfNotExists is set)
            await table.CreateIndexAsync(indexName, (b) => b.Category, indexDefinition, new CreateIndexCommandOptions()
            {
                IfNotExists = true
            });
            // compare: {"createIndex":{"name":"category_idx","definition":{"column":"category","options":{"ascii":"true","caseSensitive":"true","normalize":"true"}},"options":{"ifNotExists":true}}}

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
    public async Task CreateIndexTestsSync_WithOptions()
    {
        var tableName = "tableIndexesTestSync_WithOptions";
        try
        {
            var table = await fixture.Database.CreateTableAsync<RowEventByDay>(tableName);
            string indexName = "category_idx";
            var indexDefinition = new TableIndexDefinition() { Ascii = true,  CaseSensitive = true, Normalize = true };

            table.CreateIndex(indexName, (b) => b.Category, indexDefinition);

            var result = await table.ListIndexesAsync();
            Assert.Contains(result.Indexes, i => i.Name == indexName);
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
            // compare: {"createIndex":{"name":"map_e_def_idx","definition":{"column":"StringMap"}}}

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
            // compare: {"createIndex":{"name":"map_e_idx","definition":{"column":"StringMap"}}}

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
            // compare: {"createIndex":{"name":"map_k_idx","definition":{"column":{"StringMap":"$keys"}}}}

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
            // compare: {"createIndex":{"name":"map_v_idx","definition":{"column":{"StringMap":"$values"}}}}

            var result = await table.ListIndexesAsync();
            Assert.Contains(result.Indexes, i => i.Name == indexName);

        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task CreateIndexTests_VectorIndex_NoOptions()
    {
        var tableName = "tableIndexesTest_VectorIndex_NoOptions";
        string indexName = "vector_idx_noopt";

        try
        {
            var table = await fixture.Database.CreateTableAsync<SimpleObjectWithVector>(tableName);

            await table.CreateVectorIndexAsync(indexName, (b) => b.VectorEmbeddings, Builders.TableIndex.Vector());
            // compare: {"createVectorIndex":{"name":"vector_idx_noopt","definition":{"column":"VectorEmbeddings"}}}

            var result = await table.ListIndexesAsync();
            Assert.Contains(result.Indexes, i => i.Name == indexName);

        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }

    }

    [Fact]
    public async Task CreateIndexTests_VectorIndex_PartialOptions()
    {
        var tableName = "tableIndexesTest_VectorIndex_PartialOptions";
        string indexName = "vector_idx_ptopt";

        try
        {
            var table = await fixture.Database.CreateTableAsync<SimpleObjectWithVector>(tableName);

            await table.CreateVectorIndexAsync(indexName, (b) => b.VectorEmbeddings, Builders.TableIndex.Vector(SimilarityMetric.DotProduct));
            // compare: {"createVectorIndex":{"name":"vector_idx_ptopt","definition":{"column":"VectorEmbeddings","options":{"metric":"dot_product"}}}}

            var result = await table.ListIndexesAsync();
            Assert.Contains(result.Indexes, i => i.Name == indexName);

        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }

    }

    [Fact]
    public async Task CreateIndexTests_VectorIndex_FullOptions()
    {
        var tableName = "tableIndexesTest_VectorIndex_FullOptions";
        string indexName = "vector_idx_fuopt";

        try
        {
            var table = await fixture.Database.CreateTableAsync<SimpleObjectWithVector>(tableName);

            await table.CreateVectorIndexAsync(indexName, (b) => b.VectorEmbeddings, Builders.TableIndex.Vector(SimilarityMetric.Euclidean, "other"));
            // compare: {"createVectorIndex":{"name":"vector_idx_fuopt","definition":{"column":"VectorEmbeddings","options":{"metric":"euclidean","sourceModel":"other"}}}}

            var result = await table.ListIndexesAsync();
            Assert.Contains(result.Indexes, i => i.Name == indexName);

        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }

    }

    [Fact]
    public async Task CreateIndexTests_TextIndex_NoOptions()
    {
        var tableName = "tableIndexesTest_TextIndex_NoOptions";
        string indexName = "text_idx_noopt";

        try
        {
            var table = await fixture.Database.CreateTableAsync<SimpleObject>(tableName);

            await table.CreateTextIndexAsync(indexName, (b) => b.Name, Builders.TableIndex.Text());
            // compare: {"createTextIndex":{"name":"text_idx_noopt","definition":{"column":"Name"}}}

            var result = await table.ListIndexesAsync();
            Assert.Contains(result.Indexes, i => i.Name == indexName);

        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }

    }

    [Fact]
    public async Task CreateIndexTests_TextIndex_WithAnalyzer()
    {
        var tableName = "tableIndexesTest_TextIndex_WithAnalyzer";
        string indexName = "text_idx_w_analyzer";

        try
        {
            var table = await fixture.Database.CreateTableAsync<SimpleObject>(tableName);

            await table.CreateTextIndexAsync(indexName, (b) => b.Name, Builders.TableIndex.Text(TextAnalyzer.Whitespace));
            // compare: {"createTextIndex":{"name":"text_idx_w_analyzer","definition":{"column":"Name","options":{"analyzer":"whitespace"}}}}

            var result = await table.ListIndexesAsync();
            Assert.Contains(result.Indexes, i => i.Name == indexName);

        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }

    }

    [Fact]
    public async Task CreateIndexTests_TextIndex_WithString()
    {
        var tableName = "tableIndexesTest_TextIndex_WithString";
        string indexName = "text_idx_w_string";

        try
        {
            var table = await fixture.Database.CreateTableAsync<SimpleObject>(tableName);

            await table.CreateTextIndexAsync(indexName, (b) => b.Name, Builders.TableIndex.Text("whitespace"));
            // compare: {"createTextIndex":{"name":"text_idx_w_string","definition":{"column":"Name","options":{"analyzer":"whitespace"}}}}

            var result = await table.ListIndexesAsync();
            Assert.Contains(result.Indexes, i => i.Name == indexName);

        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }

    }

    [Fact]
    public async Task CreateIndexTests_TextIndex_WithAOptions()
    {
        var tableName = "tableIndexesTest_TextIndex_WithAOptions";
        string indexName = "text_idx_w_aoptions";

        try
        {
            var table = await fixture.Database.CreateTableAsync<SimpleObject>(tableName);

            await table.CreateTextIndexAsync(indexName, (b) => b.Name, Builders.TableIndex.Text(
                new AnalyzerOptions{
                    Tokenizer = new TokenizerOptions { Name = "standard" },
                    Filters = {
                        "lowercase",
                        "stop",
                        "porterstem",
                        "asciifolding"
                    }
                }
            ));
            // compare: {"createTextIndex":{"name":"text_idx_w_aoptions","definition":{"column":"Name","options":{"analyzer":{"tokenizer":{"name":"standard","args":{}},"charFilters":[],"filters":[{"name":"lowercase"},{"name":"stop"},{"name":"porterstem"},{"name":"asciifolding"}]}}}}}

            var result = await table.ListIndexesAsync();
            Assert.Contains(result.Indexes, i => i.Name == indexName);

        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }

    }

    [Fact]
    public async Task CreateIndexTests_TextIndex_WithFreeform()
    {
        var tableName = "tableIndexesTest_TextIndex_WithFreeform";
        string indexName = "text_idx_w_freeform";

        try
        {
            var table = await fixture.Database.CreateTableAsync<SimpleObject>(tableName);

            await table.CreateTextIndexAsync(indexName, (b) => b.Name, Builders.TableIndex.Text(
                new Dictionary<string, object>
                {
                    ["tokenizer"] = new Dictionary<string, object>
                    {
                        ["name"] = "whitespace"
                    }
                }
            ));
            // compare: {"createTextIndex":{"name":"text_idx_w_freeform","definition":{"column":"Name","options":{"analyzer":{"tokenizer":{"name":"whitespace"}}}}}}

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
