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
            var foundIndex = result.Indexes.Single(i => i.Name == indexName);
            Assert.NotNull(foundIndex);
            Assert.Equal("category", foundIndex.Definition.Column);
            Assert.IsType<TableIndexDefinition>(foundIndex.Definition);

            var insertResult = await TableIndexesFixture.AddTableRows(table);
            Assert.Equal(3, insertResult.InsertedCount);
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task CreateIndexTests_Untyped_NoOptions()
    {
        var tableName = "tableIndexesTest_Untyped_NoOptions";
        string indexName = "category_idx_untyped_noopt";

        try
        {
            var table = await fixture.Database.CreateTableAsync<RowEventByDay>(tableName);

            await table.CreateIndexAsync(indexName, "category");
            // compare: {"createIndex":{"name":"category_idx_untyped_noopt","definition":{"column":"category"}}}

            var result = await table.ListIndexesAsync();
            var foundIndex = result.Indexes.Single(i => i.Name == indexName);
            Assert.NotNull(foundIndex);
            Assert.Equal("category", foundIndex.Definition.Column);
            Assert.IsType<TableIndexDefinition>(foundIndex.Definition);

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
            // compare: {"createIndex":{"name":"category_idx","definition":{"column":"category","options":{"ascii":true,"caseSensitive":true,"normalize":true}}}}

            // second creation (should fail)
            var ex = await Assert.ThrowsAsync<CommandException>(() =>
                table.CreateIndexAsync(indexName, (b) => b.Category, indexDefinition));

            Assert.Contains("already exists", ex.Message);

            // third creation (should not fail when IfNotExists is set)
            await table.CreateIndexAsync(indexName, (b) => b.Category, indexDefinition, new CreateIndexCommandOptions()
            {
                IfNotExists = true
            });
            // {"createIndex":{"name":"category_idx","definition":{"column":"category","options":{"ascii":true,"caseSensitive":true,"normalize":true}},"options":{"ifNotExists":true}}}

            var result = await table.ListIndexesAsync();
            var foundIndex = result.Indexes.Single(i => i.Name == indexName);
            Assert.NotNull(foundIndex);
            Assert.Equal("category", foundIndex.Definition.Column);
            Assert.IsType<TableIndexDefinition>(foundIndex.Definition);
            Assert.True(((TableIndexDefinition)foundIndex.Definition).Ascii);
            Assert.True(((TableIndexDefinition)foundIndex.Definition).CaseSensitive);
            Assert.True(((TableIndexDefinition)foundIndex.Definition).Normalize);

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

            var result = table.ListIndexes();
            var foundIndex = result.Indexes.Single(i => i.Name == indexName);
            Assert.NotNull(foundIndex);
            Assert.Equal("category", foundIndex.Definition.Column);
            Assert.IsType<TableIndexDefinition>(foundIndex.Definition);
            Assert.True(((TableIndexDefinition)foundIndex.Definition).Ascii);
            Assert.True(((TableIndexDefinition)foundIndex.Definition).CaseSensitive);
            Assert.True(((TableIndexDefinition)foundIndex.Definition).Normalize);

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
            // compare: {"createIndex":{"name":"map_e_def_idx","definition":{"column":"StringMap"}}}

            var result = await table.ListIndexesAsync();
            var foundIndex = result.Indexes.Single(i => i.Name == indexName);
            Assert.NotNull(foundIndex);
            Assert.IsType<TableIndexDefinition>(foundIndex.Definition);
            Assert.Equal("StringMap", foundIndex.Definition.Column);

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
            var foundIndex = result.Indexes.Single(i => i.Name == indexName);
            Assert.NotNull(foundIndex);
            Assert.IsType<TableIndexDefinition>(foundIndex.Definition);
            Assert.Equal("StringMap", foundIndex.Definition.Column);

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
            var foundIndex = result.Indexes.Single(i => i.Name == indexName);
            Assert.NotNull(foundIndex);
            Assert.IsType<TableIndexDefinition>(foundIndex.Definition);
            Assert.Equal(new Dictionary<string,string> { ["StringMap"] = "$keys" }, foundIndex.Definition.Column);

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
            var foundIndex = result.Indexes.Single(i => i.Name == indexName);
            Assert.NotNull(foundIndex);
            Assert.IsType<TableIndexDefinition>(foundIndex.Definition);
            Assert.Equal(new Dictionary<string,string> { ["StringMap"] = "$values" }, foundIndex.Definition.Column);

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
            var foundIndex = result.Indexes.Single(i => i.Name == indexName);
            Assert.NotNull(foundIndex);
            Assert.IsType<TableVectorIndexDefinition>(foundIndex.Definition);
            Assert.Equal("VectorEmbeddings", foundIndex.Definition.Column);

        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }

    }

    [Fact]
    public async Task CreateIndexTests_VectorIndex_Untyped_NoOptions()
    {
        var tableName = "tableIndexesTest_VectorIndex_Untyped_NoOptions";
        string indexName = "vector_idx_untyped_noopt";

        try
        {
            var table = await fixture.Database.CreateTableAsync<SimpleObjectWithVector>(tableName);

            await table.CreateVectorIndexAsync(indexName, "VectorEmbeddings");
            // compare: {"createVectorIndex":{"name":"vector_idx_untyped_noopt","definition":{"column":"VectorEmbeddings"}}}

            var result = await table.ListIndexesAsync();
            var foundIndex = result.Indexes.Single(i => i.Name == indexName);
            Assert.NotNull(foundIndex);
            Assert.IsType<TableVectorIndexDefinition>(foundIndex.Definition);
            Assert.Equal("VectorEmbeddings", foundIndex.Definition.Column);

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
            var foundIndex = result.Indexes.Single(i => i.Name == indexName);
            Assert.NotNull(foundIndex);
            Assert.IsType<TableVectorIndexDefinition>(foundIndex.Definition);
            Assert.Equal("VectorEmbeddings", foundIndex.Definition.Column);
            Assert.Equal(SimilarityMetric.DotProduct, ((TableVectorIndexDefinition)foundIndex.Definition).Metric);

        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }

    }

    [Fact]
    public async Task CreateIndexTests_VectorIndex_PartialSMOptions()
    {
        var tableName = "tableIndexesTest_VectorIndex_PartialSMOptions";
        string indexName = "vector_idx_ptsmopt";

        try
        {
            var table = await fixture.Database.CreateTableAsync<SimpleObjectWithVector>(tableName);

            await table.CreateVectorIndexAsync(indexName, (b) => b.VectorEmbeddings, Builders.TableIndex.Vector("bert"));

            var result = await table.ListIndexesAsync();
            var foundIndex = result.Indexes.Single(i => i.Name == indexName);
            Assert.NotNull(foundIndex);
            Assert.IsType<TableVectorIndexDefinition>(foundIndex.Definition);
            Assert.Equal("VectorEmbeddings", foundIndex.Definition.Column);
            Assert.Equal("bert", ((TableVectorIndexDefinition)foundIndex.Definition).SourceModel);

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

            await table.CreateVectorIndexAsync(indexName, (b) => b.VectorEmbeddings, Builders.TableIndex.Vector(SimilarityMetric.Euclidean, "bert"));
            // compare: {"createVectorIndex":{"name":"vector_idx_fuopt","definition":{"column":"VectorEmbeddings","options":{"metric":"euclidean","sourceModel":"other"}}}}

            var result = await table.ListIndexesAsync();
            var foundIndex = result.Indexes.Single(i => i.Name == indexName);
            Assert.NotNull(foundIndex);
            Assert.IsType<TableVectorIndexDefinition>(foundIndex.Definition);
            Assert.Equal("VectorEmbeddings", foundIndex.Definition.Column);
            Assert.Equal("bert", ((TableVectorIndexDefinition)foundIndex.Definition).SourceModel);
            Assert.Equal(SimilarityMetric.Euclidean, ((TableVectorIndexDefinition)foundIndex.Definition).Metric);

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
            var foundIndex = result.Indexes.Single(i => i.Name == indexName);
            Assert.NotNull(foundIndex);
            Assert.IsType<TableTextIndexDefinition>(foundIndex.Definition);
            Assert.Equal("Name", foundIndex.Definition.Column);

        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }

    }

    [Fact]
    public async Task CreateIndexTests_TextIndex_Untyped_NoOptions()
    {
        var tableName = "tableIndexesTest_TextIndex_Untyped_NoOptions";
        string indexName = "text_idx_untyped_noopt";

        try
        {
            var table = await fixture.Database.CreateTableAsync<SimpleObject>(tableName);

            await table.CreateTextIndexAsync(indexName, "Name");
            // compare: {"createTextIndex":{"name":"text_idx_untyped_noopt","definition":{"column":"Name"}}}

            var result = await table.ListIndexesAsync();
            var foundIndex = result.Indexes.Single(i => i.Name == indexName);
            Assert.NotNull(foundIndex);
            Assert.IsType<TableTextIndexDefinition>(foundIndex.Definition);
            Assert.Equal("Name", foundIndex.Definition.Column);

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
            var foundIndex = result.Indexes.Single(i => i.Name == indexName);
            Assert.NotNull(foundIndex);
            Assert.IsType<TableTextIndexDefinition>(foundIndex.Definition);
            Assert.Equal("Name", foundIndex.Definition.Column);
            Assert.Equal("whitespace", ((TableTextIndexDefinition)foundIndex.Definition).Analyzer);

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
            var foundIndex = result.Indexes.Single(i => i.Name == indexName);
            Assert.NotNull(foundIndex);
            Assert.IsType<TableTextIndexDefinition>(foundIndex.Definition);
            Assert.Equal("Name", foundIndex.Definition.Column);
            Assert.Equal("whitespace", ((TableTextIndexDefinition)foundIndex.Definition).Analyzer);

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
            var foundIndex = result.Indexes.Single(i => i.Name == indexName);
            Assert.NotNull(foundIndex);
            Assert.IsType<TableTextIndexDefinition>(foundIndex.Definition);
            Assert.Equal("Name", foundIndex.Definition.Column);
            var theAnalyzer = ((TableTextIndexDefinition)foundIndex.Definition).Analyzer;
            // Assert.Equal(
            //     new Dictionary<string, object>{["name"] = "standard", ["args"] = new Dictionary<string, string>()},
            //     ((Dictionary<string, object>)theAnalyzer)["tokenizer"]
            // ); <== Fails because nested dictionaries != nested JSONElements

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
            var foundIndex = result.Indexes.Single(i => i.Name == indexName);
            Assert.NotNull(foundIndex);
            Assert.IsType<TableTextIndexDefinition>(foundIndex.Definition);
            Assert.Equal("Name", foundIndex.Definition.Column);
            var theAnalyzer = ((TableTextIndexDefinition)foundIndex.Definition).Analyzer;
            // Assert.Equal(
            //     new Dictionary<string, string>{["name"] = "whitespace"},
            //     ((Dictionary<string, object>)theAnalyzer)["tokenizer"]
            // ); <== Fails because dictionary != JSONElements

        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }

    }

    // [Fact(Skip="Run manually after some CQL setup!")]
    [Fact]
    public async Task ListIndexesTests_UnknownUnsupportedCQLIndex()
    {
        var tableName = "table_with_unsupported_index";
        string indexName = "unsupported_idx";

        /*
        THIS MUST BE RUN ON HCD!
        MANUAL CQL SETUP before running this test:
            CREATE TABLE table_with_unsupported_index(
                id TEXT PRIMARY KEY, value TEXT, TheCamelVec VECTOR<FLOAT,3>
            );
            CREATE INDEX unsupported_idx ON table_with_unsupported_index (value);
        CORRESPONDING RESPONSE:
                {
                    "status": {
                        "indexes": [
                            {
                                "name": "unsupported_idx",
                                "definition": {
                                    "column": "UNKNOWN",
                                    "apiSupport": {
                                        "createIndex": false,
                                        "filter": false,
                                        "cqlDefinition": "CREATE INDEX unsupported_idx ON default_keyspace.table_with_unsupported_index (value);"
                                    }
                                },
                                "indexType": "UNKNOWN"
                            }
                        ]
                    }
                }
        */

        var table = fixture.Database.GetTable(tableName);

        var result = await table.ListIndexesAsync();
        var foundIndex = result.Indexes.Single(i => i.Name == indexName);
        Assert.NotNull(foundIndex);
        Assert.IsType<TableUnknownIndexDefinition>(foundIndex.Definition);
        Assert.Equal("UNKNOWN", foundIndex.Definition.Column);
        Assert.Null(foundIndex.Definition.Options);
        var apiSupport = ((TableUnknownIndexDefinition)foundIndex.Definition).APISupport;
        Assert.IsType<TableUnknownIndexAPISupport>(apiSupport);
        Assert.False(apiSupport.CreateIndex);
        Assert.False(apiSupport.Filter);
        Assert.Equal(
            "CREATE INDEX unsupported_idx ON default_keyspace.table_with_unsupported_index (value);",
            apiSupport.CQLDefinition
        );

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
