using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;
using DataStax.AstraDB.DataApi.Tables;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

// Tests in this collection should be run individually; changes are applied to the underlying fixture.
[Collection("TableAlter")]
public class TableAlterTests
{
    private readonly TableAlterFixture fixture;

    public TableAlterTests(TableAlterFixture fixture)
    {
        this.fixture = fixture;
    }

    [Fact]
    public async Task AlterTableAddColumns()
    {
        const string tableName = "addColumnsTest";
        try
        {
            var table = await fixture.CreateTestTable(tableName);

            var newColumns = new Dictionary<string, AlterTableColumnDefinition>
            {
                ["is_archived"] = new AlterTableColumnDefinition { Type = "boolean" },
                ["review_notes"] = new AlterTableColumnDefinition { Type = "text" }
            };

            await table.AlterAsync(new AlterTableAddColumns(newColumns), null, runSynchronously: false);

            //throws error on dupe
            var ex = await Assert.ThrowsAsync<DataStax.AstraDB.DataApi.Core.Commands.CommandException>(() =>
                     table.AlterAsync(new AlterTableAddColumns(newColumns), null, runSynchronously: false));

            Assert.Contains("unique", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task AlterTableAddColumnsMapSet()
    {
        const string tableName = "addColumnsMapSet";
        try
        {
            var table = await fixture.CreateTestTable(tableName);

            var newColumns = new Dictionary<string, AlterTableColumnDefinition>
            {
                ["column_test_map"] = new AlterTableColumnDefinition
                {
                    Type = "map",
                    KeyType = "text",
                    ValueType = "text"
                },
                ["column_test_set"] = new AlterTableColumnDefinition
                {
                    Type = "text",
                    ValueType = "text"
                }

            };

            await table.AlterAsync(new AlterTableAddColumns(newColumns), null, runSynchronously: false);
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    // Requires a pre-configured embedding provider on the Astra backend.
    [Fact]
    public async Task AlterTableAddVectorColumnsWithEmbedding()
    {
        const string tableName = "alterTableAddVectorColumnsWithEmbedding";
        try
        {
            var table = await fixture.CreateTestTable(tableName);

            await table.AlterAsync(new AlterTableAddVectorColumns(new Dictionary<string, AlterTableVectorColumnDefinition>
            {
                ["plot_synopsis"] = new AlterTableVectorColumnDefinition
                {
                    //VectorDimension = 1536,
                    VectorDimension = null,
                    Service = new VectorServiceOptions
                    {
                        Provider = "nvidia",
                        ModelName = "NV-Embed-QA"
                    }
                }
            }), null, false);
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task AlterTableAddVectorColumnsNoConfig()
    {
        const string tableName = "alterTableAddVectorColumnsNoConfig";
        try
        {
            var table = await fixture.CreateTestTable(tableName);

            await table.AlterAsync(new AlterTableAddVectorColumns(new Dictionary<string, AlterTableVectorColumnDefinition>
            {
                ["plot_synopsis_no_config"] = new AlterTableVectorColumnDefinition
                {
                    VectorDimension = 2
                }
            }), null, false);
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task AlterTableDropColumn()
    {
        const string tableName = "alterTableDropColumn";
        try
        {
            var table = await fixture.CreateTestTable(tableName);

            var newColumns = new Dictionary<string, AlterTableColumnDefinition>
            {
                ["is_archived_drop"] = new AlterTableColumnDefinition { Type = "boolean" }
            };

            await table.AlterAsync(new AlterTableAddColumns(newColumns), null, runSynchronously: false);

            await table.AlterAsync(new AlterTableDropColumns(new[] { "is_archived_drop" }), null, runSynchronously: false);
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task AlterTableDropVectorColumns()
    {
        const string tableName = "alterTableDropVectorColumns";
        try
        {
            var table = await fixture.CreateTestTable(tableName);

            await table.AlterAsync(new AlterTableAddVectorColumns(new Dictionary<string, AlterTableVectorColumnDefinition>
            {
                ["plot_synopsis_drop"] = new AlterTableVectorColumnDefinition
                {
                    VectorDimension = 2
                }
            }), null, false);

            var dropColumn = new AlterTableDropColumns(new[] { "plot_synopsis_drop" });
            await table.AlterAsync(dropColumn, null, runSynchronously: false);
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task AlterTableAddVectorize()
    {
        const string tableName = "alterTableAddVectorize";
        try
        {
            var table = await fixture.CreateTestTable(tableName);

            await table.AlterAsync(new AlterTableAddVectorColumns(new Dictionary<string, AlterTableVectorColumnDefinition>
            {
                ["plot_synopsis_vectorize"] = new AlterTableVectorColumnDefinition
                {
                    VectorDimension = 1024
                }
            }), null, false);

            await table.AlterAsync(new AlterTableAddVectorize(new Dictionary<string, VectorServiceOptions>
            {
                ["plot_synopsis_vectorize"] = new VectorServiceOptions
                {
                    Provider = "nvidia",
                    ModelName = "NV-Embed-QA"
                }

            }), null, false);
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task AlterTableOperationDropVectorize()
    {
        const string tableName = "alterTableOperationDropVectorize";
        try
        {
            var table = await fixture.CreateTestTable(tableName);

            await table.AlterAsync(new AlterTableAddVectorColumns(new Dictionary<string, AlterTableVectorColumnDefinition>
            {
                ["plot_synopsis_vectorize_drop"] = new AlterTableVectorColumnDefinition
                {
                    VectorDimension = 1024
                }
            }), null, false);

            await table.AlterAsync(new AlterTableAddVectorize(new Dictionary<string, VectorServiceOptions>
            {
                ["plot_synopsis_vectorize_drop"] = new VectorServiceOptions
                {
                    Provider = "nvidia",
                    ModelName = "NV-Embed-QA"
                }

            }), null, false);

            var dropVectorize = new AlterTableDropVectorize(new[] { "plot_synopsis_vectorize_drop" });
            await table.AlterAsync(dropVectorize, null, runSynchronously: false);

            var dropColumn = new AlterTableDropColumns(new[] { "plot_synopsis_vectorize_drop" });
            await table.AlterAsync(dropColumn, null, runSynchronously: false);
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

}
