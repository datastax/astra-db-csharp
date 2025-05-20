using DataStax.AstraDB.DataApi.Core;
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
        var table = fixture.Database.GetTable<RowEventByDay>("tableAlterTest", null);

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

    [Fact]
    public async Task AlterTableAddColumnsMapSet()
    {
        var table = fixture.Database.GetTable<RowEventByDay>("tableAlterTest", null);

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

    // Requires a pre-configured embedding provider on the Astra backend.
    [Fact]
    public async Task AlterTableAddVectorColumnsWithEmbedding()
    {
        var table = fixture.Database.GetTable<RowEventByDay>("tableAlterTest", null);

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

    [Fact]
    public async Task AlterTableAddVectorColumnsNoConfig()
    {
        var table = fixture.Database.GetTable<RowEventByDay>("tableAlterTest", null);

        await table.AlterAsync(new AlterTableAddVectorColumns(new Dictionary<string, AlterTableVectorColumnDefinition>
        {
            ["plot_synopsis_no_config"] = new AlterTableVectorColumnDefinition
            {
                VectorDimension = 2
            }
        }), null, false);
    }

    [Fact]
    public async Task AlterTableDropColumn()
    {
        var table = fixture.Database.GetTable<RowEventByDay>("tableAlterTest", null);

        var newColumns = new Dictionary<string, AlterTableColumnDefinition>
        {
            ["is_archived_drop"] = new AlterTableColumnDefinition { Type = "boolean" }
        };

        await table.AlterAsync(new AlterTableAddColumns(newColumns), null, runSynchronously: false);

        await table.AlterAsync(new AlterTableDropColumns(new[] { "is_archived_drop" }), null, runSynchronously: false);
    }

    [Fact]
    public async Task AlterTableDropVectorColumns()
    {
        var table = fixture.Database.GetTable<RowEventByDay>("tableAlterTest", null);

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

    [Fact]
    public async Task AlterTableAddVectorize()
    {
        var table = fixture.Database.GetTable<RowEventByDay>("tableAlterTest", null);

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

    [Fact]
    public async Task AlterTableOperationDropVectorize()
    {
        var table = fixture.Database.GetTable<RowEventByDay>("tableAlterTest", null);

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

}
