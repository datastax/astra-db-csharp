using DataStax.AstraDB.DataApi.Core;

namespace DataStax.AstraDB.DataApi.Tests;

public class CommandOptionsTests
{
    [Fact]
    public void MergeTest()
    {
        var one = new CommandOptions();
        var two = new CommandOptions { Environment = DBEnvironment.Production };
        var three = new CommandOptions { Environment = DBEnvironment.Test };

        var result = CommandOptions.Merge(one, two, three);
        Assert.Equal(DBEnvironment.Test, result.Environment);

        result = CommandOptions.Merge(two, three, one);
        Assert.Equal(DBEnvironment.Test, result.Environment);

        result = CommandOptions.Merge(three, one, two);
        Assert.Equal(DBEnvironment.Production, result.Environment);

        result = CommandOptions.Merge(three, two, one);
        Assert.Equal(DBEnvironment.Production, result.Environment);
    }
}