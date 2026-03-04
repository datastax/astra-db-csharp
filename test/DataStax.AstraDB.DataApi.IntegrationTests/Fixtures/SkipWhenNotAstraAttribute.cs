using Microsoft.Extensions.Configuration;
using System.Reflection;
using Xunit.v3;

namespace DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;

[AttributeUsage(AttributeTargets.Method | AttributeTargets.Class)]
public class SkipWhenNotAstraAttribute : BeforeAfterTestAttribute
{
    private static readonly Lazy<string> _destination = new(() =>
    {
        var config = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables(prefix: "ASTRA_DB_")
            .Build();
        return (config["DESTINATION"] ?? config["AstraDB:Destination"])?.ToLower();
    });

    public override void Before(MethodInfo methodUnderTest, IXunitTest test)
    {
        var destination = _destination.Value;
        if (!string.IsNullOrEmpty(destination) && destination != "astra")
            throw new Exception($"{DynamicSkipToken.Value}Requires Astra destination (current: '{destination}')");
    }
}