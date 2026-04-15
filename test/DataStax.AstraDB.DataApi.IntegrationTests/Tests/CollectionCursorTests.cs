using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Commands;
using DataStax.AstraDB.DataApi.Core.Enumeration;
using DataStax.AstraDB.DataApi.Core.Results;
using DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;
using DataStax.AstraDB.DataApi.SerDes;
using MongoDB.Bson;
using System.Text;
using UUIDNext;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

[Collection("CollectionCursor")]
public class CollectionCursorTests
{

    private readonly CollectionCursorFixture _fixture;
    Collection<CursorTestDocument> filledCollection;

    public CollectionCursorTests(AssemblyFixture assemblyFixture, CollectionCursorFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public void Test_CollectionCursor_IdleProperties()
    {
        var filledCollection = _fixture.FilledCollection;

        // pristine cursor properties
        var cur = filledCollection.Find();
        Assert.Equal(CursorState.Idle, cur.State);
        Assert.Equal(0, cur.Consumed);
        Assert.Empty(cur.ConsumeBuffer(3));
        Assert.Equal(0, cur.Buffered());
        Assert.Equal(0, cur.Consumed);

        /* This part cannot be completed because there is no `.Close()` cursor method (I think should be added)
        var toClose = cur.Clone();
        toClose.Close();
        toClose.Close();
        Assert.Equal(CursorState.Closed, cur.State);

        // TODO depythonize this part (ahem)
        with pytest.raises(CursorException):
            async for row in toclose:
                pass
        with pytest.raises(StopAsyncIteration):
            await toclose.__anext__()
        with pytest.raises(CursorException):
            await toclose.for_each(lambda row: None)
        with pytest.raises(CursorException):
            await toclose.to_list()
        */

        // rewinding
        cur.Rewind();
        Assert.Equal(CursorState.Idle, cur.State);
        Assert.Equal(0, cur.Consumed);
        Assert.Equal(0, cur.Buffered());

        // various fluent-api methods
        cur.Filter(Builders<CursorTestDocument>.CollectionFilter.Eq(d => d.PInt, 789));
        cur.Project(Builders<CursorTestDocument>.Projection.Include(d => d.PInt));
        cur.Sort(Builders<CursorTestDocument>.Sort.Ascending(d => d.PInt));
        cur.Limit(1);
        cur.IncludeSimilarity(false);
        cur.IncludeSortVector(true);
        cur.Skip(1);
    }

}
