using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Commands;
using DataStax.AstraDB.DataApi.Core.Enumeration;
using DataStax.AstraDB.DataApi.Core.Results;
using DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;
using DataStax.AstraDB.DataApi.SerDes;
using System.Linq;
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
    public async Task Test_CollectionCursor_IdleProperties()
    {
        var filledCollection = _fixture.FilledCollection;

        // pristine cursor properties
        var cur = filledCollection.Find();
        Assert.Equal(CursorState.Idle, cur.State);
        Assert.Equal(0, cur.Consumed);
        Assert.Empty(cur.ConsumeBuffer(3));
        Assert.Equal(0, cur.Buffered());
        Assert.Equal(0, cur.Consumed);

        var toClose = cur.Clone();
        toClose.Dispose();
        toClose.Dispose();
        Assert.Equal(CursorState.Closed, toClose.State);
        Assert.Equal(CursorState.Idle, cur.State);

        await Assert.ThrowsAsync<CursorException>( async () =>
        {
            await foreach (var item in toClose) { /* moot */ }
        });
        Assert.Throws<CursorException>(() =>
        {
            foreach (var item in toClose) { /* moot */ }
        });
        await Assert.ThrowsAsync<CursorException>( async () =>
            await toClose.ToListAsync()
        );
        Assert.Throws<CursorException>( () =>
            toClose.ToList()
        );
        // TODO: c# does not error on "MoveNextAsync" for closed cursor (python does):
        // await Assert.ThrowsAsync<CursorException>( async () =>
        //     await toClose.MoveNextAsync()
        // );

        // rewinding
        toClose.Rewind();
        Assert.Equal(CursorState.Idle, toClose.State);
        Assert.Equal(0, toClose.Consumed);
        Assert.Equal(0, toClose.Buffered());

        // various fluent-api methods
        toClose.Filter(Builders<CursorTestDocument>.CollectionFilter.Eq(d => d.PInt, 789));
        toClose.Project(Builders<CursorTestDocument>.Projection.Include(d => d.PInt));
        toClose.Sort(Builders<CursorTestDocument>.CollectionSort.Ascending(d => d.PInt));
        toClose.Limit(1);
        toClose.IncludeSimilarity(false);
        toClose.IncludeSortVector(true);
        toClose.Skip(1);
    }

    [Fact]
    public async Task Test_CollectionCursor_ClosedProperties()
    {
        var filledCollection = _fixture.FilledCollection;

        var cur0 = filledCollection.Find();
        cur0.Dispose();
        cur0.Rewind();
        Assert.Equal(CursorState.Idle, cur0.State);

        var cur1 = filledCollection.Find();
        Assert.Equal(0, cur1.Consumed);
        await foreach (var item in cur1) { /* moot */ }
        Assert.Equal(CursorState.Closed, cur1.State);
        Assert.Empty(cur1.ConsumeBuffer(2));
        Assert.Equal(_fixture.FilledCollectionCount, cur1.Consumed);
        Assert.Equal(0, cur1.Buffered());
        var cloned = cur1.Clone();
        Assert.Equal(0, cloned.Consumed);
        Assert.Equal(0, cloned.Buffered());
        Assert.Equal(CursorState.Idle, cloned.State);

        // Closed cursors shouldn't receive fluent-API edits:

        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur1.Filter(
        //     Builders<CursorTestDocument>.CollectionFilter.Empty()
        // ); });
        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur1.Project(
        //     Builders<CursorTestDocument>.Projection.Include(d => d.PText)
        // ); });
        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur1.Sort(
        //     Builders<CursorTestDocument>.Sort.Ascending(d => d.PText)
        // ); });
        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur1.Limit(1); });
        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur1.Skip(1); });
        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur1.IncludeSimilarity(true); });
        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur1.IncludeSortVector(true); });
        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur1.InitialPageState("blaaaa"); });

        // (note the full prefix required otherwise ambiguous a/sync unresolved)
        await Assert.ThrowsAsync<CursorException>(async () => { 
            await System.Linq.AsyncEnumerable.Select(cur1, doc => doc.PText).ToListAsync();
         });

    }

    [Fact]
    public async Task Test_CollectionCursor_StartedProperties()
    {
        var filledCollection = _fixture.FilledCollection;

        var cur = filledCollection.Find();
        await cur.MoveNextAsync();
        // now: 19 in buffer, one consumed:
        Assert.Equal(1, cur.Consumed);
        Assert.Equal(19, cur.Buffered());
        Assert.Equal(3, cur.ConsumeBuffer(3).Count);
        // 16 in buffer, 4 consumed:
        Assert.Equal(4, cur.Consumed);
        Assert.Equal(16, cur.Buffered());
        // from time to time the buffer is empty:
        for(int i=0; i<16; i++){
            await cur.MoveNextAsync();
        }
        Assert.Equal(0, cur.Buffered());
        Assert.Equal(0, cur.ConsumeBuffer(3).Count);
        Assert.Equal(20, cur.Consumed);
        Assert.Equal(0, cur.Buffered());

        // Started cursors can't receive fluent-API edits:

        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur.Filter(
        //     Builders<CursorTestDocument>.CollectionFilter.Empty()
        // ); });
        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur.Project(
        //     Builders<CursorTestDocument>.Projection.Include(d => d.PText)
        // ); });
        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur.Sort(
        //     Builders<CursorTestDocument>.Sort.Ascending(d => d.PText)
        // ); });
        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur.Limit(1); });
        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur.Skip(1); });
        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur.IncludeSimilarity(true); });
        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur.IncludeSortVector(true); });
        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur.InitialPageState("blaaaa"); });

        // TODO: this one *does not throw* (other clients don't admit setting mapping *after* started)
        //       But in this case "we're in LINQ's hands" and can't really do much about it. Are we ok?
        //
        // (note the full prefix required otherwise ambiguous a/sync unresolved)
        //await Assert.ThrowsAsync<CursorException>(async () => { 
            await System.Linq.AsyncEnumerable.Select(cur, doc => doc.PText).ToListAsync();
        // });

    }

    [Fact]
    public async Task Test_CollectionCursor_HasNext()
    {
        var filledCollection = _fixture.FilledCollection;
        var cur = filledCollection.Find();

        Assert.Equal(CursorState.Idle, cur.State);
        Assert.Equal(0, cur.Consumed);
        Assert.True(await cur.HasNextAsync());
        Assert.Equal(CursorState.Started, cur.State);
        Assert.Equal(0, cur.Consumed);
        await cur.MoveNextAsync();
        Assert.Equal(CursorState.Started, cur.State);
        await foreach (var item in cur) { /* moot */ };
        Assert.Equal(CursorState.Closed, cur.State);
        Assert.Equal(_fixture.FilledCollectionCount, cur.Consumed);

        var curMf = filledCollection.Find();
        await curMf.MoveNextAsync();
        await curMf.MoveNextAsync();
        Assert.Equal(2, curMf.Consumed);
        Assert.Equal(CursorState.Started, curMf.State);
        Assert.True(await curMf.HasNextAsync());
        Assert.Equal(2, curMf.Consumed);
        Assert.Equal(CursorState.Started, curMf.State);
        for(int i=0; i<18; i++){
            await curMf.MoveNextAsync();
        }
        Assert.True(await curMf.HasNextAsync());
        Assert.Equal(20, curMf.Consumed);
        Assert.Equal(CursorState.Started, curMf.State);
        Assert.Equal(_fixture.FilledCollectionCount - 20, curMf.Buffered());

        var cur0 = filledCollection.Find();
        cur0.Dispose();
        Assert.False(await cur0.HasNextAsync());
    }

    [Fact]
    public async Task Test_CollectionCursor_ZeroMatches()
    {
        var filledCollection = _fixture.FilledCollection;
        var cur = filledCollection.Find().Filter(
            Builders<CursorTestDocument>.CollectionFilter.Eq(d => d.PText, "ZZ"));

        Assert.False(await cur.HasNextAsync());
        await Assert.ThrowsAsync<CursorException>( async () =>
        {
            await cur.ToListAsync();
        });
    }

    [Fact]
    public async Task Test_CollectionCursor_EarlyClosing()
    {
        var filledCollection = _fixture.FilledCollection;
        var cur = filledCollection.Find();
        for (int i = 0; i < 12; i++){
            await cur.MoveNextAsync();
        }
        cur.Dispose();
        Assert.Equal(CursorState.Closed, cur.State);
        Assert.Equal(0, cur.Buffered());
        Assert.Equal(12, cur.Consumed);

        cur.Rewind();
        Assert.Equal(_fixture.FilledCollectionCount, cur.ToList().Count);
    }

    [Fact]
    public async Task Test_CollectionCursor_CollectiveMethods()
    {
        var filledCollection = _fixture.FilledCollection;
        var baseRows = await filledCollection.Find().ToListAsync();

        // full ToList (list equalities projected on scalar lists for conciseness)
        var tlCur = filledCollection.Find();
        Assert.Equal(baseRows.Select(d => d.Id), (await tlCur.ToListAsync()).Select(d => d.Id));
        Assert.Equal(CursorState.Closed, tlCur.State);

        // partially-consumed ToList
        var ptlCur = filledCollection.Find();
        for (int i = 0; i < 15; i++){
            await ptlCur.MoveNextAsync();
        }
        Assert.Equal(baseRows.Skip(15).Select(d => d.Id), (await ptlCur.ToListAsync()).Select(d => d.Id));
        Assert.Equal(CursorState.Closed, ptlCur.State);

        // Tests on *mapped cursors + ToList* are omitted, as such logic occurs not in cursor territory anymore (rather LINQ's).

        // Full ForEach
        /* TODO (this section):
            c# differs greatly from other clients. There's no ForEach on cursors (and arguably there shouldn't be).
            As is now, this part passes but adds little. Even the client pattern of a "ForEach(callback)", with the
            callback returning whether to stop or not, probably is not needed here, nor is it idiomatic.
            I think we should be ok with there not being any ForEach fancy thing other than the LINQ stuff (so dropping this part of test?)
        */
        var accum0 = new List<CursorTestDocument>();
        var feCur = filledCollection.Find();
        await foreach (var row in feCur)
        {
            accum0.Add(row);
        }
        Assert.Equal(baseRows.Select(d => d.Id), accum0.Select(d => d.Id));
        Assert.Equal(CursorState.Closed, feCur.State);

        /* TODO in the same spirit, porting from Python I am skipping:
            1. same as above with a coroutine callback (does not apply here)
            2. foreach on a partially-consumed cursor (trivial once exited cursor to LINQ-land)
            3. 1+2
            4. mapped ForEach (we're not testing LINQ after all)
            5. mapped ForEach with coroutine
            6. early-break ForEach & coroutine forms, various return types thereof (don't apply)
        */
    }

    [Fact]
    public async Task Test_CollectionCursor_InitialPageState()
    {
        const int PAGE_SIZE = 20;

        var filledPagCollection = _fixture.FilledPaginationCollection;
        var cur = filledPagCollection.Find(
            Builders<CursorPaginationTestDocument>.CollectionFilter.Eq(d => d.even, true));

        // TODO This test is made of two parts (in its Python inspiration)
        // Part 1: accesses the cursor private page state and puts it to test in a few ways.
        //      This cannot be done here. The rest is just this (which could be moved to next test):

        // Part 2: tests  an overload `Rewind(initialPageState)` which is not available.
        //      TODO should it be added? (Python has it)
    }

    [Fact]
    public async Task Test_CollectionCursor_FetchNextPage()
    {
        var filledPagCollection = _fixture.FilledPaginationCollection;

        var cur0 = filledPagCollection.Find(
            Builders<CursorPaginationTestDocument>.CollectionFilter.Eq(d => d.even, true));
        var page0 = await cur0.FetchNextPageAsync();
        // TODO this should be `Results` (see yellow doc for 'specs') and not `Results`
        var ids0 = page0.Results.Select(d => d.id).ToList();
        var nps0 = page0.NextPageState;
        Assert.IsType<string>(nps0);

        // TODO: are we ok that there is no 'options' to Find, alternative to fluent way to set initialPS?
        var cur1 = filledPagCollection.Find(
            Builders<CursorPaginationTestDocument>.CollectionFilter.Eq(d => d.even, true)
        ).InitialPageState(nps0);
        var page1 = await cur1.FetchNextPageAsync();
        var ids1 = page1.Results.Select(d => d.id).ToList();
        var nps1 = page1.NextPageState;
        Assert.IsType<string>(nps1);

        var cur2 = filledPagCollection.Find(
            Builders<CursorPaginationTestDocument>.CollectionFilter.Eq(d => d.even, true)
        ).InitialPageState(nps1);
        var page2 = await cur2.FetchNextPageAsync();
        var ids2 = page2.Results.Select(d => d.id).ToList();
        Assert.Null(page2.NextPageState);

        var expectedIds = new List<int>();
        for (int id = 0; id < _fixture.FilledPaginationCollectionCount; id+=2){ expectedIds.Add(id); }
        var retrievedIds = ids0.Concat(ids1).Concat(ids2).ToList();
        Assert.Equal(expectedIds, retrievedIds.OrderBy(x => x).ToList());

        // Fetching consecutive pages on a given cursor
        var cur0x = filledPagCollection.Find(
            Builders<CursorPaginationTestDocument>.CollectionFilter.Eq(d => d.even, true));
        await cur0x.FetchNextPageAsync();
        var page1x = await cur0x.FetchNextPageAsync();
        Assert.Equal(page1x.NextPageState, page1.NextPageState);
        Assert.Equivalent(page1x.Results, page1.Results);
        Assert.Equal(page1x.SortVector, page1.SortVector);

        // Forbidden: mixing pagination and ordinary usage
        var cur0y = filledPagCollection.Find(
            Builders<CursorPaginationTestDocument>.CollectionFilter.Eq(d => d.even, true));
        await cur0y.MoveNextAsync();
        await Assert.ThrowsAsync<CursorException>( async () =>
        {
            // errors because we're in mid-page
            await cur0y.FetchNextPageAsync();
        });
        await cur0y.ToListAsync();
        await Assert.ThrowsAsync<CursorException>( async () =>
        {
            // errors because closed already (by ToList)
            await cur0y.FetchNextPageAsync();
        });            

        // Include-sort-vector:
        var vcur0 = filledPagCollection.Find()
            .IncludeSortVector(true)
            .Limit(15)
            .Sort(Builders<CursorPaginationTestDocument>.CollectionSort.Vector(new float[] { 1.0f, 1.0f }));
        var vpage0 = await vcur0.FetchNextPageAsync();
        Assert.Null(vpage0.NextPageState);
        Assert.Equal(15, vpage0.Results.Count);
        Assert.IsType<float[]>(vpage0.SortVector);
    }

}
