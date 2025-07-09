using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;

[CollectionDefinition("DatabaseAndCollections")]
public class DatabaseAndCollectionsCollection : ICollectionFixture<AssemblyFixture>, ICollectionFixture<CollectionsFixture>
{
}

public class CollectionsFixture : BaseFixture, IAsyncLifetime
{
    public CollectionsFixture(AssemblyFixture assemblyFixture) : base(assemblyFixture, "collections")
    {
    }

    public Collection<SimpleObject> SearchCollection { get; private set; }

    public async ValueTask InitializeAsync()
    {
        await CreateSearchCollection();
        var collection = Database.GetCollection<SimpleObject>(_queryCollectionName);
        SearchCollection = collection;
    }

    public async ValueTask DisposeAsync()
    {
        await Database.DropCollectionAsync(_queryCollectionName);
    }

    private const string _queryCollectionName = "simpleObjectsQueryTests";
    private async Task CreateSearchCollection()
    {
        List<SimpleObject> items = new List<SimpleObject>() {
                new()
                {
                    _id = 0,
                    Name = "Cat",
                    Properties = new Properties() {
                        PropertyOne = "groupone",
                        PropertyTwo = "cat",
                        IntProperty = 1,
                        BoolProperty = true,
                        StringArrayProperty = new[] { "cat1", "cat2", "cat3" },
                        DateTimeProperty = new DateTime(2020, 1, 1, 1, 1, 0)
                    }
                },
                new()
                {
                    _id = 1,
                    Name = "Dog",
                    Properties = new Properties() {
                        PropertyOne = "groupone",
                        PropertyTwo = "dog",
                        IntProperty = 2,
                        BoolProperty = true,
                        StringArrayProperty = new[] { "dog1", "dog2", "dog3" },
                        DateTimeProperty = new DateTime(2020, 1, 1, 1, 2, 0)
                    }
                },
                new()
                {
                    _id = 2,
                    Name = "Horse",
                    Properties = new Properties() {
                        PropertyOne = "grouptwo",
                        PropertyTwo = "horse",
                        IntProperty = 3,
                        BoolProperty = true,
                        StringArrayProperty = new[] { "horse1", "horse2", "horse3" },
                        DateTimeProperty = new DateTime(2020, 1, 1, 1, 3, 0)
                    }
                },
                new()
                {
                    _id = 3,
                    Name = "Cow",
                    Properties = new Properties() {
                        PropertyOne = "grouptwo",
                        PropertyTwo = "cow",
                        IntProperty = 4,
                        BoolProperty = true,
                        StringArrayProperty = new[] { "cow1", "cow2", "cow3" },
                        DateTimeProperty = new DateTime(2020, 1, 1, 1, 4, 0)
                    }
                },
                new()
                {
                    _id = 4,
                    Name = "Alligator",
                    Properties = new Properties() {
                        PropertyOne = "grouptwo",
                        PropertyTwo = "alligator",
                        IntProperty = 5,
                        BoolProperty = true,
                        StringArrayProperty = new[] { "alligator1", "alligator2", "alligator3" },
                        DateTimeProperty = new DateTime(2020, 1, 1, 1, 5, 0)
                    }
                },
            };

        for (var i = 5; i <= 30; i++)
        {
            items.Add(new()
            {
                _id = i,
                Name = $"Animal{i}",
                Properties = new Properties()
                {
                    PropertyOne = "groupthree",
                    PropertyTwo = $"animal{i}",
                    IntProperty = i + 1,
                    BoolProperty = true,
                    StringArrayProperty = new[] { $"animal{i}1", $"animal{i}2" },
                    DateTimeProperty = new DateTime(2020, 1, 1, 1, i + 1, 0)
                }
            });
        }
        items.Add(new()
        {
            _id = 31,
            Name = "Cow Group 4",
            Properties = new Properties()
            {
                PropertyOne = "groupfour",
                PropertyTwo = "cow",
                IntProperty = 32,
                BoolProperty = true,
                StringArrayProperty = new[] { "cow1", "cow2" },
                DateTimeProperty = new DateTime(2020, 1, 1, 1, 32, 0)
            }
        });
        items.Add(new()
        {
            _id = 32,
            Name = "Alligator Group 4",
            Properties = new Properties()
            {
                PropertyOne = "groupfour",
                PropertyTwo = "alligator",
                IntProperty = 33,
                BoolProperty = true,
                StringArrayProperty = new[] { "alligator1", "alligator2" },
                DateTimeProperty = new DateTime(2020, 1, 1, 1, 33, 0)
            }
        });
        var collection = await Database.CreateCollectionAsync<SimpleObject>(_queryCollectionName);
        await collection.InsertManyAsync(items);

        SearchCollection = collection;
    }

}