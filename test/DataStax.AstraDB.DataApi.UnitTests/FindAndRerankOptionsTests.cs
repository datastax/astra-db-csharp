/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using DataStax.AstraDB.DataApi.Core.Query;
using Xunit;
using System.Collections.Generic;
using System.Linq;

namespace DataStax.AstraDB.DataApi.UnitTests.Core.Query;

public class TestDocument { }

public class FindAndRerankOptionsTests
{
    [Fact]
    public void Clone_WithAllPropertiesSet_ShouldCreateExactCopy()
    {
        // Arrange
        var original = new FindAndRerankOptions<TestDocument>
        {
            RerankOn = "score_field",
            IncludeScores = true,
            IncludeSortVector = true,
            RerankQuery = "test query",
            Limit = 10,
            HybridLimits = new Dictionary<string, int> { { "vector", 5 }, { "text", 15 } },
            Filter = Filter<TestDocument>.Eq("field1", "value1") & Filter<TestDocument>.Neq("field2", 42),
            Projection = new ProjectionBuilder<TestDocument>().Include("field1").Exclude("field2"),
            Sorts = new List<Sort> 
            { 
                Sort.Ascending("field1"),
                Sort.Descending("field2"),
                Sort.Vector(new float[] { 1.0f, 2.0f, 3.0f })
            }
        };

        // Act
        var clone = original.Clone();

        // Assert - Verify all properties are equal
        Assert.Equal(original.RerankOn, clone.RerankOn);
        Assert.Equal(original.IncludeScores, clone.IncludeScores);
        Assert.Equal(original.IncludeSortVector, clone.IncludeSortVector);
        Assert.Equal(original.RerankQuery, clone.RerankQuery);
        Assert.Equal(original.Limit, clone.Limit);
        
        // Verify HybridLimits is a deep copy
        Assert.NotNull(clone.HybridLimits);
        Assert.NotSame(original.HybridLimits, clone.HybridLimits);
        Assert.Equal(original.HybridLimits.Count, clone.HybridLimits.Count);
        foreach (var kvp in original.HybridLimits)
        {
            Assert.True(clone.HybridLimits.ContainsKey(kvp.Key));
            Assert.Equal(kvp.Value, clone.HybridLimits[kvp.Key]);
        }

        // Verify Filter is a deep copy
        Assert.NotNull(clone.Filter);
        Assert.NotSame(original.Filter, clone.Filter);
        
        // Verify Projection is a deep copy
        Assert.NotNull(clone.Projection);
        Assert.NotSame(original.Projection, clone.Projection);
        Assert.Equal(original.Projection.Projections.Count, clone.Projection.Projections.Count);
        
        // Verify Sorts is a deep copy
        Assert.NotNull(clone.Sorts);
        Assert.NotSame(original.Sorts, clone.Sorts);
        Assert.Equal(original.Sorts.Count, clone.Sorts.Count);
        for (int i = 0; i < original.Sorts.Count; i++)
        {
            Assert.Equal(original.Sorts[i].Name, clone.Sorts[i].Name);
            if (original.Sorts[i].Value is float[] originalVector)
            {
                var cloneVector = (float[])clone.Sorts[i].Value;
                Assert.Equal(originalVector, cloneVector);
                Assert.NotSame(originalVector, cloneVector);
            }
            else
            {
                Assert.Equal(original.Sorts[i].Value, clone.Sorts[i].Value);
            }
        }
    }

    [Fact]
    public void Clone_WithNullProperties_ShouldHandleNullsCorrectly()
    {
        // Arrange
        var original = new FindAndRerankOptions<TestDocument>
        {
            // Only set required properties, leave others null
            RerankOn = null,
            IncludeScores = null,
            IncludeSortVector = null,
            RerankQuery = null,
            Limit = null,
            HybridLimits = null,
            Filter = null,
            Projection = null,
            Sorts = null
        };

        // Act
        var clone = original.Clone();

        // Assert
        Assert.Null(clone.RerankOn);
        Assert.Null(clone.IncludeScores);
        Assert.Null(clone.IncludeSortVector);
        Assert.Null(clone.RerankQuery);
        Assert.Null(clone.Limit);
        Assert.Null(clone.HybridLimits);
        Assert.Null(clone.Filter);
        Assert.Null(clone.Projection);
        Assert.NotNull(clone.Sorts); // Should be an empty list, not null
        Assert.Empty(clone.Sorts);
    }

    [Fact]
    public void Clone_WithEmptyCollections_ShouldCreateEmptyCollections()
    {
        // Arrange
        var original = new FindAndRerankOptions<TestDocument>
        {
            HybridLimits = new Dictionary<string, int>(),
            Sorts = new List<Sort>()
        };

        // Act
        var clone = original.Clone();

        // Assert
        Assert.NotNull(clone.HybridLimits);
        Assert.Empty(clone.HybridLimits);
        Assert.NotNull(clone.Sorts);
        Assert.Empty(clone.Sorts);
    }

    [Fact]
    public void Clone_WithComplexFilter_ShouldDeepCloneFilter()
    {
        // Arrange
        var filter = Filter<TestDocument>.Eq("field1", "value1") | 
                    (Filter<TestDocument>.Gt("field2", 10) & Filter<TestDocument>.Lt("field2", 20));
        
        var original = new FindAndRerankOptions<TestDocument>
        {
            Filter = filter
        };

        // Act
        var clone = original.Clone();

        // Assert
        Assert.NotNull(clone.Filter);
        Assert.NotSame(original.Filter, clone.Filter);
        // Note: More detailed filter structure validation would require exposing more of the filter internals
    }

    [Fact]
    public void Clone_WithVectorSort_ShouldDeepCloneVector()
    {
        // Arrange
        var vector = new float[] { 1.0f, 2.0f, 3.0f };
        var original = new FindAndRerankOptions<TestDocument>
        {
            Sorts = new List<Sort> { Sort.Vector(vector) }
        };

        // Act
        var clone = original.Clone();

        // Assert
        var originalVector = (float[])original.Sorts[0].Value;
        var cloneVector = (float[])clone.Sorts[0].Value;
        
        Assert.Equal(originalVector, cloneVector);
        Assert.NotSame(originalVector, cloneVector);
    }
}
