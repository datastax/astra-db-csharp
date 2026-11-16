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

using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.SerDes;

namespace DataStax.AstraDB.DataApi.UnitTests;

public class CollectionDefinitionTests
{
    [Fact]
    public void CollectionDefinitionMultiDimensionCompatibilityTests()
    {
        // Combinations of multiply-defined dimension attributes, conflicting and in agreement,
        // and their behaviour re: CheckAddDefinitionsFromAttributes
        var collDef = new CollectionDefinition
        {
            Vector = new VectorOptions
            {
                Dimension = 10
            }
        };

        // baseline 1: DEF
        CollectionDefinition result_bl_def = CollectionDefinition.CheckAddDefinitionsFromAttributes<CollMultiAttrObj_Zero>(collDef);
        Assert.Equal(10, result_bl_def.Vector.Dimension = 10);
        // baseline 2: VEC
        CollectionDefinition result_bl_vec = CollectionDefinition.CheckAddDefinitionsFromAttributes<CollMultiAttrObj_D_Vec>(new CollectionDefinition());
        Assert.Equal(10, result_bl_vec.Vector.Dimension = 10);
        // baseline 2: VZE
        CollectionDefinition result_bl_vze = CollectionDefinition.CheckAddDefinitionsFromAttributes<CollMultiAttrObj_D_Vze>(new CollectionDefinition());
        Assert.Equal(10, result_bl_vze.Vector.Dimension = 10);

        // DEF+VEC, agreement
        CollectionDefinition result_def_vec_agree = CollectionDefinition.CheckAddDefinitionsFromAttributes<CollMultiAttrObj_D_Vec>(collDef);
        Assert.Equal(10, result_def_vec_agree.Vector.Dimension = 10);
        // DEF+VZE, agreement
        CollectionDefinition result_def_vze_agree = CollectionDefinition.CheckAddDefinitionsFromAttributes<CollMultiAttrObj_D_Vze>(collDef);
        Assert.Equal(10, result_def_vze_agree.Vector.Dimension = 10);
        // VEC+VZE, agreement
        CollectionDefinition result_vec_vze_agree = CollectionDefinition.CheckAddDefinitionsFromAttributes<CollMultiAttrObj_D_VecVze>(new CollectionDefinition());
        Assert.Equal(10, result_vec_vze_agree.Vector.Dimension = 10);

        // DEF+VEC, clash!
        Assert.ThrowsAny<InvalidOperationException>(() =>
            CollectionDefinition.CheckAddDefinitionsFromAttributes<CollMultiAttrObj_D2_Vec>(collDef)
        );
        // DEF+VZE, clash!
        Assert.ThrowsAny<InvalidOperationException>(() =>
            CollectionDefinition.CheckAddDefinitionsFromAttributes<CollMultiAttrObj_D2_Vze>(collDef)
        );
        // VEC+VZE, clash!
        Assert.ThrowsAny<InvalidOperationException>(() =>
            CollectionDefinition.CheckAddDefinitionsFromAttributes<CollMultiAttrObj_D2_VecVze>(new CollectionDefinition())
        );

    }
}

// Classes to test collection creation with multiple dimension/similarity directives from the attributes
public class CollMultiAttrObj_Zero
{
    [DocumentId]
    public int? Id { get; set; }
    public string Name { get; set; }
}

[CollectionVector(SimilarityMetric.Euclidean, 10)]
public class CollMultiAttrObj_D_Vec
{
    [DocumentId]
    public int? Id { get; set; }
    public string Name { get; set; }
}

[CollectionVectorize("TheProvider", "TheModel", 10)]
public class CollMultiAttrObj_D_Vze
{
    [DocumentId]
    public int? Id { get; set; }
    public string Name { get; set; }
}

[CollectionVector(SimilarityMetric.Euclidean, 10)]
[CollectionVectorize("TheProvider", "TheModel", 10)]
public class CollMultiAttrObj_D_VecVze
{
    [DocumentId]
    public int? Id { get; set; }
    public string Name { get; set; }
}

[CollectionVector(SimilarityMetric.Euclidean, 11)]
public class CollMultiAttrObj_D2_Vec
{
    [DocumentId]
    public int? Id { get; set; }
    public string Name { get; set; }
}

[CollectionVectorize("TheProvider", "TheModel", 11)]
public class CollMultiAttrObj_D2_Vze
{
    [DocumentId]
    public int? Id { get; set; }
    public string Name { get; set; }
}

[CollectionVector(SimilarityMetric.Euclidean, 10)]
[CollectionVectorize("TheProvider", "TheModel", 11)]
public class CollMultiAttrObj_D2_VecVze
{
    [DocumentId]
    public int? Id { get; set; }
    public string Name { get; set; }
}
