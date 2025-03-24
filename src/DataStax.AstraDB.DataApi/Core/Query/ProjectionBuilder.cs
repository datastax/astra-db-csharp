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

using DataStax.AstraDB.DataApi.Query;
using DataStax.AstraDB.DataApi.Utils;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace DataStax.AstraDB.DataApi.Core.Query;

public class ProjectionBuilder<T>
{
    public ProjectionBuilder() { }

    public InclusiveProjectionBuilder<T> Include(string field)
    {
        var builder = new InclusiveProjectionBuilder<T>();
        builder.Include(field);
        return builder;
    }

    public ExclusiveProjectionBuilder<T> Exclude(string field)
    {
        var builder = new ExclusiveProjectionBuilder<T>();
        builder.Exclude(field);
        return builder;
    }

    public InclusiveProjectionBuilder<T> Include<TField>(Expression<Func<T, TField>> fieldExpression)
    {
        var builder = new InclusiveProjectionBuilder<T>();
        builder.Include(fieldExpression);
        return builder;
    }

    public ExclusiveProjectionBuilder<T> Exclude<TField>(Expression<Func<T, TField>> fieldExpression)
    {
        var builder = new ExclusiveProjectionBuilder<T>();
        builder.Exclude(fieldExpression);
        return builder;
    }

}

public interface IProjectionBuilder
{
    internal List<Projection> Projections { get; }
}

public abstract class ProjectionBuilderBase<T, TBuilder> : IProjectionBuilder where TBuilder : ProjectionBuilderBase<T, TBuilder>
{
    protected readonly List<Projection> _projections = new List<Projection>();

    List<Projection> IProjectionBuilder.Projections => _projections;

    public TBuilder Include(SpecialField specialField)
    {
        _projections.Add(new Projection() { FieldName = specialField.ToString(), Include = true });
        return (TBuilder)this;
    }

    public TBuilder Exclude(SpecialField specialField)
    {
        _projections.Add(new Projection() { FieldName = specialField.ToString(), Include = false });
        return (TBuilder)this;
    }

}

public class InclusiveProjectionBuilder<T> : ProjectionBuilderBase<T, InclusiveProjectionBuilder<T>>
{
    internal InclusiveProjectionBuilder() { }

    public InclusiveProjectionBuilder<T> Include(string fieldName)
    {
        _projections.Add(new Projection() { FieldName = fieldName, Include = true });
        return this;
    }

    public InclusiveProjectionBuilder<T> Include<TField>(Expression<Func<T, TField>> fieldExpression)
    {
        _projections.Add(new Projection() { FieldName = fieldExpression.GetMemberNameTree(), Include = true });
        return this;
    }
}

public class ExclusiveProjectionBuilder<T> : ProjectionBuilderBase<T, ExclusiveProjectionBuilder<T>>
{
    internal ExclusiveProjectionBuilder() { }

    public ExclusiveProjectionBuilder<T> Exclude(string fieldName)
    {
        _projections.Add(new Projection() { FieldName = fieldName, Include = false });
        return this;
    }

    public ExclusiveProjectionBuilder<T> Exclude<TField>(Expression<Func<T, TField>> fieldExpression)
    {
        _projections.Add(new Projection() { FieldName = fieldExpression.GetMemberNameTree(), Include = false });
        return this;
    }
}
