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

using DataStax.AstraDB.DataApi.Utils;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace DataStax.AstraDB.DataApi.Core.Query;

/// <summary>
/// A utility for specifying projections to apply to the results of an operation.
/// </summary>
/// <typeparam name="T"></typeparam>
public class ProjectionBuilder<T>
{
    /// <summary>
    /// Create an inclusive projection by specifying a field to include.
    /// </summary>
    /// <param name="field">The field to include in the projection.</param>
    /// <returns>The projection builder.</returns>
    /// <remarks>
    /// We recommend using the <see cref="Include{TField}"/> method instead.
    /// </remarks>
    public InclusiveProjectionBuilder<T> Include(string field)
    {
        var builder = new InclusiveProjectionBuilder<T>();
        builder.Include(field);
        return builder;
    }

    /// <summary>
    /// Create an exclusive projection by specifying a field to exclude.
    /// </summary>
    /// <param name="field">The field to exclude from the projection.</param>
    /// <returns>The projection builder.</returns>
    /// <remarks>
    /// We recommend using the <see cref="Exclude{TField}"/> method instead.
    /// </remarks>
    public ExclusiveProjectionBuilder<T> Exclude(string field)
    {
        var builder = new ExclusiveProjectionBuilder<T>();
        builder.Exclude(field);
        return builder;
    }

    /// <summary>
    /// Create an inclusive projection by specifying a field to include.
    /// </summary>
    /// <typeparam name="TField">The type of the field to include.</typeparam>
    /// <param name="fieldExpression">The field to include in the projection.</param>
    /// <returns>The projection builder.</returns>
    public InclusiveProjectionBuilder<T> Include<TField>(Expression<Func<T, TField>> fieldExpression)
    {
        var builder = new InclusiveProjectionBuilder<T>();
        builder.Include(fieldExpression);
        return builder;
    }

    /// <summary>
    /// Create an exclusive projection by specifying a field to exclude.
    /// </summary>
    /// <typeparam name="TField">The type of the field to exclude.</typeparam>
    /// <param name="fieldExpression">The field to exclude from the projection.</param>
    /// <returns>The projection builder.</returns>
    public ExclusiveProjectionBuilder<T> Exclude<TField>(Expression<Func<T, TField>> fieldExpression)
    {
        var builder = new ExclusiveProjectionBuilder<T>();
        builder.Exclude(fieldExpression);
        return builder;
    }

}

/// <summary>
/// Interface for a projection builder.
/// </summary>
public interface IProjectionBuilder
{
    internal List<Projection> Projections { get; }
}

/// <summary>
/// Base class for projection builders.
/// Not intended to be used directly.
/// </summary>
/// <typeparam name="T">The type of the document.</typeparam>
/// <typeparam name="TBuilder">The type of the projection builder.</typeparam>
public abstract class ProjectionBuilderBase<T, TBuilder> : IProjectionBuilder where TBuilder : ProjectionBuilderBase<T, TBuilder>
{
    internal readonly List<Projection> _projections = new List<Projection>();

    List<Projection> IProjectionBuilder.Projections => _projections;

}

/// <summary>
/// A builder for inclusive projections.
/// </summary>
/// <typeparam name="T">The type of the document.</typeparam>
public class InclusiveProjectionBuilder<T> : ProjectionBuilderBase<T, InclusiveProjectionBuilder<T>>
{
    internal InclusiveProjectionBuilder() { }

    /// <summary>
    /// Specify a field to include in the projection.
    /// </summary>
    /// <param name="fieldName">The name of the field to include.</param>
    /// <returns>The projection builder.</returns>
    public InclusiveProjectionBuilder<T> Include(string fieldName)
    {
        _projections.Add(new Projection() { FieldName = fieldName, Include = true });
        return this;
    }

    /// <summary>
    /// Specify a field to include in the projection.
    /// </summary>
    /// <typeparam name="TField">The type of the field to include.</typeparam>
    /// <param name="fieldExpression">The field to include in the projection.</param>
    /// <returns>The projection builder.</returns>
    public InclusiveProjectionBuilder<T> Include<TField>(Expression<Func<T, TField>> fieldExpression)
    {
        _projections.Add(new Projection() { FieldName = fieldExpression.GetMemberNameTree(), Include = true });
        return this;
    }
}

/// <summary>
/// A builder for exclusive projections.
/// </summary>
/// <typeparam name="T">The type of the document.</typeparam>
public class ExclusiveProjectionBuilder<T> : ProjectionBuilderBase<T, ExclusiveProjectionBuilder<T>>
{
    internal ExclusiveProjectionBuilder() { }

    /// <summary>
    /// Specify a field to exclude from the projection.
    /// </summary>
    /// <param name="fieldName">The name of the field to exclude.</param>
    /// <returns>The projection builder.</returns>
    public ExclusiveProjectionBuilder<T> Exclude(string fieldName)
    {
        _projections.Add(new Projection() { FieldName = fieldName, Include = false });
        return this;
    }

    /// <summary>
    /// Specify a field to exclude from the projection.
    /// </summary>
    /// <typeparam name="TField">The type of the field to exclude.</typeparam>
    /// <param name="fieldExpression">The field to exclude from the projection.</param>
    /// <returns>The projection builder.</returns>
    public ExclusiveProjectionBuilder<T> Exclude<TField>(Expression<Func<T, TField>> fieldExpression)
    {
        _projections.Add(new Projection() { FieldName = fieldExpression.GetMemberNameTree(), Include = false });
        return this;
    }
}
