
using DataStax.AstraDB.DataApi.Utils;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace DataStax.AstraDB.DataApi.Core.Query;

/// <summary>
/// A builder for creating filter definitions for table queries
/// </summary>
/// <typeparam name="T"></typeparam>
public class TableFilterBuilder<T> : FilterBuilder<T>
{
    /// <summary>
    /// Creates a filter that matches rows where the keys of a map column contain any of the specified keys.
    /// </summary>
    /// <typeparam name="T2"></typeparam>
    /// <param name="fieldName"></param>
    /// <param name="array"></param>
    /// <returns></returns>
    public Filter<T> KeysIn<T2>(string fieldName, T2[] array)
    {
        return new Filter<T>(fieldName, FilterOperator.Keys, new Filter<T>(FilterOperator.In, array));
    }

    /// <summary>
    /// Creates a filter that matches rows where the keys of a map column contain any of the specified keys.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TVal"></typeparam>
    /// <param name="expression"></param>
    /// <param name="array"></param>
    /// <returns></returns>
    public Filter<T> KeysIn<TKey, TVal>(Expression<Func<T, Dictionary<TKey, TVal>>> expression, TKey[] array)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.Keys, new Filter<T>(FilterOperator.In, array));
    }

    /// <summary>
    /// Creates a filter that matches rows where the keys of a map column contain all of the specified keys.
    /// </summary>
    /// <typeparam name="T2"></typeparam>
    /// <param name="fieldName"></param>
    /// <param name="array"></param>
    /// <returns></returns>
    public Filter<T> KeysAll<T2>(string fieldName, T2[] array)
    {
        return new Filter<T>(fieldName, FilterOperator.Keys, new Filter<T>(FilterOperator.All, array));
    }

    /// <summary>
    /// Creates a filter that matches rows where the keys of a map column contain all of the specified keys.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TVal"></typeparam>
    /// <param name="expression"></param>
    /// <param name="array"></param>
    /// <returns></returns>
    public Filter<T> KeysAll<TKey, TVal>(Expression<Func<T, Dictionary<TKey, TVal>>> expression, TKey[] array)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.Keys, new Filter<T>(FilterOperator.All, array));
    }

    /// <summary>
    /// Creates a filter that matches rows where the keys of a map column do not contain any of the specified keys.
    /// </summary>
    /// <typeparam name="T2"></typeparam>
    /// <param name="fieldName"></param>
    /// <param name="array"></param>
    /// <returns></returns>
    public Filter<T> KeysNin<T2>(string fieldName, T2[] array)
    {
        return new Filter<T>(fieldName, FilterOperator.Keys, new Filter<T>(FilterOperator.NotIn, array));
    }

    /// <summary>
    /// Creates a filter that matches rows where the keys of a map column do not contain any of the specified keys.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TVal"></typeparam>
    /// <param name="expression"></param>
    /// <param name="array"></param>
    /// <returns></returns>
    public Filter<T> KeysNin<TKey, TVal>(Expression<Func<T, Dictionary<TKey, TVal>>> expression, TKey[] array)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.Keys, new Filter<T>(FilterOperator.NotIn, array));
    }

    /// <summary>
    /// Creates a filter that matches rows where the values of a map column contain any of the specified values.
    /// </summary>
    /// <typeparam name="TValue"></typeparam>
    /// <param name="fieldName"></param>
    /// <param name="array"></param>
    /// <returns></returns>
    public Filter<T> ValuesIn<TValue>(string fieldName, TValue[] array)
    {
        return new Filter<T>(fieldName, FilterOperator.Values, new Filter<T>(FilterOperator.In, array));
    }

    /// <summary>
    /// Creates a filter that matches rows where the values of a map column contain any of the specified values.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TVal"></typeparam>
    /// <param name="expression"></param>
    /// <param name="array"></param>
    /// <returns></returns>
    public Filter<T> ValuesIn<TKey, TVal>(Expression<Func<T, Dictionary<TKey, TVal>>> expression, TVal[] array)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.Values, new Filter<T>(FilterOperator.In, array));
    }

    /// <summary>
    /// Creates a filter that matches rows where the values of a map column contain all of the specified values.
    /// </summary>
    /// <typeparam name="TValue"></typeparam>
    /// <param name="fieldName"></param>
    /// <param name="array"></param>
    /// <returns></returns>
    public Filter<T> ValuesAll<TValue>(string fieldName, TValue[] array)
    {
        return new Filter<T>(fieldName, FilterOperator.Values, new Filter<T>(FilterOperator.All, array));
    }

    /// <summary>
    /// Creates a filter that matches rows where the values of a map column contain all of the specified values.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    /// <param name="expression"></param>
    /// <param name="array"></param>
    /// <returns></returns>
    public Filter<T> ValuesAll<TKey, TValue>(Expression<Func<T, Dictionary<TKey, TValue>>> expression, TValue[] array)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.Values, new Filter<T>(FilterOperator.All, array));
    }

    /// <summary>
    /// Creates a filter that matches rows where the values of a map column do not contain any of the specified values.
    /// </summary>
    /// <typeparam name="T2"></typeparam>
    /// <param name="fieldName"></param>
    /// <param name="array"></param>
    /// <returns></returns>
    public Filter<T> ValuesNin<T2>(string fieldName, T2[] array)
    {
        return new Filter<T>(fieldName, FilterOperator.Values, new Filter<T>(FilterOperator.NotIn, array));
    }

    /// <summary>
    /// Creates a filter that matches rows where the values of a map column do not contain any of the specified values.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    /// <param name="expression"></param>
    /// <param name="array"></param>
    /// <returns></returns>
    public Filter<T> ValuesNin<TKey, TValue>(Expression<Func<T, Dictionary<TKey, TValue>>> expression, TValue[] array)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.Values, new Filter<T>(FilterOperator.NotIn, array));
    }

    /// <summary>
    /// Lexical match operator -- Matches rows where the rows's lexical field value is a lexicographical match to the specified string of space-separated keywords or terms
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Filter<T> TableLexicalMatch(string fieldName, string value)
    {
        return new Filter<T>(fieldName, FilterOperator.Match, value);
    }

    /// <summary>
    /// Lexical match operator -- Matches rows where the rows's lexical field value is a lexicographical match to the specified string of space-separated keywords or terms
    /// </summary>
    /// <param name="value"></param>
    /// <param name="expression"></param>
    /// <returns></returns>
    public Filter<T> TableLexicalMatch<TKey, TValue>(Expression<Func<T, TValue>> expression, string value)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.Match, value);
    }

}