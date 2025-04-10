
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

using DataStax.AstraDB.DataApi.SerDes;
using MongoDB.Bson;
using System;
using System.Linq;
using System.Reflection;

namespace DataStax.AstraDB.DataApi.Utils;

internal static class InsertValidator
{
    internal static T Validate<T>(T document)
    {
        if (document == null)
            throw new ArgumentNullException(nameof(document), "Document cannot be null.");

        PropertyInfo idProperty = GetIdProperty(typeof(T));
        if (idProperty == null)
        {
            // Will not error, but will not be able to deserialize the id
            return document;
        }

        object idValue = idProperty.GetValue(document);
        Type idType = idProperty.PropertyType;

        bool isNullOrEmpty = IsNullOrEmpty(idValue, idType);

        if (isNullOrEmpty)
        {
            if (idType != typeof(Guid) && idType != typeof(Guid?) && idType != typeof(ObjectId) && idType != typeof(ObjectId?))
            {
                throw new InvalidOperationException(
                    $"The _id property '{idProperty.Name}' of type '{idType}' is null or empty. " +
                    "Auto-generated ids are guids. Provide a valid _id value or use an object with a Guid _id property.");
            }
        }

        return document;
    }

    private static PropertyInfo GetIdProperty(Type type)
    {
        PropertyInfo explicitId = type.GetProperties()
            .FirstOrDefault(p => p.Name.Equals("_id", StringComparison.OrdinalIgnoreCase) && p.CanWrite);

        if (explicitId != null)
            return explicitId;

        return type.GetProperties()
            .FirstOrDefault(p => p.GetCustomAttribute<DocumentMappingAttribute>()?.Field == DocumentMappingField.Id && p.CanWrite);
    }

    private static bool IsNullOrEmpty(object value, Type type)
    {
        if (value == null)
            return true;

        if (type == typeof(string))
        {
            return string.IsNullOrEmpty((string)value);
        }
        if (type == typeof(Guid))
        {
            return (Guid)value == Guid.Empty;
        }
        if (type == typeof(Guid?))
        {
            return !((Guid?)value).HasValue || ((Guid?)value).Value == Guid.Empty;
        }

        return false;
    }
}