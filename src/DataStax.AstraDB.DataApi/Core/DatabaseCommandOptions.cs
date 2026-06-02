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

namespace DataStax.AstraDB.DataApi.Core;

/// <summary>
/// Command options specific to the database level.
/// </summary>
public class DatabaseCommandOptions : CommandOptions
{
    /// <summary>
    /// The current keyspace to use for commands run against the database.
    /// 
    /// Defaults to the default keyspace for the database.
    /// </summary>
    public new string Keyspace
    {
        get => base.Keyspace;
        set => base.Keyspace = value;
    }

    static internal DatabaseCommandOptions BinaryMerge(DatabaseCommandOptions options1, DatabaseCommandOptions options2)
    {
        var options = CommandOptions.Merge(new CommandOptions[] {options1, options2});
        return FromCommandOptions(options);
    }

    /// <summary>
    /// Creates a new instance of <see cref="DatabaseCommandOptions"/> with default values.
    /// </summary>
    public DatabaseCommandOptions()
    {
    }

    /// <summary>
    /// Creates a new instance of <see cref="DatabaseCommandOptions"/> from a <see cref="CommandOptions"/> object.
    /// </summary>
    /// <param name="source">The source object from which draw values.</param>
    protected DatabaseCommandOptions(CommandOptions source) : base(source)
    {
    }

    static internal DatabaseCommandOptions FromCommandOptions(CommandOptions options)
    {
        return options == null ? null : new DatabaseCommandOptions(options);
    }

}