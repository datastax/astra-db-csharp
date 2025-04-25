
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Results;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Core.Query;

internal interface IQueryRunner<TBase>
{
    internal Task<ApiResponseWithData<DocumentsResult<TProjected>, FindStatusResult>> RunFindManyAsync<TProjected>(
        Filter<TBase> filter, FindOptions<TBase> findOptions, CommandOptions commandOptions, bool runSynchronously);
}