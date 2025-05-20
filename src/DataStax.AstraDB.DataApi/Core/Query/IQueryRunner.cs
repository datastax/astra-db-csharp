
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Results;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Core.Query;

internal interface IQueryRunner<TBase, TSort> where TSort : SortBuilder<TBase>
{
    internal Task<ApiResponseWithData<DocumentsResult<TProjected>, FindStatusResult>> RunFindManyAsync<TProjected>(
        Filter<TBase> filter, IFindManyOptions<TBase, TSort> findOptions, CommandOptions commandOptions, bool runSynchronously)
        where TProjected : class;
}