from transforms.api import transform_df, Output, Check, configure
from transforms import expectations as E
from myproject.datasets.utils import get_inputs, perform_union

domain = 'procedure_occurrence'
id_col = 'procedure_occurrence_id'
inputs = get_inputs(domain)


@configure(profile=['EXECUTOR_MEMORY_OVERHEAD_MEDIUM', 'NUM_EXECUTORS_8'])
@transform_df(
    Output(
        "/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/LDS Union/unioned_{}".format(domain),
        checks=Check(E.primary_key(id_col), 'Valid primary key', on_error='FAIL')
    ),
    **inputs
)
def my_compute_function(ctx, **inputs):
    df_out = perform_union(ctx, domain, **inputs)
    # df_out = df_out.repartition(60)
    return df_out
