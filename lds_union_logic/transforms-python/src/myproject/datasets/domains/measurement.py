from transforms.api import configure, transform_df, Output, Check
from transforms import expectations as E
from myproject.datasets.utils import get_inputs, perform_union

domain = 'measurement'
id_col = 'measurement_id'
inputs = get_inputs(domain)


@configure(profile=['EXECUTOR_MEMORY_OVERHEAD_LARGE', 'NUM_EXECUTORS_16'])
@transform_df(
    Output(
        "/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/LDS Union/unioned_{}".format(domain),
        checks=Check(E.primary_key(id_col), 'Valid primary key', on_error='FAIL')
    ),
    **inputs
)
def my_compute_function(ctx, **inputs):
    df = perform_union(ctx, domain, **inputs)
    return df.repartition(3000)
