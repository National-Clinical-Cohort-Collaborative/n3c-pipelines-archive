from transforms.api import transform_df, Output, Check, configure
from transforms import expectations as E
from myproject.datasets.utils import get_inputs, perform_union

domain = 'control_map'
id_col = 'control_map_id'
#inputs = get_inputs(domain, include_pedsnet=False)
inputs = get_inputs(domain)


@configure(profile=['NUM_EXECUTORS_8'])
@transform_df(
    Output(
        "/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/LDS Union/unioned_{}".format(domain),
        checks=Check(E.primary_key(id_col), 'Valid primary key', on_error='FAIL')
    ),
    **inputs
)
def my_compute_function(ctx, **inputs):
    df_out = perform_union(ctx, domain, **inputs)
    df_out = df_out.coalesce(50)
    return df_out
