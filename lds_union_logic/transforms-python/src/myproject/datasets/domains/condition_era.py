from transforms.api import transform_df, Output, Check
from transforms import expectations as E
from myproject.datasets.utils import get_inputs, perform_union

domain = 'condition_era'
id_col = 'condition_era_id'
inputs = get_inputs(domain)


@transform_df(
    Output(
        "/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/LDS Union/unioned_{}".format(domain),
        checks=Check(E.primary_key(id_col), 'Valid primary key', on_error='FAIL')
    ),
    **inputs
)
def my_compute_function(ctx, **inputs):
    df_out = perform_union(ctx, domain, **inputs)
    # df_out = df_out.repartition(20)
    return df_out
