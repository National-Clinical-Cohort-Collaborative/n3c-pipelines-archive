from transforms.api import transform_df, Output, Check
from transforms import expectations as E
from myproject.datasets.utils import get_inputs, perform_union

domain = 'payer_plan_period'
id_col = 'payer_plan_period_id'
inputs = get_inputs(domain, include_pcornet=True, include_omop=False)
# include_pedsnet=False, include_act=False, include_trinetx=False)


@transform_df(
    Output(
        "/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/LDS Union/unioned_{}".format(domain),
        checks=Check(E.primary_key(id_col), 'Valid primary key', on_error='FAIL')
    ),
    **inputs
)
def my_compute_function(ctx, **inputs):
    df_out = perform_union(ctx, domain, **inputs)
    # df_out = df_out.repartition(10)
    return df_out
