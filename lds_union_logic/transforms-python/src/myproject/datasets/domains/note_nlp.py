from transforms.api import configure, transform_df, Output, Check
from transforms import expectations as E
from myproject.datasets.utils import get_inputs, perform_union

domain = 'note_nlp'
id_col = 'note_nlp_id'
inputs = get_inputs(domain, include_omop=True)  # , include_pcornet=True)
#  include_trinetx=False, include_act=False, include_pedsnet=False)


@configure(profile=['EXECUTOR_MEMORY_MEDIUM'])
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
