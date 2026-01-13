from transforms.api import transform_df, Input, Output, Check, Markings
from transforms import expectations as E
from myproject.datasets.utils import get_inputs, perform_union
from pyspark.sql import functions as F
from pyspark.sql.window import Window

domain = 'person'
id_col = 'person_id'
inputs = get_inputs(domain)


@transform_df(
    Output(
        "/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/LDS Union/unioned_{}".format(domain),
        checks=Check(E.primary_key(id_col), 'Valid primary key', on_error='FAIL')
    ),
    global_person_id_mapping=Input("ri.foundry.main.dataset.8f2a521d-4c60-495b-a5aa-85de8ab683ba"),
    dedup_sites=Input("ri.foundry.main.dataset.15816de8-e81f-4452-932c-7fbadeecdcd4", 
                      stop_propagating=Markings(["5e003460-bb7d-4553-9049-922ad376eb23"], on_branches=["master"])),
    **inputs
)
def my_compute_function(ctx, global_person_id_mapping, dedup_sites, **inputs):
    df_out = perform_union(ctx, domain, **inputs)
    df_out = df_out.repartition(10)

    # Logic to add the columns 'global_person_id' and 'duplication_type'
    df_out = df_out.join(global_person_id_mapping, 'person_id', 'left')

    # Get sites that are participating in PPRL (so they allow at least intra-site de-duplication)
    dedup_sites = dedup_sites.withColumn('is_participating_in_pprl', F.lit(True))
    df_out = df_out.join(dedup_sites, 'data_partner_id', 'left')
    df_out = df_out.withColumn('is_participating_in_pprl', F.when(F.col('is_participating_in_pprl').isNull(), F.lit(False)).otherwise(F.col('is_participating_in_pprl')))
    df_out = df_out.withColumn('duplication_type', F.when(F.col('is_participating_in_pprl') == False, F.lit('Unknown: data partner not participating in linkage')).otherwise(F.lit('No Duplicate or Linkage Found')))

    df_out = df_out.withColumn('global_person_id', F.coalesce(F.col('global_person_id'), F.col('duplication_type')))
    df_out = df_out.drop('is_participating_in_pprl', 'duplication_type')

    return df_out
