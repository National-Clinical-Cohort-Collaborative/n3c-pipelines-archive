from pyspark.sql import functions as F
from pyspark.sql import types as T
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("ri.foundry.main.dataset.15816de8-e81f-4452-932c-7fbadeecdcd4"),
    mapping_table=Input("ri.foundry.main.dataset.3bff9d3c-70e5-4090-93ca-c45d0ad9e856"),
    data_partner_ref_table=Input("ri.foundry.main.dataset.3332ae36-a617-4ec4-bfde-b4d5a3cc6fbd"),
    pprl_site_opt_ins=Input("ri.foundry.main.dataset.7b29e936-f131-469f-9507-23d431c3456c")
)

# Temporary solution: TO DO: ask RI/NCATS for a dataset containing the sites that signed up for PPRL deduplication
def compute(mapping_table, data_partner_ref_table, pprl_site_opt_ins):


    df = mapping_table.select('data_partner_id').dropDuplicates()
    df = df.withColumn('data_partner_id', F.col('data_partner_id').cast(T.IntegerType()))
    df = df.filter(F.col('data_partner_id') != 666)

    # Get sites that are participating in PPRL (so they allow at least intra-site de-duplication)
    # this logic should be simplified if we start relying on institution ror ID
    sites_info = data_partner_ref_table.filter(~F.col('site_name').contains('TESTING')).select('data_partner_id', 'site_name', 'institutionid').dropDuplicates()
    site_ids = sites_info.withColumnRenamed('site_name', 'institutionname')
    opt_in_sites = pprl_site_opt_ins.select('institutionid', 'institutionname')
    opt_in_sites = opt_in_sites.join(site_ids.select('data_partner_id', 'institutionname'), 'institutionname', 'left')
    site_ids = site_ids.withColumnRenamed('data_partner_id', 'data_partner_id_2')
    opt_in_sites = opt_in_sites.join(site_ids.select('data_partner_id_2', 'institutionid'), 'institutionid', 'left')
    opt_in_sites = opt_in_sites.withColumn('data_partner_id', F.coalesce(F.col('data_partner_id'), F.col('data_partner_id_2')))
    opt_in_sites = opt_in_sites.select('data_partner_id').filter(F.col('data_partner_id').isNotNull()).dropDuplicates()

    df = df.unionByName(opt_in_sites).dropDuplicates()

    return df
