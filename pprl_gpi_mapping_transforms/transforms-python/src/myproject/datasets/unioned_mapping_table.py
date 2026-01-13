from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from functools import reduce


@transform_df(
    Output("ri.foundry.main.dataset.3bff9d3c-70e5-4090-93ca-c45d0ad9e856"),
    sites_only=Input("ri.foundry.main.dataset.de74563a-6fa9-4174-888c-a0ccb50fddf0"),
    cms_only=Input("ri.foundry.main.dataset.1606d82c-6d74-4d71-80ef-64532a5b453d")
)
def compute(sites_only, cms_only):
    sites_only = sites_only.select('gpi', 'n3c_person_id', 'site_person_id', 'institution', 'data_partner_id')
    cms_only = cms_only.select('gpi', 'n3c_person_id', 'site_person_id', 'institution', 'data_partner_id')
    dfs = [sites_only, cms_only]
    unioned_dfs = reduce(F.DataFrame.unionByName, dfs)
    return unioned_dfs
