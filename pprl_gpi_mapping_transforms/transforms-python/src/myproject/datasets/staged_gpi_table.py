# from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("ri.foundry.main.dataset.e1dd6c95-3c46-4758-9d17-af5502f0d92b"),
    source_df=Input("ri.foundry.main.dataset.3bff9d3c-70e5-4090-93ca-c45d0ad9e856"),
)
def compute(source_df):
    # Renamed in order to align with OMOP standards
    source_df = source_df.withColumnRenamed('n3c_person_id', 'person_id').withColumnRenamed('gpi', 'global_person_id')

    # A distinct added to remove duplicates created by removing the site identifiers
    return source_df.select('global_person_id', 'person_id', 'data_partner_id').distinct()
