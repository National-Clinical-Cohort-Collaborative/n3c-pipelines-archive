from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("ri.foundry.main.dataset.23047c61-a5c7-4021-97c5-dbe6d99ed17c"),
    source_df=Input("ri.foundry.main.dataset.3bff9d3c-70e5-4090-93ca-c45d0ad9e856"),
)
def compute(source_df):
    return source_df.exceptAll(source_df.dropDuplicates(["gpi", "n3c_person_id", "site_person_id", "data_partner_id", "institution"]))  # noqa