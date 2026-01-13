from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("ri.foundry.main.dataset.ffb10942-92ed-48c1-a01c-fef1b4ee5552"),
    gpi_table=Input("ri.foundry.main.dataset.2ee3d79e-98e1-4b0a-96ba-7f1cfe19ac8f")
)
def compute(gpi_table):
    gpi_table = gpi_table.withColumn("split", F.split("pseudo_id", ":"))
    gpi_table = gpi_table.withColumn("institution", F.col("split").getItem(0))
    gpi_table = gpi_table.withColumn("site_person_id", F.col("split").getItem(1))
    gpi_table = gpi_table.drop('split')

    return gpi_table
