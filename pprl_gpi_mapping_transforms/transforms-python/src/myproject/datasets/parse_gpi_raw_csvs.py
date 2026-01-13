from transforms.api import Input, Output, transform
from source_cdm_utils.parse import parse_csv_latest
from pyspark.sql import types as T

# Define the schema
schema = T.StructType([
    T.StructField("pseudo_id", T.StringType(), True),
    T.StructField("gpi", T.StringType(), True)
])


@transform(
    out=Output("ri.foundry.main.dataset.2ee3d79e-98e1-4b0a-96ba-7f1cfe19ac8f"),
    source_df=Input("ri.foundry.main.dataset.2d9cd2e3-054e-4d90-987a-2b95ecba6806"),
)
def compute(source_df, out, ctx):
    regex = "(?i).*gpi_.*\\.csv"
    # ensure we parse only the latest csv file from RI
    parse_csv_latest(source_df, regex, schema, out, ctx)
