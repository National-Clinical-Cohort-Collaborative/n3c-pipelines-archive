# from pyspark.sql import functions as F
from transforms.api import configure, transform_df, Input, Output


@configure(profile=['NUM_EXECUTORS_64'])
@transform_df(
    Output("ri.foundry.main.dataset.227c3494-02d4-4b7a-82da-a25bb5144267"),
    df_a=Input("ri.foundry.main.dataset.53d0cdaa-c42f-4017-b1e9-4c74f4b6d6a1"),
    df_b=Input("ri.foundry.main.dataset.0c3cab22-805d-4d84-98d6-4960651710bd"),
)
def compute(df_a, df_b):
    return df_a.unionByName(df_b)
