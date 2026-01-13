# from pyspark.sql import functions as F
from transforms.api import configure, transform_df, Input, Output


@configure(profile=['NUM_EXECUTORS_64'])
@transform_df(
    Output("ri.foundry.main.dataset.9592371d-4579-47c4-873a-0b0507c6c619"),
    df_a=Input("ri.foundry.main.dataset.9d927f90-4b58-4802-96d4-d840c7814a9b"),
    df_b=Input("ri.foundry.main.dataset.2e55bdef-6143-4ea4-bc68-ac989a82f252")
)
def compute(df_a, df_b):
    return df_a.unionByName(df_b)
