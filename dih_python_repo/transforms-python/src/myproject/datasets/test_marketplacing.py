from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/UNITE/[RP-4A9E27] DI&H - Data Quality/datasets/Test_Marketplacing_Table"),
    canonical=Input("ri.foundry.main.dataset.09b4a60a-3da4-4754-8a7e-0b874e2a6f2b")
)
def compute(canonical):
    return canonical
