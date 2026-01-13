from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output, Markings
from myproject.utils import FINAL_MORTALITY_COLS


@transform_df(
    Output("ri.foundry.main.dataset.b3bbb7ad-9ed1-456b-886f-2b568b93e402"),
    mortality_all=Input("ri.foundry.main.dataset.3236fe6c-521b-4a03-a426-38e5900ad561"),
    data_partner_id_map=Input("ri.foundry.main.dataset.4d4cf17b-9dfb-48e8-bb19-4f62960b75ec"),
    pprl_participation=Input("ri.foundry.main.dataset.1dd5c179-f0f0-4bd3-8f26-7469c94fed2a",
                             stop_propagating=Markings(
                                ["5e003460-bb7d-4553-9049-922ad376eb23"],
                                on_branches=["master"]
                             ))
)
def compute(mortality_all, data_partner_id_map, pprl_participation):

    # Get sites who have opted-in to mortality PPRL data linkage
    pprl_participation = pprl_participation \
        .filter(F.col("feature_name") == "Mortality") \
        .filter(F.col("is_participating_yn") == "Yes")

    # Get institution ids
    mortality_all = mortality_all \
        .join(data_partner_id_map.select("data_partner_id", "institutionid"),
              on="data_partner_id",
              how="inner")

    # Filter to only sites that have opted-in to mortality PPRL data linkage
    mortality_filtered = mortality_all \
        .join(pprl_participation.select("institutionid"),
              on="institutionid",
              how="inner")

    return mortality_filtered.select(*FINAL_MORTALITY_COLS)
