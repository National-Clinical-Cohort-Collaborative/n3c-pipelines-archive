from transforms.api import transform_df, Input, Output, Markings
from myproject.utils import FINAL_MORTALITY_COLS


@transform_df(
    Output("ri.foundry.main.dataset.3236fe6c-521b-4a03-a426-38e5900ad561"),
    mortality_all=Input("ri.foundry.main.dataset.adee547f-dfd3-4893-9b1d-bf74240d02b7"),
    mortality_id_map=Input("ri.foundry.main.dataset.0c711c87-bc0a-4d53-ace7-d8f429e70dab",
                           stop_propagating=Markings(
                                ["16cff3b7-de46-4cbc-bd19-60177ab30620"],
                                on_branches=["master"]
                           ))
)
def compute(mortality_all, mortality_id_map, ctx):

    # Use LHB matches to determine n3c_person_id
    mortality_linked = mortality_all \
        .join(mortality_id_map.select(["datasource_type", "record_id", "data_partner_id", "n3c_person_id"]).distinct(),
              on=['datasource_type', 'record_id'],
              how="inner") \
        .withColumnRenamed("n3c_person_id", "person_id")

    # Only ONE person identifier column is permitted since we are removing the "PPRL Linkage Map: Mortality" marking
    return mortality_linked.select(*FINAL_MORTALITY_COLS)
