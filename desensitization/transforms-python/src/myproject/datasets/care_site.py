from transforms.api import transform_df, Input, Output, Markings
from myproject.datasets import utils
from myproject.datasets.utils import clear_columns

LDS = "032e4f9b-276a-48dc-9fb8-0557136ffe47"

@transform_df(
    Output("ri.foundry.main.dataset.e6edc1e7-689c-45d4-bf80-2b19290edbef"),
    my_input=Input("ri.foundry.main.dataset.083469be-08a1-4205-a5a8-861b87d130d0", stop_propagating=Markings(LDS, on_branches=utils.release_branches))
)
def my_compute_function(my_input):
    df = my_input
    df = clear_columns(df, "care_site_name")
    return df
