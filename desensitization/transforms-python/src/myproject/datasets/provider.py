from transforms.api import transform_df, Input, Output, Markings
from myproject.datasets import utils
from myproject.datasets.utils import clear_columns


LDS = "032e4f9b-276a-48dc-9fb8-0557136ffe47"

@transform_df(
    Output("ri.foundry.main.dataset.50faebb5-4692-4902-919b-f212cbdcdbd1"),
    my_input=Input("ri.foundry.main.dataset.09e5d408-d9c2-43fa-81ea-c69e3a8788f1", stop_propagating=Markings(LDS, on_branches=utils.release_branches)),
)
def my_compute_function(my_input):
    df = my_input
    df = clear_columns(df, "provider_name")
    return df
