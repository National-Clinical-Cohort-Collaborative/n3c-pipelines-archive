from transforms.api import transform_df, Input, Output, Markings
from myproject.datasets import utils

LDS = "032e4f9b-276a-48dc-9fb8-0557136ffe47"


@transform_df(
    Output("ri.foundry.main.dataset.5702968d-ffc2-4b3b-afbe-fce4a3cf234b"),
    my_input=Input("ri.foundry.main.dataset.6730e0d3-e65e-44e8-9fe2-cc3497c3d6dc", stop_propagating=Markings(LDS, on_branches=utils.release_branches))
)
def my_compute_function(my_input):
    df = my_input
    return df
