from transforms.api import transform_df, Input, Output, Markings
from myproject.datasets import utils

LDS = "032e4f9b-276a-48dc-9fb8-0557136ffe47"

@transform_df(
    Output("ri.foundry.main.dataset.d87628fb-b9cd-449d-bddc-b463c272c9da"),
    my_input=Input("ri.foundry.main.dataset.545f8090-b78a-4667-a953-f7a6264534ef", stop_propagating=Markings(LDS, on_branches=utils.release_branches)),
)
def my_compute_function(my_input):
    df = my_input

    return df
