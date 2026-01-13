from transforms.api import transform_df, Input, Output, Markings
from myproject.datasets import utils
from myproject.datasets.utils import clear_columns, handle_zip
from myproject.datasets import utils

LDS = "032e4f9b-276a-48dc-9fb8-0557136ffe47"

@transform_df(
    Output("ri.foundry.main.dataset.de701b3a-92dc-46d1-afde-1393519bc68d"),
    my_input=Input("ri.foundry.main.dataset.05d84305-8aec-4749-ad54-91a784fd36e4", stop_propagating=Markings(LDS, on_branches=utils.release_branches)),
)
def my_compute_function(my_input):
    df = my_input
    df = clear_columns(df, "city", "address_1", "address_2", "county")
    df = handle_zip(df, "zip")
    return df
