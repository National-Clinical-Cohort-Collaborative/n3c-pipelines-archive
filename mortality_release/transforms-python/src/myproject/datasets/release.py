from transforms.api import transform_df, Input, Output, Markings
from myproject.datasets import utils

# removing the unreleased category marking for mortality and release dataset creation
@transform_df(
    Output("ri.foundry.main.dataset.75e4af77-b327-4152-968f-ab797cd92d41"),
    mortality_df=Input("ri.foundry.main.dataset.63e13a77-c9da-49a6-8366-afdf6e49bb06",
                                    stop_propagating=Markings(
                                        ["7e46faa7-1c35-4df9-92c8-77e0579666ba"],
                                        on_branches=utils.release_branches)
                    )
)
def compute_function(mortality_df):

    return mortality_df
