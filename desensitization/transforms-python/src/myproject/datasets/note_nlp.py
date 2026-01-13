from transforms.api import transform_df, Input, Output, Markings, configure
from myproject.datasets.utils import release_branches, clear_columns

LDS = "032e4f9b-276a-48dc-9fb8-0557136ffe47"

@configure(profile=["DYNAMIC_ALLOCATION_MAX_64"])
@transform_df(
    Output("ri.foundry.main.dataset.c7cf119c-06e9-446f-8daf-3a61458d1fd6"),
    my_input=Input("ri.foundry.main.dataset.bb0b226c-a552-4a58-a0be-587cd55f48eb", stop_propagating=Markings(LDS, on_branches=release_branches))
)
def my_compute_function(my_input):
    df = my_input

    date_cols = ["nlp_date"]
    timestamp_cols = ["nlp_datetime"]

    # These times represent when the NLP pipeline was run on the note data, so is irrelevant to researchers. Nulling it
    # out per Chris Chute. - tschwab
    df = clear_columns(df, *date_cols, *timestamp_cols)

    return df
