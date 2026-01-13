from transforms.api import configure, transform_df, Input, Output, Markings
from myproject.datasets import utils
from myproject.datasets.utils import shift_date, clear_columns

LDS = "032e4f9b-276a-48dc-9fb8-0557136ffe47"

@configure(profile=['NUM_EXECUTORS_8', 'EXECUTOR_MEMORY_MEDIUM'])
@transform_df(
    Output("ri.foundry.main.dataset.11a469b3-e1c8-4b9a-9aef-8f3af428f5d4"),
    my_input=Input("ri.foundry.main.dataset.51e24554-cd36-421b-984d-880ed3dc7d1b", stop_propagating=Markings(LDS, on_branches=utils.release_branches)),
    date_shifts_df=Input("ri.foundry.main.dataset.60aa488e-dda0-4f28-babe-e6a9a50caa92", stop_propagating=Markings(LDS, on_branches=utils.release_branches))
)
def visit_occurrence(my_input, date_shifts_df):
    df = my_input

    df = utils.get_patient_shifts(df, date_shifts_df)

    date_cols = ['visit_start_date', 'visit_end_date']
    timestamp_cols = ["visit_start_datetime", "visit_end_datetime"]
    
    df = clear_columns(df, *timestamp_cols)
    df = shift_date(df, *date_cols)

    df = df.drop("secret_time_shift_s", "secret_date_shift_days")
    return df
