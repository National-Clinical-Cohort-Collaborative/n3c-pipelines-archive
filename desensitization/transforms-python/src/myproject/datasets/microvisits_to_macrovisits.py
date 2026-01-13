from transforms.api import configure, transform_df, Input, Output, Markings
from myproject.datasets import utils
from myproject.datasets.utils import shift_date, clear_columns

LDS = "032e4f9b-276a-48dc-9fb8-0557136ffe47"


@configure(profile=['NUM_EXECUTORS_8', 'EXECUTOR_MEMORY_MEDIUM'])
@transform_df(
    Output("ri.foundry.main.dataset.d9dab4a6-cc64-41e2-a78e-73b71ed561ba"),
    my_input=Input("ri.foundry.main.dataset.d69f1c1d-50e7-4401-8fa8-d6fe29e71eb6", stop_propagating=Markings(LDS, on_branches=utils.release_branches)),
    date_shifts_df=Input("ri.foundry.main.dataset.60aa488e-dda0-4f28-babe-e6a9a50caa92", stop_propagating=Markings(LDS, on_branches=utils.release_branches)),
)

def my_compute_function(my_input, date_shifts_df):
    df = my_input

    df = utils.get_patient_shifts(df, date_shifts_df)
    date_cols = ['macrovisit_start_date', 'macrovisit_end_date', 'visit_start_date', 'visit_end_date']
    df = shift_date(df, *date_cols)

    df = df.drop("secret_time_shift_s", "secret_date_shift_days")

    # clear datetime columns from SH visit datasets due to an inability to fuzz the time and safely keep the date the same
    timestamp_cols = ["visit_start_datetime", "visit_end_datetime"]
    df = clear_columns(df, *timestamp_cols)

    return df
