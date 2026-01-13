from transforms.api import transform_df, Input, Output, Markings, configure
from myproject.datasets.utils import get_patient_shifts, shift_date, clear_columns
from myproject.datasets import utils

LDS = "032e4f9b-276a-48dc-9fb8-0557136ffe47"


@configure(profile=['NUM_EXECUTORS_16', 'EXECUTOR_MEMORY_MEDIUM'])
@transform_df(
    Output("ri.foundry.main.dataset.dc0beeda-6206-4cb8-8737-4bf1100adc0f"),
    my_input=Input("ri.foundry.main.dataset.2bc8edff-4a25-4584-8861-d8e26f277386", stop_propagating=Markings(LDS, on_branches=utils.release_branches)),
    date_shifts_df=Input("ri.foundry.main.dataset.60aa488e-dda0-4f28-babe-e6a9a50caa92", stop_propagating=Markings(LDS, on_branches=utils.release_branches))
)
def my_compute_function(my_input, date_shifts_df):
    df = my_input
    df = get_patient_shifts(df, date_shifts_df)

    date_cols = ["procedure_date"]
    timestamp_cols = ["procedure_datetime"]
    timestamp_date_col_pairs = list(zip(timestamp_cols, date_cols))

    df = clear_columns(df, *timestamp_cols)
    df = shift_date(df, *date_cols)

    df = df.drop("secret_time_shift_s", "secret_date_shift_days")
    return df
