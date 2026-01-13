from transforms.api import transform_df, Input, Output, Markings
from myproject.datasets.utils import get_patient_shifts, shift_date, clear_columns
from myproject.datasets import utils

LDS = "032e4f9b-276a-48dc-9fb8-0557136ffe47"


@transform_df(
    Output("ri.foundry.main.dataset.62028757-ada4-4c43-ae48-10fa582b32fa"),
    my_input=Input("ri.foundry.main.dataset.c875e60b-530f-41e3-847b-f10e7348ee4f", stop_propagating=Markings(LDS, on_branches=utils.release_branches)),
    date_shifts_df=Input("ri.foundry.main.dataset.60aa488e-dda0-4f28-babe-e6a9a50caa92", stop_propagating=Markings(LDS, on_branches=utils.release_branches))
)
def my_compute_function(my_input, date_shifts_df):
    df = my_input
    df = get_patient_shifts(df, date_shifts_df)

    date_cols = ["device_exposure_start_date", "device_exposure_end_date"]
    timestamp_cols = ["device_exposure_start_datetime", "device_exposure_end_datetime"]
    timestamp_date_col_pairs = list(zip(timestamp_cols, date_cols))

    df = clear_columns(df, *timestamp_cols)
    df = shift_date(df, *date_cols)

    df = df.drop("secret_time_shift_s", "secret_date_shift_days")
    return df
