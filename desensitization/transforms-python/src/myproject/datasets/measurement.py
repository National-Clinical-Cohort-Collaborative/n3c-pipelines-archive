from transforms.api import transform_df, Input, Output, Markings, configure
from myproject.datasets.utils import get_patient_shifts, shift_date, clear_columns, update_measurement_time_col
from myproject.datasets import utils

LDS = "032e4f9b-276a-48dc-9fb8-0557136ffe47"


@configure(profile=['DYNAMIC_ALLOCATION_MAX_256', 'DRIVER_MEMORY_LARGE', 'SHUFFLE_PARTITIONS_LARGE', 'EXECUTOR_MEMORY_MEDIUM'])
@transform_df(
    Output("ri.foundry.main.dataset.f5cc487f-4d58-4adf-8ea5-434bc11bbe6f"),
    df=Input("ri.foundry.main.dataset.206d5649-f7cb-4c7f-ae86-c178c38f5e36", stop_propagating=Markings(LDS, on_branches=utils.release_branches)),
    date_shifts_df=Input("ri.foundry.main.dataset.60aa488e-dda0-4f28-babe-e6a9a50caa92", stop_propagating=Markings(LDS, on_branches=utils.release_branches))
)
def my_compute_function(df, date_shifts_df):
    df = df
    df = get_patient_shifts(df, date_shifts_df, broadcast=True)
    df = clear_columns(df, "measurement_time")

    date_cols = ["measurement_date"]
    timestamp_cols = ["measurement_datetime"]
    timestamp_date_col_pairs = list(zip(timestamp_cols, date_cols))

    df = clear_columns(df, *timestamp_cols)
    df = shift_date(df, *date_cols)

    # df = update_measurement_time_col(df, "measurement_datetime")

    df = df.drop("secret_time_shift_s", "secret_date_shift_days")
    return df
