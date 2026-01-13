from transforms.api import transform_df, Input, Output, Markings, configure
from myproject.datasets.utils import get_patient_shifts, shift_date, clear_columns
from myproject.datasets import utils

LDS = "032e4f9b-276a-48dc-9fb8-0557136ffe47"


@configure(profile=['DYNAMIC_ALLOCATION_MAX_16', 'DRIVER_MEMORY_LARGE', 'EXECUTOR_MEMORY_MEDIUM', 'SHUFFLE_PARTITIONS_LARGE'])
@transform_df(
    Output("ri.foundry.main.dataset.3bcd0206-ba2b-490d-aace-2304bea9d954"),
    my_input=Input("ri.foundry.main.dataset.3e5e4e33-93c9-405d-bd5c-810d217abee7", stop_propagating=Markings(LDS, on_branches=utils.release_branches)),
    date_shifts_df=Input("ri.foundry.main.dataset.60aa488e-dda0-4f28-babe-e6a9a50caa92", stop_propagating=Markings(LDS, on_branches=utils.release_branches))
)
def my_compute_function(my_input, date_shifts_df):
    df = my_input
    df = get_patient_shifts(df, date_shifts_df, broadcast=True)

    date_cols = ["drug_exposure_start_date", "drug_exposure_end_date"]
    timestamp_cols = ["drug_exposure_start_datetime", "drug_exposure_end_datetime"]
    timestamp_date_col_pairs = list(zip(timestamp_cols, date_cols))

    df = clear_columns(df, *timestamp_cols)
    df = shift_date(df, *date_cols)
    df = shift_date(df, "verbatim_end_date")

    df = df.drop("secret_time_shift_s", "secret_date_shift_days")
    df = clear_columns(df, "verbatim_end_date")
    return df
