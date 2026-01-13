from transforms.api import transform_df, Input, Output, Markings
from myproject.datasets.utils import get_patient_shifts, shift_date
from myproject.datasets import utils

LDS = "032e4f9b-276a-48dc-9fb8-0557136ffe47"


@transform_df(
    Output("ri.foundry.main.dataset.8a476774-940d-4ac8-94d6-00c2c85d54df"),
    my_input=Input("ri.foundry.main.dataset.fe8ce820-7a24-4bf4-b0c6-53b1f3e945a4", stop_propagating=Markings(LDS, on_branches=utils.release_branches)),
    date_shifts_df=Input("ri.foundry.main.dataset.60aa488e-dda0-4f28-babe-e6a9a50caa92", stop_propagating=Markings(LDS, on_branches=utils.release_branches))
)
def my_compute_function(my_input, date_shifts_df):
    df = my_input
    df = get_patient_shifts(df, date_shifts_df)

    date_cols = ["condition_era_start_date", "condition_era_end_date"]
    df = shift_date(df, *date_cols)

    df = df.drop("secret_time_shift_s", "secret_date_shift_days")
    return df
