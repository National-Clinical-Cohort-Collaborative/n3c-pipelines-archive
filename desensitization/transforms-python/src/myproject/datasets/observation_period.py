from transforms.api import transform_df, Input, Output, Markings
from myproject.datasets.utils import get_patient_shifts, shift_date
from myproject.datasets import utils

LDS = "032e4f9b-276a-48dc-9fb8-0557136ffe47"


@transform_df(
    Output("ri.foundry.main.dataset.5b6a5d5e-c544-4c2b-bc82-24c9441d4135"),
    my_input=Input("ri.foundry.main.dataset.48a4dacb-5d24-430f-ad94-0297ad5a7751", stop_propagating=Markings(LDS, on_branches=utils.release_branches)),
    date_shifts_df=Input("ri.foundry.main.dataset.60aa488e-dda0-4f28-babe-e6a9a50caa92", stop_propagating=Markings(LDS, on_branches=utils.release_branches))
)
def my_compute_function(my_input, date_shifts_df):
    df = my_input
    df = get_patient_shifts(df, date_shifts_df)

    date_cols = ["observation_period_start_date", "observation_period_end_date"]
    df = shift_date(df, *date_cols)

    df = df.drop("secret_time_shift_s", "secret_date_shift_days")
    return df
