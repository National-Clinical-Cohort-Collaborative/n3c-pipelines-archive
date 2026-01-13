from transforms.api import transform_df, Input, Output, Markings
from myproject.datasets.utils import shift_date, get_patient_shifts
from myproject.datasets import utils

LDS = "032e4f9b-276a-48dc-9fb8-0557136ffe47"


@transform_df(
    Output("ri.foundry.main.dataset.15b1cc4b-7680-4e76-ad2c-f2badb53cd77"),
    my_input=Input("ri.foundry.main.dataset.e8cb687b-2958-4145-8ab6-b1836811598d", stop_propagating=Markings(LDS, on_branches=utils.release_branches)),
    date_shifts_df=Input("ri.foundry.main.dataset.60aa488e-dda0-4f28-babe-e6a9a50caa92", stop_propagating=Markings(LDS, on_branches=utils.release_branches))
)
def my_compute_function(my_input, date_shifts_df):
    df = my_input
    df = get_patient_shifts(df, date_shifts_df)

    date_cols = ["payer_plan_period_start_date", "payer_plan_period_end_date"]
    df = shift_date(df, *date_cols)

    df = df.drop("secret_time_shift_s", "secret_date_shift_days")
    return df
