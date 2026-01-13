from transforms.api import transform_df, Input, Output, Markings
from myproject.datasets.utils import shift_date, get_patient_shifts
from myproject.datasets import utils

LDS = "032e4f9b-276a-48dc-9fb8-0557136ffe47"


@transform_df(
    Output("ri.foundry.main.dataset.cc793ade-7177-4d35-a312-d06e6c8c4461"),
    my_input=Input("ri.foundry.main.dataset.ae1d4947-5822-4856-8b91-d6cc10a077b5", stop_propagating=Markings(LDS, on_branches=utils.release_branches)),
    date_shifts_df=Input("ri.foundry.main.dataset.60aa488e-dda0-4f28-babe-e6a9a50caa92", stop_propagating=Markings(LDS, on_branches=utils.release_branches))
)
def my_compute_function(my_input, date_shifts_df):
    df = my_input
    df = get_patient_shifts(df, date_shifts_df)

    date_cols = ["drug_era_start_date", "drug_era_end_date"]
    df = shift_date(df, *date_cols)

    df = df.drop("secret_time_shift_s", "secret_date_shift_days")
    return df
