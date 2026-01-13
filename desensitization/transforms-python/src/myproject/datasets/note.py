from transforms.api import transform_df, Input, Output, Markings
from myproject.datasets.utils import clear_columns, shift_date, get_patient_shifts
from myproject.datasets import utils

LDS = "032e4f9b-276a-48dc-9fb8-0557136ffe47"


@transform_df(
    Output("ri.foundry.main.dataset.993a0f8c-0d4f-4c9b-afc5-8f0ba856ade9"),
    my_input=Input("ri.foundry.main.dataset.b55047ac-6449-46f1-b4cf-f0dcf0451a86", stop_propagating=Markings(LDS, on_branches=utils.release_branches)),
    date_shifts_df=Input("ri.foundry.main.dataset.60aa488e-dda0-4f28-babe-e6a9a50caa92", stop_propagating=Markings(LDS, on_branches=utils.release_branches))
)
def my_compute_function(my_input, date_shifts_df):
    df = my_input
    df = get_patient_shifts(df, date_shifts_df)

    date_cols = ["note_date"]
    timestamp_cols = ["note_datetime"]
    timestamp_date_col_pairs = list(zip(timestamp_cols, date_cols))

    df = clear_columns(df, *timestamp_cols)
    df = shift_date(df, *date_cols)

    df = df.drop("secret_time_shift_s", "secret_date_shift_days")
    return df
