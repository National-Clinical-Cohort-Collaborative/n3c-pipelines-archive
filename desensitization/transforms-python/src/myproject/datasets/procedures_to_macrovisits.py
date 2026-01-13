from transforms.api import transform_df, Input, Output, Markings
from myproject.datasets import utils
from myproject.datasets.utils import shift_date


LDS = "032e4f9b-276a-48dc-9fb8-0557136ffe47"


@transform_df(
    Output("ri.foundry.main.dataset.e2bc9b82-bdfe-4e76-913a-c61db12cbc8e"),
    my_input=Input("ri.foundry.main.dataset.273ee2ad-3da7-4fad-84c6-afe9966f564b"),
    date_shifts_df=Input("ri.foundry.main.dataset.60aa488e-dda0-4f28-babe-e6a9a50caa92", stop_propagating=Markings(LDS, on_branches=utils.release_branches)),
)
def my_compute_function(my_input, date_shifts_df):
    df = my_input

    df = utils.get_patient_shifts(df, date_shifts_df)

    date_cols = ['procedure_date', 'macrovisit_start_date', 'macrovisit_end_date']
    df = shift_date(df, *date_cols)

    df = df.drop("secret_time_shift_s", "secret_date_shift_days")

    return df