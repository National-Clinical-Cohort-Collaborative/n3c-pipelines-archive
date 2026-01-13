from transforms.api import transform_df, Input, Output, Markings, configure
from myproject.datasets import utils
from myproject.datasets.utils import shift_date


LDS = "032e4f9b-276a-48dc-9fb8-0557136ffe47"


@configure(profile=['NUM_EXECUTORS_8', 'EXECUTOR_MEMORY_MEDIUM'])
@transform_df(
    Output("ri.foundry.main.dataset.79bad16c-9ef2-4553-9683-f99868164cff"),
    my_input=Input("ri.foundry.main.dataset.55154600-c918-4d52-b52c-62e2868674d3"),
    date_shifts_df=Input("ri.foundry.main.dataset.60aa488e-dda0-4f28-babe-e6a9a50caa92", stop_propagating=Markings(LDS, on_branches=utils.release_branches)),
)
def my_compute_function(my_input, date_shifts_df):
    df = my_input

    df = utils.get_patient_shifts(df, date_shifts_df)

    date_cols = ['measurement_date', 'macrovisit_start_date', 'macrovisit_end_date']
    df = shift_date(df, *date_cols)

    df = df.drop("secret_time_shift_s", "secret_date_shift_days")

    return df