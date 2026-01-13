from pyspark.sql import functions as F
from pyspark.sql import types as T
from transforms.api import configure, transform_df, Input, Output, Markings
from myproject.datasets import utils
from myproject.datasets.utils import clear_columns, get_patient_shifts, shift_date


LDS = "032e4f9b-276a-48dc-9fb8-0557136ffe47"


@configure(profile=['NUM_EXECUTORS_16', 'EXECUTOR_MEMORY_OVERHEAD_MEDIUM'])
@transform_df(
    Output("ri.foundry.main.dataset.ca924c5d-0674-49f5-92f4-5c4c8bd82a9d"),
    my_input=Input("ri.foundry.main.dataset.8908ccc6-29fb-4711-91aa-80d0004f45b9", stop_propagating=Markings(LDS, on_branches=utils.release_branches)),
    date_shifts_df=Input("ri.foundry.main.dataset.60aa488e-dda0-4f28-babe-e6a9a50caa92", stop_propagating=Markings(LDS, on_branches=utils.release_branches))
)
def my_compute_function(my_input, date_shifts_df):
    df = my_input
    df = get_patient_shifts(df, date_shifts_df)

    # Always set birth_datetime = null
    df = clear_columns(df, *["birth_datetime"])

    # Split person table into two groups for processing; "Null Year of Birth" and "Non-null Year of Birth"
    df_null_dob = df.filter(F.col("year_of_birth").isNull())
    df_non_null_dob = df.filter(F.col("year_of_birth").isNotNull())

    # PROCESS NULL YEAR OF BIRTH ----------------------------------------------------------------------
    # If year_of_birth is null, month_of_birth and day_of_birth should also be null
    df_null_dob = clear_columns(df_null_dob, *["month_of_birth", "day_of_birth"])
    df_null_dob = df_null_dob.withColumn("is_age_90_or_older", F.lit(None))

    # PROCESS NON-NULL YEAR OF BIRTH ------------------------------------------------------------------
    # Assume month_of_birth==1 and day_of_birth==1 if null
    # Error on the side of them being older to protect them as soon as they *might* be >=90 years of age
    df_non_null_dob = df_non_null_dob \
        .withColumn("month_of_birth", F.coalesce(F.col("month_of_birth"), F.lit(1))) \
        .withColumn("day_of_birth", F.coalesce(F.col("day_of_birth"), F.lit(1)))

    # Shift DOB
    df_non_null_dob = df_non_null_dob \
        .withColumn("temp_dob", F.to_date(F.concat_ws("-", "year_of_birth", "month_of_birth", "day_of_birth")))
    df_non_null_dob = shift_date(df_non_null_dob, *["temp_dob"])

    # Force shifted day_of_birth == 1
    # day_of_birth will be null for users, so make them as old as possibly assumed for age calculations
    df_non_null_dob = df_non_null_dob \
        .withColumn("temp_dob", F.to_date(F.concat_ws("-", F.year("temp_dob"), F.month("temp_dob"), F.lit(1))))

    # Calculate shifted age and flag if >= 89 years old
    # Cut off is inentionally placed at 89 instead of 90 because we must protect the person based on their true age,
    # even if their shifted age makes them appear younger.
    df_non_null_dob = df_non_null_dob \
        .withColumn("temp_age", F.months_between(F.current_date(), F.col("temp_dob")) / F.lit(12).cast(T.DoubleType())) \
        .withColumn("is_age_90_or_older", F.lit(F.col("temp_age") >= F.lit(89))) \
        .drop("temp_age")

    # Update DOB columns with the new shifted values, redacting data when is_age_90_or_older==True
    df_non_null_dob = df_non_null_dob \
        .withColumn("year_of_birth", F.when(F.col("is_age_90_or_older"), F.lit(None))
                                      .otherwise(F.year("temp_dob"))) \
        .withColumn("month_of_birth", F.when(F.col("is_age_90_or_older"), F.lit(None))
                                       .otherwise(F.month("temp_dob"))) \
        .withColumn("day_of_birth", F.lit(None)) \
        .drop("temp_dob")

    # UNION ---------------------------------------------------------------------------------
    # Union back together after processing as two separate groups
    df = df_null_dob.unionByName(df_non_null_dob)
    df = df.drop("secret_time_shift_s", "secret_date_shift_days")

    return df
