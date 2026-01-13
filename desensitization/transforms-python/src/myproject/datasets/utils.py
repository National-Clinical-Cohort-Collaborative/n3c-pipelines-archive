from pyspark.sql import functions as F
from pyspark.sql import types as T


release_branches = [
    "master",
]


def get_dtype(df, col):
    return [dtype for name, dtype in df.dtypes if name == col][0]


def clear_columns(df, *cols):
    cast_cols = [F.lit(None).cast(get_dtype(df, col)).alias(col) for col in cols]
    og_cols = [c for c in df.columns if c not in cols]
    df = df.select(*cast_cols, *og_cols)
    return df


def get_patient_shifts(df, date_shifts_df, broadcast=False):
    """
    Joins a dateshift and a timeshift column onto df, and returns the result.

    df: LDS OMOP Domain dataframe
    date_shifts_df: map of person_id to a date shift and a time shift.
    """
    # date_shifts_df = date_shifts_df.withColumnRenamed("PERSON_ID", "person_id")
    if broadcast:
        date_shifts_df = F.broadcast(date_shifts_df)
    df = df.join(date_shifts_df, "person_id")
    return df


def shift_time(df, *timestamp_date_col_pairs):
    """
    Shifts the timestamp column by number of s determined by the secret_time_shift_s column, and updates all
    date_cols, if necessary, to account for date boundaries that may have been crossed as a result of this shift.

    Note: secret_time_shift_s is in rand(-10800s, +10800s) \ {0}  (i.e. rand(-180min, +180min) \ {0})

    df: LDS OMOP Domain data, with an additional secret_time_shift_s column.
    timestamp_date_col_pairs: List of tuples in the form (timestamp_column_name, date_column_name) to match datetime
        columns to their corresponding date columns. The second object in the tuple can be a single column name or an
        iterable of multiple column names.
    """

    for timestamp_col, date_col in timestamp_date_col_pairs:

        # Shift timestamp column by number of days given by the random dateshift
        df = df.withColumn(timestamp_col, (F.unix_timestamp(timestamp_col) + (F.col("secret_date_shift_days")*24*60*60)).cast("timestamp"))

        # Generate per-event random fuzz. +/- 30 seconds, excluding 0.
        timeFuzzSecondsMin = -30
        timeFuzzSecondsMax = 29
        df = df.withColumn("secret_time_fuzz_s", F.round(F.rand()*(timeFuzzSecondsMax-timeFuzzSecondsMin)+timeFuzzSecondsMin, 0).cast(T.IntegerType()))
        df = df.withColumn("secret_time_fuzz_s", F.when(df.secret_time_fuzz_s >= 0, df.secret_time_fuzz_s + 1).otherwise(df.secret_time_fuzz_s))

        # Generate per-event random fuzz. +/- 10 minutes, excluding 0.
        timeFuzzMinutesMin = -10
        timeFuzzMinutesMax = 9
        df = df.withColumn("secret_time_fuzz_m", F.round(F.rand()*(timeFuzzMinutesMax-timeFuzzMinutesMin)+timeFuzzMinutesMin, 0).cast(T.IntegerType()))
        df = df.withColumn("secret_time_fuzz_m", F.when(df.secret_time_fuzz_m >= 0, df.secret_time_fuzz_m + 1).otherwise(df.secret_time_fuzz_m))

        df = df.withColumn("secret_time_shift_s", df.secret_time_shift_s + df.secret_time_fuzz_s + (60*df.secret_time_fuzz_m))

        # Shift the timestamp column by secret_time_shift_s seconds
        df = df.withColumn("unshifted_time_date", df[timestamp_col].cast("date"))
        df = df.withColumn(timestamp_col, (F.unix_timestamp(timestamp_col) + F.col("secret_time_shift_s")).cast("timestamp"))

        # Calculate date difference (-1, 0, or +1) to check if this time shift crosses a date boundary
        df = df.withColumn("shifted_time_date", df[timestamp_col].cast("date"))
        df = df.withColumn("date_delta", F.datediff("shifted_time_date", "unshifted_time_date"))

        # If timestamp isn't null, update the corresponding date column(s) to keep in sync with the timestamp column, which
        # may have crossed a date boundary.
        # Allow for date_col to be a single column name or an iterable of columns.
        date_col_list = []
        if isinstance(date_col, str):
            date_col_list.append(date_col)
        else:
            date_col_list = date_col

        for col_name in date_col_list:
            df = df.withColumn(col_name, F.coalesce(
                F.expr("date_add(" + col_name + ", date_delta)")
                , df[col_name]
            ))

    df = df.drop("unshifted_time_date", "shifted_time_date", "date_delta", "secret_time_fuzz_m", "secret_time_fuzz_s")
    return df


def update_measurement_time_col(df, datetime_col):
    df = df.withColumn("measurement_time", F.from_unixtime(F.unix_timestamp(F.col(datetime_col)), format="HH:mm"))
    return df


def shift_date(df, *cols):
    for col in cols:
        df = df.withColumn(col, F.expr("date_add("+col+", secret_date_shift_days)"))
    return df


# Reduce zip codes to 3 digits and remove zip codes with populations of fewer than 20,000
restricted_zips = ["036", "059", "102", "203", "556", "692", "821", "823", "878", "879", "884", "893", "205", "369"]
def handle_zip(df, *cols):
    for col in cols:
        df = df.withColumn(col, F.when(df[col].substr(1, 3).isin(restricted_zips), F.lit("000")).otherwise(df[col].substr(1, 3)))
    # Clear county, state, and location_source_value columns for restricted zips
    # Records with restricted zips will only have location_id, zip = "000", and data_partner_id
    restricted_zip_cleared_cols = ["county", "location_source_value", "state"]
    for col in restricted_zip_cleared_cols:
        df = df.withColumn(col, F.when(df["zip"] == "000", F.lit(None).cast(get_dtype(df, col))).otherwise(df[col]))
    return df
