from transforms.api import transform, Input, Output
from pyspark.sql.functions import rand, round
from pyspark.sql import types as T
from pyspark.sql import functions as F

'''
Objective
Create Shift date lookup table based on Enclave-specific Global Person ID (eGPI)
'''

@transform(
    date_shifted=Output("ri.foundry.main.dataset.60aa488e-dda0-4f28-babe-e6a9a50caa92"),
    source_df=Input("ri.foundry.main.dataset.8908ccc6-29fb-4711-91aa-80d0004f45b9"),
)
def compute(source_df, date_shifted):
    # create a consolidated GPI/personID column upon which to perform the hash
    source_df = source_df.dataframe().select("global_person_id", "person_id").distinct()

    source_df = source_df.withColumn("global_person_id", F.when(F.col("global_person_id").contains("No Duplicate or Linkage Found"),
                                     F.lit(None)).otherwise(F.col("global_person_id")))

    source_df = source_df.withColumn("consolidated_person_identifier", F.coalesce(F.col("global_person_id"),
                                                                           F.col("person_id")))
    # unique shift per consolidated person identifier (coalesced gpi and person id)
    df = source_df.select("consolidated_person_identifier").distinct()

    dateShiftMin = -180
    dateShiftMax = 179
    df = df.withColumn("secret_date_shift_days", round(rand()*(dateShiftMax-dateShiftMin)+dateShiftMin, 0).cast(T.IntegerType()))
    df = df.withColumn("secret_date_shift_days", F.when(df.secret_date_shift_days >= 0, df.secret_date_shift_days + 1).otherwise(df.secret_date_shift_days))

    timeShiftMin = -10800
    timeShiftMax = 10799
    df = df.withColumn("secret_time_shift_s", F.round(F.rand()*(timeShiftMax-timeShiftMin)+timeShiftMin, 0).cast(T.IntegerType()))
    df = df.withColumn("secret_time_shift_s", F.when(df.secret_time_shift_s >= 0, df.secret_time_shift_s + 1).otherwise(df.secret_time_shift_s))

    # join back in the person id col
    df_date_shifted = source_df.join(df, source_df.consolidated_person_identifier == df.consolidated_person_identifier, "left") \
                               .select("person_id", "secret_date_shift_days", "secret_time_shift_s") \

    return date_shifted.write_dataframe(df_date_shifted, output_format='parquet', options={'noho': "true"})
