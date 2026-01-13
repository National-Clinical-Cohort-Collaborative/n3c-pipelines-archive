from transforms.api import transform_df, Input, Output
import pyspark.sql.functions as F
import datetime as dt


@transform_df(
    Output("ri.foundry.main.dataset.a956594c-7d67-488a-98b5-fb67b899c824"),
    visit_occurrence=Input("ri.foundry.main.dataset.ebfde1bd-4847-4fda-9924-5a67ed6074ef"),
)
def my_compute_function(visit_occurrence):
    df = visit_occurrence
    # harmonize dates (upper and lower limit)
    currentdate = dt.datetime.now()
    df = df.withColumn("visit_start_date", F.when((df.visit_start_date > (currentdate + dt.timedelta(days=730))) | (df.visit_start_date < (currentdate - dt.timedelta(days=3650))), None).otherwise(df.visit_start_date))
    df = df.withColumn("visit_end_date", F.when((df.visit_end_date > (currentdate + dt.timedelta(days=730))) | (df.visit_end_date < (currentdate - dt.timedelta(days=3650))), None).otherwise(df.visit_end_date))
    return df
