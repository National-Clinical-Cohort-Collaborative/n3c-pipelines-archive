from transforms.api import configure
from transforms.api import transform, incremental, Input, Output
from myproject.datasets import utils
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
import datetime


@incremental(snapshot_inputs=['measurement', 'concept_sets', 'manifest'], semantic_version=2)
@configure(profile=['NUM_EXECUTORS_8', 'EXECUTOR_MEMORY_MEDIUM'])
@transform(
    out=Output("ri.foundry.main.dataset.37e10bee-851a-4b04-a5bb-22dd8b1a80c7"),
    measurement=Input("ri.foundry.main.dataset.1937488e-71c5-4808-bee9-0f71fd1284ac"),
    concept_sets=Input("ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    manifest=Input("ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683")
)
def my_compute_function(ctx, measurement, concept_sets, manifest, out):
    measurement, concept_sets, manifest = measurement.dataframe(), concept_sets.dataframe(), manifest.dataframe()

    # Get all the COVID tests from the most recent codesets for PCR and Ab tests
    concept_sets = concept_sets.selectExpr("concept_id AS covid_test_concept_id")
    concept_sets_to_use = ['ATLAS SARS-CoV-2 rt-PCR and AG', 'Atlas #818 [N3C] CovidAntibody retry', 'CovidAmbiguous']
    concept_sets = concept_sets.where(
        (F.col("concept_set_name").isin(concept_sets_to_use)) &
        (F.col("is_most_recent_version") == True)
    )

    # Filter to released sites
    released_sites = manifest.where(F.col("released")).select("data_partner_id")
    released_measurements = measurement.join(released_sites, "data_partner_id", "inner")

    # Compute the total sum of all covid tests for each site
    covid_tests = released_measurements.join(
        concept_sets,
        released_measurements["measurement_concept_id"] == concept_sets["covid_test_concept_id"],
        "inner"
    ).groupBy(
        "data_partner_id", "payload",
    ).count().withColumnRenamed("count", "covid_test_count")

    schema = [
        "data_partner_id", "payload_name", "payload_parsed_date", "payload_rank", "date_processed",
        "prev_payload_name", "prev_payload_parsed_date",
        "covid_test_count", "prev_covid_test_count", "percent_change_from_previous_payload",
        "is_most_recent_payload", "percent_change_most_recent"
    ]
    today = datetime.datetime.today().strftime('%Y-%m-%d')  # YYYY-MM-DD
    # Every payload in the current df is the most recent payload
    covid_tests = utils.parse_date_from_payload_name(covid_tests, payload_col="payload", new_col_name="payload_parsed_date")
    covid_tests = covid_tests.withColumnRenamed(
        "payload", "payload_name"
    ).withColumn(
        "payload_rank", F.lit(1).cast("integer")
    ).withColumn(
        "date_processed", F.lit(today).cast("date")
    ).withColumn(
        "is_most_recent_payload", F.lit(True).cast(T.BooleanType())
    ).withColumn(
        "data_partner_id", F.col("data_partner_id").cast("string")
    ).withColumn(
        "percent_change_from_previous_payload", F.lit(None).cast(T.DoubleType())
    ).withColumn(
        "percent_change_most_recent", F.lit(None).cast(T.DoubleType())
    ).withColumn(
        "prev_covid_test_count", F.lit(None).cast(T.LongType())
    ).withColumn(
        "prev_payload_name", F.lit(None).cast("string")
    ).withColumn(
        "prev_payload_parsed_date", F.lit(None).cast("date")
    ).select(schema)

    if ctx.is_incremental:
        # Retreive existing version of the df
        cache = out.dataframe('previous', schema=T.StructType([
            T.StructField('data_partner_id', T.StringType(), True),
            T.StructField('payload_name', T.StringType(), True),
            T.StructField('payload_parsed_date', T.DateType(), True),
            T.StructField('payload_rank', T.IntegerType(), True),
            T.StructField('date_processed', T.DateType(), True),
            T.StructField('prev_payload_name', T.StringType(), True),
            T.StructField('prev_payload_parsed_date', T.DateType(), True),
            T.StructField('covid_test_count', T.LongType(), True),
            T.StructField('prev_covid_test_count', T.LongType(), True),
            T.StructField('percent_change_from_previous_payload', T.DoubleType(), True),
            T.StructField('is_most_recent_payload', T.BooleanType(), True),
            T.StructField('percent_change_most_recent', T.DoubleType(), True),
        ]))
        # Remove current (most recent) payloads from previous snapshot
        cache_without_most_recent = cache.join(
            covid_tests,
            (covid_tests["payload_name"] == cache["payload_name"]) & (covid_tests["covid_test_count"] == cache["covid_test_count"]),
            "left_anti"
        )
        cache_without_most_recent = cache_without_most_recent.withColumn("is_most_recent_payload", F.lit(None).cast(T.BooleanType()))

        # Union and re-compute columns: payload_rank, prev_covid_test_count, percent_change_from_previous_payload, percent_change_most_recent
        w = Window.partitionBy("data_partner_id").orderBy(F.col("date_processed").asc(), F.col("is_most_recent_payload"))
        df_out = cache_without_most_recent.unionByName(covid_tests).withColumn(
            "payload_rank", F.row_number().over(w)
        ).withColumn(
            "prev_covid_test_count", F.lag("covid_test_count").over(w)
        ).withColumn(
            "prev_payload_name", F.lag("payload_name").over(w)
        ).withColumn(
            "prev_payload_parsed_date", F.lag("payload_parsed_date").over(w)
        ).withColumn(
            "percent_change_from_previous_payload", (F.col("covid_test_count") - F.col("prev_covid_test_count")) / F.col("prev_covid_test_count")
        ).withColumn(
            "percent_change_most_recent", F.when(F.col("is_most_recent_payload"), F.col("percent_change_from_previous_payload")).otherwise(F.lit(None))
        ).select(schema)
    else:
        df_out = covid_tests

    df_out = df_out.sort("data_partner_id", "payload_rank")
    df_out.cache().count()
    out.set_mode('replace')
    return out.write_dataframe(df_out)
