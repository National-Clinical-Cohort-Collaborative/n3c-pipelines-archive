from transforms.api import transform, incremental, Input, Output
from myproject.datasets import utils
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
import datetime
from collections import OrderedDict


DOMAINS = [
    "CARE_SITE",
    "CONDITION_ERA",
    "CONDITION_OCCURRENCE",
    "DEATH",
    "DRUG_ERA",
    "DRUG_EXPOSURE",
    "DEVICE_EXPOSURE",
    "LOCATION",
    "MEASUREMENT",
    "OBSERVATION",
    "OBSERVATION_PERIOD",
    "PAYER_PLAN_PERIOD",
    "PERSON",
    "PROCEDURE_OCCURRENCE",
    "PROVIDER",
    "VISIT_OCCURRENCE"
]
domain_inputs = {domain.lower(): Input("/UNITE/LDS/clean/{domain}".format(domain=domain.lower())) for domain in DOMAINS}


@incremental(snapshot_inputs=['manifest', *domain_inputs.keys()], semantic_version=2)
@transform(
    out=Output("/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/Released Sites DQ Regression Flags/domain_row_counts"),
    manifest=Input("/UNITE/LDS/clean/manifest_clean"),
    **domain_inputs
)
def my_compute_function(ctx, manifest, out, **domain_inputs):
    manifest = manifest.dataframe()
    released_sites = manifest.where(F.col("released")).select("data_partner_id", "cdm_name")
    lds_clean_payloads = domain_inputs["person"].dataframe().select("data_partner_id", "payload").distinct()

    counts_df = released_sites.join(lds_clean_payloads, "data_partner_id", "left")
    for domain, df in domain_inputs.items():
        # Filter to released sites
        df = df.dataframe().join(released_sites, "data_partner_id", "inner")
        # Compute row count by data partner
        df = df.groupBy("data_partner_id").count().withColumnRenamed("count", f"{domain}_rows")
        # Add this domain to the counts dataset
        counts_df = counts_df.join(df, "data_partner_id", "left")

    today = datetime.datetime.today().strftime('%Y-%m-%d')  # YYYY-MM-DD
    # Every payload in the current df is the most recent payload
    counts_df = utils.parse_date_from_payload_name(counts_df, payload_col="payload", new_col_name="payload_parsed_date")
    counts_df = counts_df.withColumnRenamed(
        "payload", "payload_name"
    ).withColumn(
        "payload_rank", F.lit(1).cast("integer")
    ).withColumn(
        "date_processed", F.lit(today).cast("date")
    ).withColumn(
        "is_most_recent_payload", F.lit(True).cast(T.BooleanType())
    ).withColumn(
        "data_partner_id", F.col("data_partner_id").cast("string")
    )

    schema_dict = OrderedDict([
        ("data_partner_id", T.StructField('data_partner_id', T.StringType(), True)),
        ("cdm_name", T.StructField('cdm_name', T.StringType(), True)),
        ("payload_name", T.StructField('payload_name', T.StringType(), True)),
        ("payload_parsed_date", T.StructField('payload_parsed_date', T.DateType(), True)),
        ("payload_rank", T.StructField('payload_rank', T.IntegerType(), True)),
        ("date_processed", T.StructField('date_processed', T.DateType(), True)),
        ("is_most_recent_payload", T.StructField('is_most_recent_payload', T.BooleanType(), True))
    ])
    for domain in domain_inputs.keys():
        schema_dict[f"{domain}_rows"] = T.StructField(f"{domain}_rows", T.LongType(), True)
        schema_dict[f"prev_{domain}_rows"] = T.StructField(f"prev_{domain}_rows", T.LongType(), True)
        schema_dict[f"{domain}_percent_change_from_previous_payload"] = T.StructField(f"{domain}_percent_change_from_previous_payload", T.DoubleType(), True)
        schema_dict[f"{domain}_percent_change_most_recent"] = T.StructField(f"{domain}_percent_change_most_recent", T.DoubleType(), True)

        counts_df = counts_df.withColumn(
            f"prev_{domain}_rows", F.lit(None).cast(T.LongType())
        ).withColumn(
            f"{domain}_percent_change_from_previous_payload", F.lit(None).cast(T.DoubleType())
        ).withColumn(
            f"{domain}_percent_change_most_recent", F.lit(None).cast(T.DoubleType())
        )

    counts_df = counts_df.select(*schema_dict.keys())
    counts_df.schema['payload_rank'].nullable = True
    counts_df.schema['is_most_recent_payload'].nullable = True

    if ctx.is_incremental:
        # Retreive existing version of the df
        cache = out.dataframe('previous', schema=counts_df.schema)

        # Remove current (most recent) payloads from previous snapshot
        cache_without_most_recent = cache.join(
            counts_df,
            (counts_df["payload_name"] == cache["payload_name"]),
            "left_anti"
        )
        cache_without_most_recent = cache_without_most_recent.withColumn("is_most_recent_payload", F.lit(None).cast(T.BooleanType()))

        # Union and re-compute columns: payload_rank, prev_covid_test_count, percent_change_from_previous_payload, percent_change_most_recent
        w = Window.partitionBy("data_partner_id").orderBy(F.col("date_processed").asc(), F.col("is_most_recent_payload"))
        df_out = cache_without_most_recent.unionByName(counts_df).withColumn(
            "payload_rank", F.row_number().over(w)
        )
        for domain in domain_inputs.keys():
            df_out = df_out.withColumn(
                f"prev_{domain}_rows", F.lag(f"{domain}_rows").over(w)
            ).withColumn(
                f"{domain}_percent_change_from_previous_payload", (F.col(f"{domain}_rows") - F.col(f"prev_{domain}_rows")) / F.col(f"prev_{domain}_rows")
            ).withColumn(
                f"{domain}_percent_change_most_recent", F.when(F.col("is_most_recent_payload"), F.col(f"{domain}_percent_change_from_previous_payload")).otherwise(F.lit(None))
            )

    else:
        df_out = counts_df

    df_out = df_out.na.fill(0).sort("data_partner_id", "payload_rank")
    df_out.cache().count()
    out.set_mode('replace')
    return out.write_dataframe(df_out)
