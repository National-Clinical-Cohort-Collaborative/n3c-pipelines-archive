from transforms.api import transform_df, Input, Output, configure
import pyspark.sql.functions as F
from myproject.datasets import utils


LINTER_APPLIED_PROFILES = [
    "KUBERNETES_NO_EXECUTORS",
    "DRIVER_MEMORY_MEDIUM",
]


@configure(profile=LINTER_APPLIED_PROFILES)
@transform_df(
    Output("/UNITE/[RP-4A9E27] DI&H - Data Quality/manifest ontology/manifest"),
    my_input=Input("/UNITE/LDS/clean/manifest_clean"),
    clean_payloads=Input("/UNITE/[RP-4A9E27] DI&H - Data Quality/manifest ontology/dqp_payload_tracker")
)
def my_compute_function(my_input, clean_payloads):
    df = my_input
    df = df.withColumnRenamed("n3c_site_id","site_id")

    # Get payload for each site that was used for DQP tests
    df = df.join(clean_payloads, "data_partner_id", "left")
    # Provide payload status
    df = df.withColumn(
        "portal_status",
        F.when(F.col("run_date") == "[Not yet processed]", F.lit(None))\
        .when(F.col("parsed_payload") != F.col("unreleased_payload"), F.lit("Processing issue"))\
        .when((F.col("parsed_payload") == F.col("unreleased_payload")) & (F.col("unreleased_payload") != F.col("dq_portal_payload")), F.lit("DQP Loading"))\
        .when((F.col("parsed_payload") == F.col("unreleased_payload")) & (F.col("unreleased_payload") == F.col("dq_portal_payload")), F.lit("Up-to-date"))
    )

    return df
