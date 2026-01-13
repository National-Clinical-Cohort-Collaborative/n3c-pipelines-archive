from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/UNITE/[RP-4A9E27] DI&H - Data Quality/datasets/Creatinine_days"),
    measurement=Input("ri.foundry.main.dataset.1937488e-71c5-4808-bee9-0f71fd1284ac"),
    concept_sets=Input("ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    canonical=Input("ri.foundry.main.dataset.09b4a60a-3da4-4754-8a7e-0b874e2a6f2b")
)
def compute(measurement, concept_sets, canonical):

    # for all the people who have creatinine, how many unique days were creatinine recorded on?
    # refinement continued in another transform

    targets = ["Creatinine, mg/dL"]
    canonical = canonical.filter(F.col("measured_variable").isin(targets))
    canonical = canonical.select("measured_variable", "codeset_id")
    canonical = canonical.withColumn("codeset_id", F.col("codeset_id").cast(IntegerType()))

    concept_sets = concept_sets.join(canonical, "codeset_id", "inner")
    concept_sets = concept_sets.select("measured_variable", "concept_id")

    measurement = measurement.join(concept_sets, measurement["measurement_concept_id"] == concept_sets["concept_id"], how = "inner")

    unique_days = measurement.groupBy("person_id", "measured_variable","data_partner_id").agg(F.countDistinct("measurement_date").cast("integer").alias("unique_days"))
    return unique_days
