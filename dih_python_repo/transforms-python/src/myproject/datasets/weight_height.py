from pyspark.sql import functions as F
from pyspark.sql.functions import *  # noqa: F403
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/UNITE/[RP-4A9E27] DI&H - Data Quality/datasets/weight_height"),
    measurement=Input("ri.foundry.main.dataset.1937488e-71c5-4808-bee9-0f71fd1284ac"),
    observation=Input("ri.foundry.main.dataset.f925d142-3b25-498e-8538-4b9685c5270f"),
    concept_sets=Input("ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    persons=Input("ri.foundry.main.dataset.456df227-b15d-4356-9df6-b113e90eb239"),
)
def count_valid_height_weight(measurement, observation, concept_sets, persons):
    # height 1000053700
    # weight 1000005707
    df_concepts = concept_sets.where(F.col("codeset_id").isin({1000053700, 1000005707}))
    df_concepts = df_concepts.withColumn("Alias", F.when(F.col("codeset_id") == 1000053700, lit("height")).otherwise(lit("weight"))).selectExpr("concept_id", "Alias")  # noqa: E501

    df_meas = measurement.join(df_concepts.withColumnRenamed("concept_id", "measurement_concept_id"), "measurement_concept_id", "inner")\
                            .filter(F.col("value_as_number").isNotNull()).select("data_partner_id", "person_id", "Alias").distinct()

    df_obs = observation.join(df_concepts.withColumnRenamed("concept_id", "observation_concept_id"), "observation_concept_id", "inner")\
                            .filter(F.col("value_as_number").isNotNull()).select("data_partner_id", "person_id", "Alias").distinct()

    df_union = df_meas.union(df_obs).distinct().groupBy("data_partner_id").pivot("Alias").agg(F.countDistinct(F.col("person_id")).cast("integer"))  # noqa: E501

    df_final = persons.groupBy("data_partner_id").agg(F.countDistinct(F.col("person_id")).cast("integer").alias("total_count")).join(df_union, "data_partner_id", "left")  # noqa: E501
    
    return df_final
