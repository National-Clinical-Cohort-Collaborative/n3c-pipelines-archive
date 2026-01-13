from pyspark.sql import functions as F
from pyspark.sql.functions import *  # noqa: F403
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("ri.foundry.main.dataset.6b0d1b47-248a-4dc2-9d64-c994b07d3716"),
    measurements=Input("ri.foundry.main.dataset.1937488e-71c5-4808-bee9-0f71fd1284ac"),
    concept_sets=Input("ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    persons=Input("ri.foundry.main.dataset.456df227-b15d-4356-9df6-b113e90eb239"),
)
def grab_all_measurement_counts(persons, measurements, concept_sets):
    # list_of_features have four components:
    # ("name_of_the_feature", "concept_set_id")
    list_of_measurements = [
                    ("Urinalysis_Appearance", 606719061),
                    ("Urinalysis_Bilirubin", 829852285),
                    ("Urinalysis_Glucose", 361050403),
                    ("Urinalysis_Ketones", 633798619),
                    ("Urinalysis_Microscopic_Examination", 249676711),
                    ("Urinalysis_Nitrite_Urine", 65423430),
                    ("Urinalysis_RBCs_in_Urine", 800503498),
                    ("Urinalysis_Myoglobinuria", 441675694),
                    ("Urinalysis_Hemoglobinuria", 740013114),
                    ("Urinalysis_pH", 232278602),
                    ("Urinalysis_Protein", 905404327),
                    ("Urinalysis_Specific_Gravity", 249676711),
                    ("Urinalysis_Urine_Color", 824857049),
                    ("Urinalysis_Urobilinogen", 852838449),
                    ("Urinalysis_WBC_Esterase", 539209525)
                    # you can add more like:("SCR", 1234534545, "measurement", "measurement")
    ]
    # compute the total count
    df_union = None
    # Organize the features based on the "list_of_measurements"
    for feature, concept_set_id in list_of_measurements:
        ## filter down to one concept  # noqa: E266
        df_concepts = concept_sets.filter(F.col("codeset_id") == concept_set_id)\
                                    .selectExpr("codeset_id", "concept_id as measurement_concept_id").withColumn("Test", F.lit(feature))  # noqa: E501
        if df_union == None:
            df_union = df_concepts
        else:
            df_union = df_union.union(df_concepts)

    # Inner join concept and domain table
    df_feature = measurements.join(df_union, "measurement_concept_id", "inner")  # noqa: E501

    # count by site and join to the df_join
    # Assuming domain_uniq_id is the correct column name for unique IDs
    df_final = df_feature.groupBy("data_partner_id", "Test", "value_as_concept_id", "value_as_concept_name").agg((F.countDistinct("measurement_id").cast("integer").alias("measurment_counts")))
    return df_final
