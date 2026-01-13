from pyspark.sql import functions as F
from pyspark.sql.functions import *  # noqa: F403
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/UNITE/[RP-4A9E27] DI&H - Data Quality/datasets/aggregate_renal_features"),
    conditions=Input("ri.foundry.main.dataset.641c645d-eacf-4a5a-983d-3296e1641f0c"),
    drugs=Input("ri.foundry.main.dataset.3feda4dc-5389-4521-ab9e-0361ea5773fd"),
    procedures=Input("ri.foundry.main.dataset.2a3c8355-7fbf-478f-bbbf-eabb6733b04d"),
    concept_sets=Input("ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    persons=Input("ri.foundry.main.dataset.456df227-b15d-4356-9df6-b113e90eb239"),
)
def grab_all_feature_counts(persons, conditions, drugs, procedures, concept_sets):
    # list_of_features have four components:
    # ("name_of_the_feature", "concept_set_id", "domain", "domain_occurrence/exposure")
    list_of_features = [
                    ("aki", 692484006, "condition", "condition_occurrence"),
                    ("ckd", 889975596, "condition", "condition_occurrence"),
                    ("acei", 882330420, "drug", "drug_exposure"),
                    ("arb", 649931553, "drug", "drug_exposure"),
                    ("rrt", 777835196, "procedure", "procedure_occurrence")
                    # you can add more like:("SCR", 1234534545, "measurement", "measurement")
    ]
    # compute the total count
    df_join = persons.groupBy("data_partner_id").agg(F.countDistinct("person_id")
                                                            .cast("integer")
                                                                .alias("total_patient_count"))
    # Organize the features based on the "list_of_features"
    for feature, concept_set_id, domain, domain_uniq in list_of_features:
        ## filter down to one concept  # noqa: E266
        df_concepts = concept_sets.filter(F.col("codeset_id") == concept_set_id)\
                                    .selectExpr("codeset_id", f"concept_id as {domain}_concept_id")
        # Select the correct domain DataFrame
        domain_df = None
        if domain == "condition":
            domain_df = conditions
        elif domain == "drug":
            domain_df = drugs
        elif domain == "procedure":
            domain_df = procedures
        # elif domain == "measurement":
        #     domain_df = measurements
        else:
            raise ValueError(f"Unknown domain: {domain}")
        if domain_df is None:
            raise ValueError(f"Domain DataFrame for {domain} was not provided or is invalid.")

        # Inner join concept and domain table
        df_feature = domain_df.join(df_concepts, f"{domain}_concept_id", "inner")

        # count by site and join to the df_join
        # Assuming domain_uniq_id is the correct column name for unique IDs
        df_feature = df_feature.groupBy("data_partner_id").agg((F.countDistinct(f"{domain_uniq}_id")
                                                                    .cast("integer")
                                                                        .alias(f"{feature}_count")),
                                                                (F.countDistinct("person_id")
                                                                    .cast("integer")
                                                                        .alias(f"patients_with_{feature}"))
                                                                )

        df_join = df_join.join(df_feature, "data_partner_id", "left")
    return df_join
