from transforms.api import configure
from transforms.api import transform_df, Input, Output
from pyspark.sql import types as T
from pyspark.sql import functions as F


DOMAINS = [
    # "CARE_SITE",
    # "CONDITION_ERA",
    # "CONDITION_OCCURRENCE",
    # "DEATH",
    # "DRUG_ERA",
    # "DRUG_EXPOSURE",
    # "LOCATION",
    "MEASUREMENT",
    # "OBSERVATION",
    # "OBSERVATION_PERIOD",
    # "PAYER_PLAN_PERIOD",
    # "PERSON",
    # "PROCEDURE_OCCURRENCE",
    # "PROVIDER",
    # "VISIT_OCCURRENCE"
]
inputs = {}
for domain in DOMAINS:
    inputs[domain.lower()] = Input("/UNITE/Data Ingestion & OMOP Mapping/LDS Union/unioned_{domain}".format(domain=domain.lower()))


@configure(profile=['NUM_EXECUTORS_4', 'EXECUTOR_MEMORY_SMALL'])
@transform_df(
    Output("ri.foundry.main.dataset.48f7cb63-f047-44c1-9489-51b822beaf0b"),
    concept_df=Input("ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772"),
    **inputs,
)
def my_compute_function(concept_df, **inputs):
    final_df = None
    concept_df = concept_df.select("concept_id", "domain_id")
    for domain, df in inputs.items():
        for col in df.columns:
            if col.endswith("_concept_id"):
                # Create a df with all unique domain_ids for the concept_ids in this column 
                this_col_df = df.join(
                    concept_df,
                    df[col] == concept_df["concept_id"],
                    "left_outer"
                ).withColumn("domain", F.lit(domain)).withColumn("column", F.lit(col))
                this_col_df = this_col_df.select("domain", "column", "domain_id").distinct()
                if final_df:
                    final_df = final_df.unionByName(this_col_df)
                else:
                    final_df = this_col_df

    return final_df
