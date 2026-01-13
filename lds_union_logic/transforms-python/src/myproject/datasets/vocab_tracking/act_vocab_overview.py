from transforms.api import transform_df, Input, Output
from myproject.datasets.input_sites import act_folder_prefix, act_sites_with_status
from pyspark.sql import functions as F

inputs = {}
for act_site_num, status_flag in act_sites_with_status.items():
    if status_flag:
        act_site_num = str(act_site_num)
        inputs[act_site_num] = Input(f"{act_folder_prefix}/Site {act_site_num}/transform/03 - prepared/observation_fact")


@transform_df(
    Output("/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/Vocab and Xwalk Analysis/act_vocab_overview"),
    **inputs
)
def my_compute_function(**inputs):
    out = None
    for site_num, df in inputs.items():
        df = df.groupBy(
            "parsed_vocab_code", "parsed_vocab_code_in_vocab_map", "mapped_vocab_code", "mapped_vocab_code_override"
        ).count()
        df = df.withColumn("data_partner_id", F.lit(site_num))

        if out:
            out = out.unionByName(df)
        else:
            out = df

    return out
