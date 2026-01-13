from transforms.api import transform, Input, Output, configure
from transforms.verbs import dataframes as D
import myproject.datasets.input_sites as input_info
from pyspark.sql import functions as F
import os.path


sites_and_folder_prefix_pairs = [
    #(input_info.trinetx_sites_with_status, input_info.trinetx_folder_prefix),
    (input_info.omop_sites_with_status, input_info.omop_folder_prefix),
    #(input_info.act_sites_with_status, input_info.act_folder_prefix),
    (input_info.pcornet_sites_with_status, input_info.pcornet_folder_prefix),
    #(input_info.pedsnet_sites_with_status, input_info.pedsnet_folder_prefix)
]

inputs = {}
for site_status_dict, folder_prefix in sites_and_folder_prefix_pairs:     # Iterate over each CDM
    for site_num, has_passed in site_status_dict.items():                 # Iterate over each site
        site_subfolder = "Site " + str(site_num)
        inputs[str(site_num)] = Input(os.path.join(folder_prefix, site_subfolder, "transform/00 - unzipped/payload_filename"))


LINTER_APPLIED_PROFILES = [
    "KUBERNETES_NO_EXECUTORS",
    "DRIVER_MEMORY_MEDIUM",
]


@configure(profile=LINTER_APPLIED_PROFILES)
@transform(
    out=Output("ri.foundry.main.dataset.4dcda842-d615-4d60-99db-df0670e255ad"),
    data_partner_ids=Input("ri.foundry.main.dataset.3332ae36-a617-4ec4-bfde-b4d5a3cc6fbd"),
    **inputs
)
def my_compute_function(ctx, out, data_partner_ids, **inputs):
    df_list = []
    for site_num, payload_df in inputs.items():
        payload_df = payload_df.dataframe()
        payload_df = payload_df.withColumn("data_partner_id", F.lit(site_num))
        df_list.append(payload_df.select("data_partner_id", "payload"))

    df = D.union_many(*df_list)

    # Remove ".zip" suffix
    df = df.withColumn("payload", F.regexp_replace(F.col("payload"), "(?i)\\.zip", ""))

    # Remove duplicates
    df = df.withColumn("payload", F.trim(F.upper(F.col("payload"))))
    df_out = df.distinct()

    df_out = df_out.withColumn(
        "parsed_date_from_filename",
        F.to_date(F.regexp_extract(F.col('payload'), '(\\d{8})', 1), 'yyyyMMdd')
    )

    df_out = df_out.sort("data_partner_id")
    return out.write_dataframe(df_out)
