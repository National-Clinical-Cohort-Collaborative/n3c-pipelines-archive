from transforms.api import transform, Input, Output, configure
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
        inputs[str(site_num)] = Input(os.path.join(folder_prefix, site_subfolder, "metadata/control_map"))

"""
TriNetX:
case_patient_id, buddy_num, control_patient_id,
case_age, case_sex, case_race, case_ethnicity,
control_age, control_sex, control_race, control_ethnicity

OMOP:
case_person_id, buddy_num, control_person_id

ACT:
case_patid, buddy_num, control_patid,
case_age, case_sex, case_race, case_ethn,
control_age, control_sex, control_race, control_ethn

PCORnet:
case_patid, buddy_num, control_patid,
case_age, case_sex, case_race, case_ethn,
control_age, control_sex, control_race, control_ethn
"""

col_name_mapping = {
    "case_patient_id": "case_source_id",
    "case_person_id": "case_source_id",
    "case_patid": "case_source_id",
    "control_patient_id": "control_source_id",
    "control_person_id": "control_source_id",
    "control_patid": "control_source_id",
    "case_ethn": "case_ethnicity",
    "control_ethn": "control_ethnicity"
}

out_schema = [
    "case_source_id",
    "buddy_num",
    "control_source_id",
    "case_age",
    "case_sex",
    "case_race",
    "case_ethnicity",
    "control_age",
    "control_sex",
    "control_race",
    "control_ethnicity",
    "data_partner_id"
]


LINTER_APPLIED_PROFILES = [
    "KUBERNETES_NO_EXECUTORS",
    "DRIVER_MEMORY_MEDIUM",
]


@configure(profile=LINTER_APPLIED_PROFILES)
@transform(
    out=Output("/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/LDS Union/unioned_harmonized_control_map"),
    data_partner_ids=Input("ri.foundry.main.dataset.3332ae36-a617-4ec4-bfde-b4d5a3cc6fbd"),
    **inputs
)
def my_compute_function(out, data_partner_ids, **inputs):
    final_df = None
    for site_num, input_df in inputs.items():
        df = input_df.dataframe()
        df = df.withColumn("data_partner_id", F.lit(site_num))

        # Remap columns to harmonized names
        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, col_name.lower())
            if col_name in col_name_mapping:
                new_name = col_name_mapping[col_name]
                df = df.withColumnRenamed(col_name, new_name)

        # Add missing columns
        for col_name in out_schema:
            if col_name not in df.columns:
                df = df.withColumn(col_name, F.lit("None"))
        df = df.select(out_schema)

        # Union
        final_df = final_df.unionByName(df) if final_df else df

    data_partner_ids = data_partner_ids.dataframe()
    data_partner_ids = data_partner_ids.select("data_partner_id", "source_cdm")
    final_df = final_df.join(data_partner_ids, "data_partner_id", "left")
    out.write_dataframe(final_df)
