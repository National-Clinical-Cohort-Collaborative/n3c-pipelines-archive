from transforms.api import configure, transform_df, Input, Output, Check
import myproject.datasets.input_sites as input_info
from pyspark.sql import functions as F
from transforms.verbs import dataframes as D
import os.path
from transforms import expectations


sites_and_folder_prefix_pairs = [
    #(input_info.trinetx_sites_with_status, input_info.trinetx_folder_prefix),
    (input_info.omop_sites_with_status, input_info.omop_folder_prefix),
    #(input_info.act_sites_with_status, input_info.act_folder_prefix),
    (input_info.pcornet_sites_with_status, input_info.pcornet_folder_prefix),
    #(input_info.pedsnet_sites_with_status, input_info.pedsnet_folder_prefix)
]
# Store PEDSnet site numbers for replacing CDM name ("OMOP" ==> "PEDSnet")
#pedsnet_sites = [str(site_num) for site_num in input_info.pedsnet_sites_with_status.keys()]

manifests = {}          # Store manifest for each site
payload_inputs = {}     # Store payload info for each site (could be payload_status dataset or payload_filename depending on site's stage in pipeline)
for site_status_dict, folder_prefix in sites_and_folder_prefix_pairs:
    # Iterate over each CDM
    for site_num, has_passed in site_status_dict.items():
        # Iterate over each site and grab manifest
        site_subfolder = "Site " + str(site_num)
        manifests[str(site_num)] = Input(os.path.join(folder_prefix, site_subfolder, "metadata/manifest"))
        # Grab payload dataset with "union_staging" info only if site has passed stage 1 of the pipeline
        # Otherwise, use dataset from unzipping step with only one date column
        if has_passed:
            payload_inputs[str(site_num)+"_payload"] = Input(os.path.join(folder_prefix, site_subfolder, "metadata/payload_status"))
        else:
            payload_inputs[str(site_num)+"_payload"] = Input(os.path.join(folder_prefix, site_subfolder, "transform/00 - unzipped/payload_filename"))

inputs = {**manifests, **payload_inputs}

manifest_schema = {
    "SITE_ABBREV": "string",
    "SITE_NAME": "string",
    "CONTACT_NAME": "string",
    "CONTACT_EMAIL": "string",
    "CDM_NAME": "string",
    "CDM_VERSION": "string",
    "VOCABULARY_VERSION": "string",
    "N3C_PHENOTYPE_YN": "string",
    "N3C_PHENOTYPE_VERSION": "string",
    "RUN_DATE": "date",
    "UPDATE_DATE": "date",
    "NEXT_SUBMISSION_DATE": "date",
    "CONTRIBUTION_DATE": "date",
    "SHIFT_DATE_YN": "string",
    "MAX_NUM_SHIFT_DAYS": "int",
    "N3C_VOCAB_VERSION": "string",
    "APPROX_EXPECTED_PERSON_COUNT": "int"
    }


@configure(profile=["KUBERNETES_NO_EXECUTORS"])
@transform_df(
    Output("/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/LDS Union/unioned_manifest", checks=[
            Check(expectations.col('DTA_ACTIVE').equals(True), 'All site DTAs are not expired', on_error='FAIL')
        ]),
    data_partner_ids=Input("ri.foundry.main.dataset.3332ae36-a617-4ec4-bfde-b4d5a3cc6fbd"),
    monocle_links=Input("ri.foundry.main.dataset.44c42584-b89c-4635-8d41-965a51cb8ccc"),
    payload_tracker=Input("/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/LDS Union/site_payload_tracker"),
    dtas=Input("ri.foundry.main.dataset.b3bcb83f-73b1-47fa-89f7-8149c979b123"),
    dta_grp_name_df=Input("ri.foundry.main.dataset.9a1b700e-23fb-45b9-a4e2-f761cd2fa407"),
    **inputs
)
def my_compute_function(data_partner_ids, monocle_links, payload_tracker, dtas, dta_grp_name_df, **inputs):

    dta_grp_name = dta_grp_name_df.select("group_name").first()["group_name"]

    # Filter and transform the dtas DataFrame
    active_dtas = dtas.filter(dtas["group_name"] == dta_grp_name) \
                      .select(["ror_id", "terminated"]) \
                      .withColumnRenamed("terminated", "dta_expiration") \
                      .withColumnRenamed("ror_id", "institutionid")

    # Add the DTA_ACTIVE column
    active_dtas = active_dtas.withColumn(
        "DTA_ACTIVE",
        F.when(F.col('dta_expiration') > F.current_date(), True).otherwise(False)
    )

    # Aggregate to check if both True and False exist for each institutionid
    aggregated_dtas = active_dtas.groupBy("institutionid").agg(
        F.max("DTA_ACTIVE").alias("max_active"),
        F.min("DTA_ACTIVE").alias("min_active")
    )

    # Determine final DTA_ACTIVE value based on the presence of both True and False
    aggregated_dtas = aggregated_dtas.withColumn(
        "DTA_ACTIVE",
        F.when((F.col("max_active") == True) & (F.col("min_active") == False), True)
        .otherwise(F.col("max_active"))
    ).select("institutionid", "DTA_ACTIVE")

    df_out_list = []
    for site_num in manifests:
        # Fetch manifest for site
        df_site = inputs[site_num]

        # Ensure manifest column names are all uppercase
        for col_name in df_site.columns:
            if not col_name.isupper():
                df_site = df_site.withColumnRenamed(col_name, col_name.upper())

        df_site = df_site.withColumn("data_partner_id", F.lit(site_num))

        payload_df = inputs[site_num + "_payload"]
        if "unreleased_payload" in payload_df.columns:
            # Site has made it to union_staging step of pipeline
            try:
                parsed_payload = payload_df.head().parsed_payload
                unreleased_payload = payload_df.head().unreleased_payload
            except IndexError:  # Handle empty person table in union_staging step
                parsed_payload = "[Processing]"
                unreleased_payload = "[Processing]"

            df_site = df_site.withColumn("PARSED_PAYLOAD", F.lit(parsed_payload))
            df_site = df_site.withColumn("UNRELEASED_PAYLOAD", F.lit(unreleased_payload))
        else:
            # Site has not yet made it to union_staging step of pipeline -- there is no payload ready to be in LDS unreleased
            if "newest_payload" in payload_df.columns:
                df_site = df_site.withColumn("PARSED_PAYLOAD", F.lit(payload_df.where(F.col("newest_payload") == True).head().payload))
            else:   # Handle old version of payload_filenames dataset
                df_site = df_site.withColumn("PARSED_PAYLOAD", F.lit(payload_df.head().payload))
            df_site = df_site.withColumn("UNRELEASED_PAYLOAD", F.lit("No processed payload"))

        # If site manifest doesn't have an expected column, fill with null
        for col in manifest_schema:
            if col not in df_site.columns:
                df_site = df_site.withColumn(col, F.lit(None).cast(manifest_schema[col]))

        df_out = df_site.select([*manifest_schema, "data_partner_id", "PARSED_PAYLOAD", "UNRELEASED_PAYLOAD"])
        df_out_list.append(df_out)

    df_out = D.union_many(*df_out_list)

    # Bring in old site ids
    df_out = df_out.join(
        data_partner_ids.select("data_partner_id", "original_data_partner_id", "institutionid"),
        "data_partner_id",
        "left"
    )

    df_out = df_out.join(aggregated_dtas, "institutionid", how="left")

    # Bring in links to pipelines' Monocle graphs
    df_out = df_out.join(
        monocle_links,
        df_out.data_partner_id == monocle_links.data_partner_id,
        "left"
    ).drop(monocle_links.data_partner_id)

    # Create column to track total payload count for each site
    # Include payloads processed in Adeptia
    payload_tracker = payload_tracker.groupBy("data_partner_id").count()
    payload_tracker = payload_tracker.selectExpr("data_partner_id", "count as PAYLOAD_COUNT_ESTIMATE")
    df_out = df_out.join(
        payload_tracker,
        df_out.data_partner_id == payload_tracker.data_partner_id,
        "left"
    ).drop(payload_tracker.data_partner_id)

    # # Replace "OMOP" CDM_NAME for PEDSnet sites
    # df_out = df_out.withColumn(
    #     "CDM_NAME",
    #     F.when(F.col("data_partner_id").isin(pedsnet_sites), F.lit("OMOP (PEDSNET)")).otherwise(F.col("CDM_NAME"))
    # )

    df_out = df_out.selectExpr(
        "CAST(data_partner_id as string) as DATA_PARTNER_ID",
        "CDM_NAME",
        "CDM_VERSION",
        "RUN_DATE",
        "CONTRIBUTION_DATE",
        "N3C_PHENOTYPE_YN",
        "N3C_PHENOTYPE_VERSION",
        "VOCABULARY_VERSION",
        "CAST(null as string) as MAPPED_VERSION",
        "CAST(null as string) as DATASETBUILDVERSION",
        "CAST(null as string) as RELEASENOTE",
        "data_partner_id as N3C_SITE_ID",
        "original_data_partner_id as OLD_SITE_ID",
        "PARSED_PAYLOAD",
        "UNRELEASED_PAYLOAD",
        "SHIFT_DATE_YN",
        "MAX_NUM_SHIFT_DAYS",
        "N3C_VOCAB_VERSION",
        "APPROX_EXPECTED_PERSON_COUNT",
        "monocle_link as MONOCLE_LINK",
        "PAYLOAD_COUNT_ESTIMATE",
        "CONTACT_EMAIL",
        "DTA_ACTIVE"
    )

    return df_out.distinct()
