import os

omop_folder_prefix = "/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/Source Data Model: OMOP"
act_folder_prefix = "/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/Source Data Model: ACT"
#trinetx_folder_prefix = "/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/Source Data Model: TriNetX/raw_trinetx"
pcornet_folder_prefix = "/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/Source Data Model: PCORnet"
#pedsnet_folder_prefix = "/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/Source Data Model: PEDSnet"

# "True" flag indicates that the site has passed through the first stage of the pipeline
# and its "union_staging" datasets have been built.
# Sites have a "False" flag until their "union_staging" datasets have been created.
# trinetx_sites_with_status = {
#     77:  True,
#     353: True,
#     484: True,
# }

omop_sites_with_status = {
    179: True
    # 407: True
   # 675: True
}

act_sites_with_status = {
    # 411: True,
    # 336: True,
    # 106: True
}

pcornet_sites_with_status = {
    126: True,
    220: True,
    349: True,
    428: True
}

# pedsnet_sites_with_status = {
#     605: True,
#     1015: True,
# }


# Filepaths to each site's folder in the CDM. Only sites flagged as "True" are included -- we don't union data
# from "False" sites that haven't passed the first stage of the pipeline.
#trinetx_filepaths = [os.path.join(trinetx_folder_prefix, "Site "+str(site_num)) for site_num in trinetx_sites_with_status if trinetx_sites_with_status[site_num]]
omop_filepaths = [os.path.join(omop_folder_prefix, "Site "+str(site_num)) for site_num in omop_sites_with_status if omop_sites_with_status[site_num]]
#act_filepaths = [os.path.join(act_folder_prefix, "Site "+str(site_num)) for site_num in act_sites_with_status if act_sites_with_status[site_num]]
pcornet_filepaths = [os.path.join(pcornet_folder_prefix, "Site "+str(site_num)) for site_num in pcornet_sites_with_status if pcornet_sites_with_status[site_num]]
#pedsnet_filepaths = [os.path.join(pedsnet_folder_prefix, "Site "+str(site_num)) for site_num in pedsnet_sites_with_status if pedsnet_sites_with_status[site_num]]
