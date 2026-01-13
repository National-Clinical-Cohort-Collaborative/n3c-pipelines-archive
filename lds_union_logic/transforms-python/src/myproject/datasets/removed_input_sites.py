import os

#trinetx_folder_prefix = "/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/Source Data Model: TriNetX/raw_trinetx"
omop_folder_prefix = "/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/Source Data Model: OMOP"
#act_folder_prefix = "/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/Source Data Model: ACT"
pcornet_folder_prefix = "/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/Source Data Model: PCORnet"
#pedsnet_folder_prefix = "/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/Source Data Model: PEDSnet"

# "True" flag indicates that the site has passed through the first stage of the pipeline
# and its "union_staging" datasets have been built.
# Sites have a "False" flag until their "union_staging" datasets have been created.
# trinetx_sites_with_status = {
#     77:  True,
#     353: True,
#     264: True,
#     808: True,
#     157: True,
#     134: True,
#     787: True,
#     376: True,
#     578: True,
#     113: True,
#     294: True,
#     901: True,
#     200: True,
#     908: True,
#     75:  True,
#     325: True,
#     806: True,
#     102: True,
# }

omop_sites_with_status = {
    # 25:  True,
    # 850: True,
    # # 325: True, # This site's CDM changed from OMOP to TriNetX
    # 181: True,
    # 698: True,
    # 569: True,
    # 923: True,
    # 170: True,
    # 217: True,
    # 406: True,
    # 888: True,
    # 664: True,
    # 829: True,
    # 939: True,
    # 213: True,
    # 117: True,
    # 266: True,
    # 565: True,
    # 655: False,  # unrelease as their irb expired, requested to removed from the release list
    # 38: True,
    # 135: True,
    # 124: True,
    # 285: True,
    # # 84: True,   # This site should NEVER get released -- "hide" from DQP
    # 23: False,   # Site 84 subsite
    # 524: False,  # Site 84 subsite
    # 183: False,  # Site 84 subsite
    # 966: False,  # Site 84 subsite
    # 770: False,  # Site 84 subsite
    # 224: True,
    # 666: True,  # temp test site for JHU OMOP CDM
    # 861: True,
    # 209: True,
}

act_sites_with_status = {
    # 411: True,
    # 336: True,
    # 688: True,
    # 1003: True,
    # 967: True,
    # 41: True,
    # 789: True,
    # 106: True
}

# pcornet_sites_with_status = {

# }

# pedsnet_sites_with_status = {
#     605: True,
#     1015: True,
# }


# Filepaths to each site's folder in the CDM. Only sites flagged as "True" are included -- we don't union data
# from "False" sites that haven't passed the first stage of the pipeline.
#removed_trinetx_filepaths = [os.path.join(trinetx_folder_prefix, "Site "+str(site_num)) for site_num in trinetx_sites_with_status if trinetx_sites_with_status[site_num]]
#removed_omop_filepaths = [os.path.join(omop_folder_prefix, "Site "+str(site_num)) for site_num in omop_sites_with_status if omop_sites_with_status[site_num]]
#removed_act_filepaths = [os.path.join(act_folder_prefix, "Site "+str(site_num)) for site_num in act_sites_with_status if act_sites_with_status[site_num]]
#removed_pcornet_filepaths = [os.path.join(pcornet_folder_prefix, "Site "+str(site_num)) for site_num in pcornet_sites_with_status if pcornet_sites_with_status[site_num]]
#removed_pedsnet_filepaths = [os.path.join(pedsnet_folder_prefix, "Site "+str(site_num)) for site_num in pedsnet_sites_with_status if pedsnet_sites_with_status[site_num]]
