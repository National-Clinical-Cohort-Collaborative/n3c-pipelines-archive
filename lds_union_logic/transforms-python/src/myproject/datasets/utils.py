from transforms.api import Input
from transforms.verbs import dataframes as D
#from myproject.datasets.removed_input_sites import removed_trinetx_filepaths, removed_omop_filepaths, removed_act_filepaths, removed_pcornet_filepaths, removed_pedsnet_filepaths
#from myproject.datasets.removed_input_sites import removed_omop_filepaths, removed_act_filepaths
#from myproject.datasets.input_sites import trinetx_filepaths, omop_filepaths, act_filepaths, pcornet_filepaths, pedsnet_filepaths
from myproject.datasets.input_sites import pcornet_filepaths, omop_filepaths
from myproject.datasets.schemas import schema_dict
import os


# Create dictionary of input datasets for the given domain
#def get_inputs(domain, include_omop=True, include_pcornet=True, include_trinetx=True, include_act=True, include_pedsnet=True):
def get_inputs(domain, include_omop=True, include_act=False, include_pcornet=True):
    all_input_site_paths = []
    if include_omop:
        all_input_site_paths += omop_filepaths
    if include_pcornet:
        all_input_site_paths += pcornet_filepaths
    # if include_trinetx:
    #     all_input_site_paths += trinetx_filepaths
    # if include_act:
    #     all_input_site_paths += act_filepaths
    # if include_pedsnet:
    #     all_input_site_paths += pedsnet_filepaths

    inputs = {}
    for i, site_path in enumerate(all_input_site_paths):
        inputs["{idx}".format(idx=i)] = Input(os.path.join(site_path, "union_staging", "staged", domain))

    return inputs


# Create dictionary of person_id_map input datasets given the path for a particular cdm
def get_person_id_map_inputs(path, cdm):
    all_input_site_paths = []
    # if cdm == "TRINETX":
    #     all_input_site_paths += trinetx_filepaths
    if cdm == "OMOP":
        all_input_site_paths += omop_filepaths
    # if cdm == "PEDSNET":
    #     all_input_site_paths += pedsnet_filepaths
    # if cdm == "ACT":
    #     all_input_site_paths += act_filepaths
    if cdm == "PCORNET":
        all_input_site_paths += pcornet_filepaths

    inputs = {}
    for i, site_path in enumerate(all_input_site_paths):
        inputs["{cdmx}_{idx}".format(idx=i, cdmx=cdm)] = Input(os.path.join(site_path, path))

    return inputs


# Union all input datasets for the given domain
def perform_union(ctx, domain, **inputs):
    # Get schemas
    schema = schema_dict[domain]

    # Enforce schema for each input dataframe
    dfs_to_union = [df_site.selectExpr(*schema) for df_site in inputs.values()]

    # Union
    unioned_df = D.union_many(*dfs_to_union)

    # The union alphabetizes the column ordering - undo this
    unioned_df = unioned_df.selectExpr(*schema)

    return unioned_df


#def get_removed_inputs(domain, include_omop=True, include_pcornet=True, include_trinetx=True, include_act=True, include_pedsnet=True):
def get_removed_inputs(domain, include_omop=True, include_pcornet=True, include_act=False):
    inputs = {}
#     i = 0
#     if include_omop:
#         for site_path in removed_omop_filepaths:
#             inputs["{idx}".format(idx=i)] = Input(os.path.join(site_path, "transform/06 - pre clean/removed", domain))
#             i += 1
#     # if include_pcornet:
#     #     for site_path in removed_pcornet_filepaths:
#     #         inputs["{idx}".format(idx=i)] = Input(os.path.join(site_path, "transform/07 - pre clean/removed", domain))
#     #         i += 1
#     # if include_trinetx:
#     #     for site_path in removed_trinetx_filepaths:
#     #         inputs["{idx}".format(idx=i)] = Input(os.path.join(site_path, "transform/07 - pre clean/removed", domain))
#     #         i += 1
#     if include_act:
#         for site_path in removed_act_filepaths:
#             inputs["{idx}".format(idx=i)] = Input(os.path.join(site_path, "transform/07 - pre clean/removed", domain))
#             i += 1
#     # if include_pedsnet:
#     #     for site_path in removed_pedsnet_filepaths:
#     #         inputs["{idx}".format(idx=i)] = Input(os.path.join(site_path, "transform/07 - pre clean/removed", domain))
#     #         i += 1
    return inputs


# Union all input datasets for the given domain
def perform_removed_union(ctx, domain, **inputs):
    # Get schemas
    schema = schema_dict[domain]
    schema = [col for col in schema if col[-12:] != "concept_name"]
    schema.append("removal_reason")

    # Enforce schema for each input dataframe
    if domain == "observation" or domain == 'measurement':
        dfs_to_union = [df_site.selectExpr(*schema).withColumn("value_as_number", df_site.value_as_number.cast("double")) for df_site in inputs.values()]
    else:
        dfs_to_union = [df_site.selectExpr(*schema) for df_site in inputs.values()]

    # Union
    unioned_df = D.union_many(*dfs_to_union)

    # The union alphabetizes the column ordering - undo this
    unioned_df = unioned_df.selectExpr(*schema)

    return unioned_df
