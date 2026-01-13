from transforms.api import transform, Input, Output


datasets = {
    'hh': {
        'input': Input("/UNITE/[PPRL] CMS Data & Repository/raw/pprl/hh_pprl"),
        'output': Output("/UNITE/[PPRL] CMS Data & Repository/raw/staging/home_health")
    },
    'dm': {
        'input': Input("/UNITE/[PPRL] CMS Data & Repository/raw/pprl/dm_pprl"),
        'output': Output("/UNITE/[PPRL] CMS Data & Repository/raw/staging/durable_medical_equipment")
    },
    'hs': {
        'input': Input("/UNITE/[PPRL] CMS Data & Repository/raw/pprl/hs_pprl"),
        'output': Output("/UNITE/[PPRL] CMS Data & Repository/raw/staging/hospice")
    },
    'ip': {
        'input': Input("/UNITE/[PPRL] CMS Data & Repository/raw/pprl/ip_pprl"),
        'output': Output("/UNITE/[PPRL] CMS Data & Repository/raw/staging/inpatient")
    },
    'mbsf': {
        'input': Input("/UNITE/[PPRL] CMS Data & Repository/raw/pprl/mbsf_pprl"),
        'output': Output("/UNITE/[PPRL] CMS Data & Repository/raw/staging/member_benefit")
    },
    'opl': {
        'input': Input("/UNITE/[PPRL] CMS Data & Repository/raw/pprl/opl_pprl"),
        'output': Output("/UNITE/[PPRL] CMS Data & Repository/raw/staging/outpatient_facility")
    },
    'pb': {
        'input': Input("/UNITE/[PPRL] CMS Data & Repository/raw/pprl/pb_pprl"),
        'output': Output("/UNITE/[PPRL] CMS Data & Repository/raw/staging/professional_billed")
    },
    'pde': {
        'input': Input("/UNITE/[PPRL] CMS Data & Repository/raw/pprl/pde_pprl"),
        'output': Output("/UNITE/[PPRL] CMS Data & Repository/raw/staging/part_d")
    },
    'sn': {
        'input': Input("/UNITE/[PPRL] CMS Data & Repository/raw/pprl/sn_pprl"),
        'output': Output("/UNITE/[PPRL] CMS Data & Repository/raw/staging/skilled_nursing")
    },
}


input_kwargs = {f"{key}_input": value['input'] for key, value in datasets.items()}
output_kwargs = {f"{key}_output": value['output'] for key, value in datasets.items()}


@transform(**input_kwargs, **output_kwargs)
def compute(**all_dfs):
    for key in datasets.keys():
        input_key = f"{key}_input"
        output_key = f"{key}_output"

        if input_key in all_dfs and output_key in all_dfs:
            input_df = all_dfs[input_key].dataframe()
            all_dfs[output_key].write_dataframe(input_df)
