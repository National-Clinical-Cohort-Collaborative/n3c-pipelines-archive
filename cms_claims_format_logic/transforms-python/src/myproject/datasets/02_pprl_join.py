from transforms.api import transform, Input, Output


def join_and_write(input_trim, output_pprl, cms_mapping_table):
    input_df = input_trim.dataframe()
    pprl_df = input_df.join(cms_mapping_table, on=input_df.BID == cms_mapping_table.original_cms_person_id, how='inner')
    pprl_df = pprl_df.drop('n3c_person_id', 'original_cms_person_id', 'data_partner_id', 'BID')
    cols = [c for c in pprl_df.columns if c not in ['cms_person_id']]
    pprl_df = pprl_df.select('cms_person_id', *cols).withColumnRenamed('cms_person_id', 'person_id')

    output_pprl.write_dataframe(pprl_df)


datasets = {
    'hh': {
        'trim': Input("/UNITE/CMS Raw Data Parsing - RWD Pipeline - N3C COVID Replica/raw/trimmed/hh_trim"),
        'pprl': Output("/UNITE/[PPRL] CMS Data & Repository/raw/pprl/hh_pprl")
    },
    'dm': {
        'trim': Input("/UNITE/CMS Raw Data Parsing - RWD Pipeline - N3C COVID Replica/raw/trimmed/dm_trim"),
        'pprl': Output("/UNITE/[PPRL] CMS Data & Repository/raw/pprl/dm_pprl")
    },
    'hs': {
        'trim': Input("/UNITE/CMS Raw Data Parsing - RWD Pipeline - N3C COVID Replica/raw/trimmed/hs_trim"),
        'pprl': Output("/UNITE/[PPRL] CMS Data & Repository/raw/pprl/hs_pprl")
    },
    'ip': {
        'trim': Input("/UNITE/CMS Raw Data Parsing - RWD Pipeline - N3C COVID Replica/raw/trimmed/ip_trim"),
        'pprl': Output("/UNITE/[PPRL] CMS Data & Repository/raw/pprl/ip_pprl")
    },
    'mbsf': {
        'trim': Input("/UNITE/CMS Raw Data Parsing - RWD Pipeline - N3C COVID Replica/raw/trimmed/mbsf_trim"),
        'pprl': Output("/UNITE/[PPRL] CMS Data & Repository/raw/pprl/mbsf_pprl")
    },
    'opl': {
        'trim': Input("/UNITE/CMS Raw Data Parsing - RWD Pipeline - N3C COVID Replica/raw/trimmed/opl_trim"),
        'pprl': Output("/UNITE/[PPRL] CMS Data & Repository/raw/pprl/opl_pprl")
    },
    'pb': {
        'trim': Input("/UNITE/CMS Raw Data Parsing - RWD Pipeline - N3C COVID Replica/raw/trimmed/pb_trim"),
        'pprl': Output("/UNITE/[PPRL] CMS Data & Repository/raw/pprl/pb_pprl")
    },
    'pde': {
        'trim': Input("/UNITE/CMS Raw Data Parsing - RWD Pipeline - N3C COVID Replica/raw/trimmed/pde_trim"),
        'pprl': Output("/UNITE/[PPRL] CMS Data & Repository/raw/pprl/pde_pprl")
    },
    'sn': {
        'trim': Input("/UNITE/CMS Raw Data Parsing - RWD Pipeline - N3C COVID Replica/raw/trimmed/sn_trim"),
        'pprl': Output("/UNITE/[PPRL] CMS Data & Repository/raw/pprl/sn_pprl")
    },
}

cms_mapping_table = Input("/UNITE/[PPRL] CMS Data & Repository/pipeline/staging/cms_mapping_table")


@transform(**{key + '_pprl': val['pprl'] for key, val in datasets.items()}, **{key + '_trim': val['trim']
                                         for key, val in datasets.items()}, cms_mapping_table=cms_mapping_table)  # noqa
def compute(cms_mapping_table, **kwargs):
    cms_mapping_table_df = cms_mapping_table.dataframe()

    for key, val in datasets.items():
        join_and_write(kwargs[key + '_trim'], kwargs[key + '_pprl'], cms_mapping_table_df)
