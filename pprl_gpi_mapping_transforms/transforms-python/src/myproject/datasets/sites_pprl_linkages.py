from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("ri.foundry.main.dataset.de74563a-6fa9-4174-888c-a0ccb50fddf0"),
    parsed_global_pprl=Input("ri.foundry.main.dataset.ffb10942-92ed-48c1-a01c-fef1b4ee5552"),
    data_partner_id_map=Input("ri.foundry.main.dataset.3332ae36-a617-4ec4-bfde-b4d5a3cc6fbd"),
    person_id_map=Input("ri.foundry.main.dataset.2af29c81-5e57-4404-80df-34aaab7ccff0"),
    LHB_to_SFTP=Input("ri.foundry.main.dataset.bd6b007e-2e88-4c1b-b599-d9d301477dee")
)
def compute(parsed_global_pprl, data_partner_id_map, person_id_map, LHB_to_SFTP):

    site_sources = parsed_global_pprl.filter((F.col('institution') != 'CMS') |
                                             (F.col('institution') != 'CMS-MEDICAID') |
                                             (F.col('institution') != 'ALLOFUS'))

    data_partner_id_map = data_partner_id_map.join(LHB_to_SFTP, data_partner_id_map.sftp_folder_name == LHB_to_SFTP.SFTP_folder)\
    .select('data_partner_id', 'LHB_abbreviation').withColumn('LHB_abbreviation', F.upper('LHB_abbreviation')).distinct()  # noqa

    site_sources = site_sources.join(data_partner_id_map, on=data_partner_id_map.LHB_abbreviation == parsed_global_pprl.institution)
    site_sources = site_sources.join(person_id_map, on=['data_partner_id', 'site_person_id'], how="inner")

    return site_sources
