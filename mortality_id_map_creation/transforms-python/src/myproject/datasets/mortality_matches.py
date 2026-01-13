from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("ri.foundry.main.dataset.0c711c87-bc0a-4d53-ace7-d8f429e70dab"),
    mortality_matches=Input("ri.foundry.main.dataset.4566116d-8cd1-4740-93e3-77b7dc8e19c7"),
    data_partner_id_map=Input("ri.foundry.main.dataset.4d4cf17b-9dfb-48e8-bb19-4f62960b75ec"),
    person_id_map=Input("ri.foundry.main.dataset.bfe18a80-5940-467f-85ad-9d9c5a95f5e2"),
    LHB_to_SFTP=Input("ri.foundry.main.dataset.eb971c9b-72fc-4bb5-8fa3-bdab942aced3")
)
def compute(mortality_matches, data_partner_id_map, person_id_map, LHB_to_SFTP):

    df = mortality_matches \
        .withColumn('datasource_type', F.upper(F.split(F.col('pseudo_id'), ':').getItem(0))) \
        .withColumn('record_id', F.split(F.col('pseudo_id'), ':').getItem(1))

    # Split mortality_matches DF into (1) mortality data sources and (2) contributing sites
    mortality_sources_only = df.filter((F.col('datasource_type') == 'OBITDOTCOM') |
                                       (F.col('datasource_type') == 'PRIVATEOBIT') |
                                       (F.col('datasource_type') == 'SSA'))
    sites_only = df \
        .filter((F.col('datasource_type') != 'OBITDOTCOM') &
                (F.col('datasource_type') != 'PRIVATEOBIT') &
                (F.col('datasource_type') != 'SSA')) \
        .withColumnRenamed("record_id", "site_person_id")\
        .withColumnRenamed("datasource_type", "LHB_abbreviation")

    # Get data_partner_id column
    # TODO: This logic is fragile, given that the 'sftp_folder_name' column does not contain unique values
    # Untested use case of F.coalesce from @eniehaus
    # sites_only = sites_only.withColumn("true_match" , F.coalesce(F.LHB_abbreviation, F.SFTP_folder))

    data_partner_id_map = data_partner_id_map.join(LHB_to_SFTP, data_partner_id_map.sftp_folder_name == LHB_to_SFTP.SFTP_folder)\
    .select('data_partner_id', 'LHB_abbreviation').withColumn('LHB_abbreviation', F.upper('LHB_abbreviation'))  # noqa
    sites_only = sites_only.join(data_partner_id_map, on='LHB_abbreviation', how='inner')

    # Get n3c_person_id column
    sites_only = sites_only.join(person_id_map, on=['data_partner_id', 'site_person_id'], how="inner")

    # Reduce to necessary columns only
    sites_only = sites_only.select('data_partner_id', "n3c_person_id", "group_id")

    mortality_person_id_map = mortality_sources_only \
        .join(sites_only,
              on="group_id",
              how="inner") \
        .select('datasource_type',
                'record_id',
                'n3c_person_id',
                'data_partner_id',
                'gender',
                'probability')

    return mortality_person_id_map.filter(mortality_person_id_map.data_partner_id != 207)
