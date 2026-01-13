from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("ri.foundry.main.dataset.1606d82c-6d74-4d71-80ef-64532a5b453d"),
    pprl=Input("ri.foundry.main.dataset.ffb10942-92ed-48c1-a01c-fef1b4ee5552"),
    cms_ids=Input("ri.foundry.main.dataset.4175ec75-aa1e-43cf-adea-2e05c888b604")
)
def compute(pprl, cms_ids):
    cms_pprl_ids = pprl.filter((F.col('institution') == 'CMS') |
                               (F.col('institution') == 'CMS-MEDICAID'))

    cms_pprl_ids = cms_pprl_ids.join(cms_ids, on=cms_pprl_ids.site_person_id == cms_ids.original_cms_person_id)

    return cms_pprl_ids
