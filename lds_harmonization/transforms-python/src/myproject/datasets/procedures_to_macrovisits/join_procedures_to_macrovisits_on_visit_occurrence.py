# from pyspark.sql import functions as F
from transforms.api import configure, transform_df, Input, Output


@configure(profile=['EXECUTOR_MEMORY_LARGE', 'NUM_EXECUTORS_64'])
@transform_df(
    Output("ri.foundry.main.dataset.2e55bdef-6143-4ea4-bc68-ac989a82f252"),
    procedures=Input("ri.foundry.main.dataset.7394de3a-f3fe-4b73-9e61-12858dcd9868"),
    visits=Input("ri.foundry.main.dataset.de27cd23-ea8f-4d9f-825e-40b61cd2c1e2")
)
def compute(procedures, visits):

    procedures = procedures.select([
        'procedure_occurrence_id', 'person_id', 'data_partner_id', 'procedure_date', 'visit_occurrence_id'])

    visits = visits.select(['macrovisit_id', 'macrovisit_start_date', 'macrovisit_end_date', 'visit_occurrence_id'])

    return procedures.join(visits, 'visit_occurrence_id', 'inner')
