# from pyspark.sql import functions as F
from transforms.api import configure, transform_df, Input, Output, ComputeBackend


@configure(
    profile=['EXECUTOR_MEMORY_LARGE', 'NUM_EXECUTORS_64', 'EXECUTOR_MEMORY_OFFHEAP_FRACTION_MINIMUM', 'EXECUTOR_CORES_MEDIUM'],
    backend=ComputeBackend.VELOX
)
@transform_df(
    Output("ri.foundry.main.dataset.0c3cab22-805d-4d84-98d6-4960651710bd"),
    measurements=Input("ri.foundry.main.dataset.0f30f7f8-dc17-4f19-b9a7-1297708050ff"),
    visits=Input("ri.foundry.main.dataset.de27cd23-ea8f-4d9f-825e-40b61cd2c1e2")
)
def compute(measurements, visits):
    measurements = measurements.select([
        'measurement_id', 'person_id', 'data_partner_id', 'measurement_date', 'visit_occurrence_id'])

    visits = visits.select(['macrovisit_id', 'macrovisit_start_date', 'macrovisit_end_date', 'visit_occurrence_id'])

    return measurements.join(visits, 'visit_occurrence_id', 'inner')
