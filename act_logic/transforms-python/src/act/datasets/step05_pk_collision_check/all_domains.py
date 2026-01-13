from transforms.api import transform, Input, Output, incremental, Check
from transforms import expectations as E
from act.pkey_utils import new_duplicate_rows_with_collision_bits
from act.anchor import path

input_checks = [Check(E.primary_key('hashed_id'), 'hashed_id must be unique', on_error='FAIL')]
output_checks = [Check(E.col('collision_bits').lt(4), 'Each 51-bit id must have fewer than 4 collisions', on_error='FAIL')]


def make_transform(domain, pkey):
    @incremental(snapshot_inputs=['omop_domain'])
    @transform(
        omop_domain=Input(path.transform + "04 - domain mapping/" + domain, checks=input_checks),
        lookup_df=Output(path.transform + "05 - pkey collision lookup tables/" + domain, checks=output_checks)
    )
    def compute_function(ctx, omop_domain, lookup_df):
        new_rows = new_duplicate_rows_with_collision_bits(omop_domain, lookup_df, ctx, pkey, "hashed_id")
        lookup_df.write_dataframe(new_rows)

    return compute_function


domains = [
    ("condition_era", "condition_era_id_51_bit"),
    ("condition_occurrence", "condition_occurrence_id_51_bit"),
    ("device_exposure", "device_exposure_id_51_bit"),
    ("drug_era", "drug_era_id_51_bit"),
    ("drug_exposure", "drug_exposure_id_51_bit"),
    ("location", "location_id_51_bit"),
    ("measurement", "measurement_id_51_bit"),
    ("note", "note_id_51_bit"),
    ("note_nlp", "note_nlp_id_51_bit"),
    ("observation", "observation_id_51_bit"),
    ("observation_period", "observation_period_id_51_bit"),
    ("person", "person_id_51_bit"),
    ("procedure_occurrence", "procedure_occurrence_id_51_bit"),
    ("visit_occurrence", "visit_occurrence_id_51_bit"),
    ("visit_detail", "visit_detail_id_51_bit"), 
    ("control_map", "control_map_id_51_bit")
]

transforms = (make_transform(*domain) for domain in domains)
