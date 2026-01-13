# Final mortality schema is only permitted to have ONE person identifier column
FINAL_MORTALITY_COLS = [
    'data_partner_id',
    'person_id',
    'date_of_birth',
    'date_of_death',
    'death_verification',
    'gender_probability_score_m',
    'gender_probability_score_f',
    'state',
    'datasource_type'
]