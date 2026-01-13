from transforms.api import transform, Input, Output
from pcornet.utils import add_site_id_col, add_mapped_vocab_code_col, create_datetime_col
from pcornet.site_specific_utils import apply_site_parsing_logic
from pcornet.anchor import path


def make_transform(domain, source_col, mapped_col, datetimes):
    @transform(
        my_input=Input(path.transform + '02 - clean/' + domain),
        site_id_df=Input(path.site_id),
        mapping_table=Input(path.mapping),
        processed=Output(path.transform + '03 - prepared/' + domain),
    )
    def compute_function(my_input, site_id_df, mapping_table, processed):
        mapping_table = mapping_table.dataframe()
        processed_df = my_input.dataframe()

        # Add a "data_partner_id" column with the site's id to enable downstream primary key generation
        processed_df = add_site_id_col(processed_df, site_id_df)

        # Apply site-specific parsing logic (if applicable)
        processed_df = apply_site_parsing_logic(processed_df, site_id_df)

        for date, time, upper in datetimes:
            processed_df = create_datetime_col(processed_df, date, time, upper)

        # Map the vocab code in source_vocab_col to a vocabulary_id that appears in the concept table
        # using the mapping spreadsheet
        if source_col and mapped_col:
            processed_df = add_mapped_vocab_code_col(
                processed_df,
                mapping_table,
                domain.upper(),
                source_col,
                mapped_col
            )

        processed.write_dataframe(processed_df)

    return compute_function


domains = [
    # domain, source_col, mapped_col, list of datetime cols
    ("condition", "condition_type", "mapped_condition_type", []),
    ("control_map", None, None, []),
    ("death", None, None, []),
    ("death_cause", "death_cause_code", "mapped_death_cause_code", []),
    ("demographic", None, None, [("birth_date", "birth_time", "BIRTH_DATETIME")]),
    ("diagnosis", "dx_type", "mapped_dx_type", []),
    ("dispensing", None, None, []),
    ("encounter", None, None, [("admit_date", "admit_time", "ADMIT_DATETIME"), ("discharge_date", "discharge_time", "DISCHARGE_DATETIME")]),
    ("immunization", "vx_code_type", "mapped_vx_code_type", []),
    # Lab result cm
    # ("lds_address_history", None, None, []),
    ("med_admin", "medadmin_type", "mapped_medadmin_type", [("medadmin_start_date", "medadmin_start_time", "MEDADMIN_START_DATETIME"), ("medadmin_stop_date", "medadmin_stop_time", "MEDADMIN_STOP_DATETIME")]),
    ("note", None, None, []),
    ("note_nlp", None, None, []),
    # Obs clin
    # Obs gen
    ("prescribing", None, None, [("rx_order_date", "rx_order_time", "RX_ORDER_DATETIME")]),
    # Pro cm
    ("procedures", "px_type", "mapped_px_type", []),
    ("provider", None, None, []),
    # ("vital", None, None, [("measure_date", "measure_time", "MEASURE_DATETIME")])
]

transforms = (make_transform(*domain) for domain in domains)
