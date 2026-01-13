# from pyspark.sql import functions as F
from transforms.api import transform, Input, Output
from pcornet.utils import add_site_id_col, create_datetime_col
from pcornet.site_specific_utils import apply_site_parsing_logic
from pyspark.sql import functions as F
from pcornet.anchor import path


@transform(
    processed=Output(path.transform + '03 - prepared/vital'),
    my_input=Input(path.transform + '02 - clean/vital'),
    mapping_table=Input(path.mapping),
    site_id_df=Input(path.site_id),
)
def compute_function(my_input, site_id_df, mapping_table, processed):
    mapping_table = mapping_table.dataframe()
    processed_df = my_input.dataframe()

    # Add a "data_partner_id" column with the site's id to enable downstream primary key generation
    processed_df = add_site_id_col(processed_df, site_id_df)

    # Apply site-specific parsing logic (if applicable)
    processed_df = apply_site_parsing_logic(processed_df, site_id_df)

    # --add the datetime column 
    #for date, time, upper in datetimes:
    #        processed_df = create_datetime_col(processed_df, date, time, upper)
    # --add datetime column - "measure_date", "measure_time", "MEASURE_DATETIME"
    processed_df = create_datetime_col(processed_df, "measure_date", "measure_time", "MEASURE_DATETIME")

    # vital - no source vocab code to map to vocab in the concept table- bp, bmi bp_position smoking and tobacco and tobacco_type are all PCORNet native. 
    # Map the vocab code in source_vocab_col to a vocabulary_id that appears in the concept table
    # using the mapping spreadsheet
    #if source_col and mapped_col:
    #        processed_df = add_mapped_vocab_code_col(
    #            processed_df,
    #            mapping_table,
    #            domain.upper(),
    #            source_col,
    #            mapped_col
    #        )        

    # sometime the sites are submitting null for bp_position along with valid numeric bp data, use NI instead of null
    processed_df = processed_df.withColumn("corrected_bp_position", F.coalesce(processed_df["bp_position"], F.lit('NI')))

    processed.write_dataframe(processed_df)