from transforms.api import transform, Input, Output
from pcornet.utils import add_site_id_col
from pcornet.site_specific_utils import apply_site_parsing_logic
from pyspark.sql.functions import lit
from pcornet.anchor import path


@transform(
    processed=Output(path.transform + '03 - prepared/lds_address_history'),
    my_input=Input(path.transform + '02 - clean/lds_address_history'),
    site_id_df=Input(path.site_id),
)
def compute_function(my_input, site_id_df, processed):

    processed_df = my_input.dataframe()

    # Add a "data_partner_id" column with the site's id to enable downstream primary key generation
    processed_df = add_site_id_col(processed_df, site_id_df)

    # Apply site-specific parsing logic (if applicable)
    processed_df = apply_site_parsing_logic(processed_df, site_id_df)

    # Add column address_county if it isn't already present
    if 'address_county' not in processed_df.columns:
        processed_df = processed_df.withColumn("address_county", lit(None).cast('string'))

    processed.write_dataframe(processed_df)
