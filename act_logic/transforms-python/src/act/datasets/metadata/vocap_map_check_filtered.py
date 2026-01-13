from transforms.api import transform_df, Input, Output
from act.anchor import path


@transform_df(
    Output(path.metadata + "vocab_map_check_filtered"),
    source_df=Input(path.metadata + "vocab_map_check"),
)
def compute(source_df):
    return source_df.filter(source_df.included_in_vocab_map == "N")
