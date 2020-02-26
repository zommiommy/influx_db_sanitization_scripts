from pandas.api.types import is_string_dtype

def get_filtered_labels(df, field):
    labels = [
        label
        for label in df.columns
        if label not in ["time", "pd_time", field]
            and 
            is_string_dtype(df.dtypes[label])
    ]
    return labels
