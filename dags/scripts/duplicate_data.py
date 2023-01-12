def handle_duplicate_data(df):
    bool_series = df.duplicated('accident_reference',keep='first')
    df_accidents_noduplicates = df[~bool_series]
    return df_accidents_noduplicates