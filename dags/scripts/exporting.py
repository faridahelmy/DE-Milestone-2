
def write_df_to_csv_file(df, file_name):
    file_name = file_name + ".csv"
    df.to_csv(file_name, index = False)