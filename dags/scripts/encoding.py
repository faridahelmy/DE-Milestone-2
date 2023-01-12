import pandas as pd
def encode(df):
    #the idea behind encoding numerical values is: all non numerical  will be encoded numerically (label encoding)
    encoding_df_accident = df.select_dtypes(include=['object']).copy()
    #loop to get columns that should be encoded
    to_be_encoded=[]
    for columnName, columnValue in encoding_df_accident.items():
        count = encoding_df_accident[columnName].unique().size
        to_be_encoded.append(columnName)
    #loop on columns that shoud be encoded, make their type as category, then transform this category to numerical value
    before_after_encoding=encoding_df_accident.copy()
    for item in to_be_encoded:
        encoding_df_accident[item] = encoding_df_accident[item].astype('category')
        encoding_df_accident[item] = encoding_df_accident[item].cat.codes

    #create dictionary to map values of columns before and after encoding
    encoding_dict=[]
    for column in encoding_df_accident:
        encoding_dict.append(dict(zip(encoding_df_accident[column],before_after_encoding[column])))

    df = df.select_dtypes(exclude=['object'])
    df_copy = df.copy()
    frames = [df_copy, encoding_df_accident]
    df_copy = pd.concat(frames, axis=1)
    df = df_copy
    


        
        