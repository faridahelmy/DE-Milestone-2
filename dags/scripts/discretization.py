import pandas as pd
def discretize(df):
    date_col = df.date

    #Now lets convert it to a datetime object
    date_col = pd.to_datetime(date_col)

    #Step 1
    week = []
    for date in date_col:    
        week.append(date.isocalendar()[1])
    
    #Step 2
    df["week_number"] = week
