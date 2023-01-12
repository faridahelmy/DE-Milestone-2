from datetime import datetime
import pandas as pd



def add_weekend_column(df):
    #First lets get a copy of the dataset and get the column alone

    date_col = df.date

    #Now lets convert it to a datetime object

    date_col = pd.to_datetime(date_col)

    #Weekend (Sat/Sun) or not (1 for true 0 for false)

    weekend = []
    for date in date_col:    
        if (date.isocalendar()[2] == 5 or date.isocalendar()[2] == 6):
            weekend.append(1)
        else:
            weekend.append(0)

    df["weekend"] = weekend


def augment(df):
    add_public_holiday(df)
    add_weekend_column(df)


def add_public_holiday(df):
    #First lets get a copy of the dataset and get the column alone

    date_col = df.date

    #Now lets convert it to a datetime object

    date_col = pd.to_datetime(date_col)
    #Public holiday or not (1 for true 0 for false)
    

    newyearsday = datetime.fromisoformat('2016-01-01').date()
    goodfriday = datetime.fromisoformat('2016-03-25').date()
    eastermonday = datetime.fromisoformat('2016-03-28').date()
    earlymay = datetime.fromisoformat('2016-05-02').date()
    springbank = datetime.fromisoformat('2016-05-30').date()
    summerbank = datetime.fromisoformat('2016-08-29').date()
    boxingday = datetime.fromisoformat('2016-12-26').date()
    christmas = datetime.fromisoformat('2016-12-27').date()

    holiday = []
    for date in date_col:    
        if (date.date()== newyearsday or date.date()== goodfriday or date.date()== eastermonday or date.date()== earlymay or date.date()== springbank or date.date()== summerbank or date.date()== boxingday or date.date()== christmas):
            holiday.append(1)
        else:
            holiday.append(0)


    df["public_holiday"] = holiday