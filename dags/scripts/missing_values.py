import pandas as pd 
import numpy as np
def handle_missing_values(df):
    df = df[df['location_easting_osgr'].notna()]
    df = df[df['speed_limit'].notna()]
    df = df.fillna(value=np.nan)
    #imputation for road type based on the mode
    df['road_type'] = df['road_type'].fillna(df['road_type'].mode()[0])
    missingWeather=df[['road_surface_conditions','weather_conditions']]

    countWeather=missingWeather.groupby(['road_surface_conditions','weather_conditions']).size()
    countWeather=countWeather.to_frame(name='count').reset_index()

    rsc=countWeather['road_surface_conditions'].unique()
    result=pd.DataFrame()
    for farida in rsc:
        alo3=countWeather[countWeather['road_surface_conditions']==farida]
        alo5=alo3[(alo3['count']==(alo3['count'].max()))]
        result=result.append(alo5)
    
    dictionary = {'Data missing or out of range' : "Fine no high winds",'Dry' :
     "Fine no high winds", 'Flood over 3cm. deep' : 'Raining no high winds', 
     'Frost or ice' : 'Fine no high winds','Snow':'Snowing no high winds',
     'Wet or damp':'Fine no high winds',
     'unknown (self reported)':'Fine no high winds'}

    series = pd.Series(dictionary)
    #imputing the weather_conditions with respect to road surface conditions column
    df.loc[df['weather_conditions'].isnull(),'weather_conditions'] = df['road_surface_conditions'].map(series)
    


