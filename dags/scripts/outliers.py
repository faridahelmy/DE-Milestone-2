def handle_outliers(df):
    Q1 = df.quantile(0.25)
    Q3 = df.quantile(0.75)
    IQR = Q3 - Q1

    cut_off = IQR * 1.5
    lower = Q1 - cut_off
    upper =  Q3 + cut_off

    df_acc_removed=[]
    for columns in IQR.index:
        Q1 = df[columns].quantile(0.25)
        Q3 = df[columns].quantile(0.75)
        IQR2 = Q3 - Q1
        cut_off = IQR2 * 1.5
        lower = Q1 - cut_off
        upper =  Q3 + cut_off
        df1 = df[df[columns]> upper]
        df2 = df[df[columns] < lower]
        df_acc_removed=df[(df[columns]< upper) & (df[columns]> lower)] 
    
    df=df_acc_removed
    
    