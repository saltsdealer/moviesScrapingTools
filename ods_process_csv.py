"""
@Author      :   Tairan Ren , Yi Zheng
@Time        :   2022/11/08 13:10:28
@Class       :   Fall2022 CS5001
@Description :   these codes do a extended cleaning to the datas, includes funtions that could remove unwanted columns or single column
                 , drop duplications, breaking nested strings, and adding dimensional data columns.
                 It will transform all the orginal csv data to operatable status.
"""
import pandas as pd
import os_helper as osh

'''
    three layer data modelling 
    ods, etl, orginal data, added columns
    dim, dimensions : month, type, country(only douban)
    ads, movies spreaded into month and types and country
'''
def read_csv(filename):
    return pd.read_csv(filename)

# this function loop throhgh the list of unwanted columns and drop them
def etl_remove_list(df,columns:list):
    '''
    params: df : dataframe from reading files
            columns : the columns to be deleted
    return: copied dataframe with columns deleted
    The function loops through the dataframe and delete unwanted columns,
    '''
    headers = df.columns.values
    bad_header = []
    for i in columns:
        for header in headers:
            if i in header:
                bad_header.append(str(header))
    
    print(f'droping: {bad_header}' )
    df_copy = df.drop(bad_header,axis=1)
    return df_copy

# the single column version of dropping 
def etl_remove(df,column):
    '''
    params: df : dataframe from reading files
            column : the column to be deleted
    return: copied dataframe with column deleted
    The function that only delete one column,
    '''
    df_copy = df 
    headers = df.columns.values
    bad_header = []
    for header in headers:
        if column in header:
            bad_header.append(str(header))
        
    df_copy.drop(columns=bad_header)
    return df_copy

# erase duplications based on id by default
def etl_duplicate(df,column:str = 'id'):
    '''
    params: df : dataframe from reading files
            column : check which column as the base for duplication
    return: copied dataframe with column deleted
    The function that only delete one column,
    '''
    return df.drop_duplicates(subset=column)

# extract the month column from time column
def adding_month(df,header,website):
    '''
    params: df : dataframe from reading files
            header : the column headerse
            website : datasources
    return: the value from matching methods return
    '''

    month = []
    df_copy = df
    if website == 'douban':
        return adding_month_douban(df,header,month,df_copy)
    if website == 'rotten':
        return adding_month_rotten(df,header,month,df_copy)

def adding_month_douban(df,header,month,df_copy):
    for string in df[header]:
        if len(string)> 4:
            list_time = string.split('-')
            #print(list_time)
            month.append(list_time[1])
        else:
            month.append(None)

    print(f'adding month column')
    df_copy['month'] = month 
    return df_copy

def adding_month_rotten(df,header,month,df_copy):
    for string in df[header]:
        if len(string) > 0:
            list_time = string.split(' ')
            month.append(list_time[1])

        else:
            month.append(None)
    
    print('adding month column')
    df_copy['month'] = month 
    return df_copy

# this is general methods specifically
def adding_primary_types(df,website):
    '''
    params: df : dataframe from reading files
            website : datasources
    return: the value from matching methods return
    '''
    primary_types = []
    
    if website == 'douban':
        return adding_types_douban(df,primary_types)
    if website == 'rotten':
        return adding_types_rotten(df,primary_types)

def adding_types_douban(df_copy,primary_types):
    
    for i in df_copy['types']:
        if not (len(i) == 0):
            end_index =i.find("'",3,len(i))
            primary_types.append(str(i[2:end_index]))
    df_copy['primary_type'] = primary_types
    print('adding primary types')
    return df_copy

# not needed at the moment
def adding_types_rotten(df:pd.DataFrame,primary_types):
# uses group by to find the ranking for each id and uses the highest as its primary type
    pass
    
# the method that explode the audience score
def adding_explode_audience(df):
    '''
    params: df : dataframe from reading files
    return: scores column exploded 
    '''
    value = []
   
    for string in df['audienceScore']:
        
        if len(string) > 2:
            end_index = string.find(",",0,len(string))
            value.append(string[10:end_index])
        else:
            value.append(None)
    
    df_copy = df
    df_copy['audience_score'] = value
    df_copy = df_copy.drop('audienceScore',axis = 1)
    return df_copy
    
# the method that explode the critic score
def adding_explode_critic(df):
    '''
    params: df : dataframe from reading files
    return: scores column exploded 
    '''
    value = []
    for string in df['criticsScore']:
        
        if len(string) > 26:
            end_index = string.find("'",9,len(string))
            string = string.split(',')
            
            value.append(string[1][9:end_index])
        else:
            value.append(None)
    
    df_copy = df
    df_copy['critics_score'] = value
    df_copy = df_copy.drop('criticsScore',axis = 1)
    return df_copy


def ods_douban(createTime =None):
    '''
    params: NA
    return: NA
    The major function calls all the other function to fullfill the process
    '''
    # read_csv
    df_douban = pd.read_csv(f'data/csv_douban/{createTime}ods_dou.csv')
    #drop unwanted columns:
    df_douban = etl_remove_list(df_douban,['is_','url','uri'])
    # spliting month
    df_douban = adding_month(df_douban,'release_date','douban')
    # spliting primary types
    df_douban = adding_primary_types(df_douban,'douban')
    # remove duplicates, don't seem neccessrary here.
    #df_douban = df_douban.drop_duplicates(subset='id')
    # write
    df_douban.to_csv('data/ods/ods_etled_douban.csv',encoding='utf-8-sig',index=False)

def ods_rot(createTime =None):
    '''
    params: NA
    return: NA
    The major function calls all the other function to fullfill the process
    '''
    # read_csv
    df_rot = pd.read_csv(f'data/csv_rotten/{createTime}ods_rot.csv')
    # drop unwanted columns:
    df_rot = etl_remove_list(df_rot,['Url','is','Uri','ems','mpx'])
    # extract month from orginal data as it is a dimension but needs processing
    df_rot = adding_month(df_rot,'releaseDateText','rotten')
    # explode the nested columns
    df_rot = adding_explode_audience(df_rot)
    df_rot = adding_explode_critic(df_rot)
    # write the data to csv
    df_rot.to_csv('data/ods/ods_etled_rotten.csv',encoding='utf-8',index=False)

if __name__ == '__main__':
    osh.change_os_path()
    ods_douban('7_')
    ods_rot('7_')
    
    
