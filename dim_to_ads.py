"""
@Author      :   Tairan Ren, Billie Liu, Wenhu Cheng, Yi Zheng
@Time        :   2022/11/16 14:07:13
@Class       :   Fall2022 CS5001
@Description :   this file has functions to add dimension data to form middle level charts, and then do the counting 
                 based on those dimensions, so that all the data in ads level is graph-ready and only refers to one
                 indicator.
"""
import numpy as np
import pandas as pd
import os_helper as osh

MONTH = ['Jan', 'Feb', 'Mar', 'Apr','May','Jun', 'Jul', 'Aug','Sep', 'Oct', 'Nov', 'Dec']
FILE_PATH_ODS = 'data/ods/'
FILE_PATH_ADS = 'data/ads/'
FILE_PATH_DIM = 'data/dim/'

def read_csv(filename):
    return pd.read_csv(filename)

# the function drops the undesired columns leave only the id and the desired dimension column
def dimension_drop(df:pd.DataFrame,dimension:str):
    '''
    params: df : dataframe from reading files
            dimension : the columns to be selected as the dimension
    return: dropped columns other than id and target dimension columns
    '''
    drop_list = ['id',dimension]
    headers = df.columns.tolist()
    dropping = [item for item in headers if item not in set(drop_list)]
    return df.drop(columns=dropping)
   
# erase the duplicated rows
def drop_duplicate(df:pd.DataFrame,key:str):
    return df.drop_duplicates(subset=key)

# create the dim data, dimension data with id , will generate dim files
def dim_to_file(filename:str,dimension:str):
    '''
    params: filename : the dim files that to be written
            dimension : the names of columns to be selected as the dimension 
    return: the created file name
    The function will read files from targeted ods level, and automatically check if which encoding is
    and auto name the out put files
    '''
    filepath = 'data/ods/'
    df = read_csv(filepath+filename)
    df_dropped = drop_duplicate(df,'id')
    # drop the other columns, only leave the key and the counts
    df_month = dimension_drop(df_dropped,dimension)
    print('printing first 5 rows:' )
    print(df_month[0:5])
    if filename[10:len(filename)] == 'douban.csv':
        encoder = 'utf-8-sig'
    else:
        encoder = 'utf-8'
    #to rename the columns
    #df_month.columns = remove_headers_dim(df_month)
    df_month.to_csv(f'data/dim/dim_{dimension}_{filename[10:len(filename)]}',encoding=encoder,index=False)
    name = 'dim_'+ dimension +'_' + filename[10:len(filename)]
    return name

# one of the ads data methods groupby and count
def groupby_count(df:pd.DataFrame,column:str):
    '''
    params: df : the dataframe to do counting
            column : which column to grouby as key, usually id
    return: the new df with groupby and count datas
    The function uses groupby (sql) and count how many rows in each group 
    '''
    df_group = df.groupby(column).count().reset_index()
    #print(df_group[0:5])    
    return df_group

# ads write to csv, will automatically generate file names 
def oped_write_csv(df:pd.DataFrame,filename:str,optype:str,key:str):
    '''
    params: df : the dataframe to do counting
            column : which column to grouby as key, usually id
    return: the new df with groupby and count datas
    The function uses groupby (sql) and count how many rows in each group 
    '''
    if filename == 'douban':
        encoder = 'utf-8-sig'
    else:
        encoder = 'utf-8'
    df.to_csv(f'{FILE_PATH_ADS}/ads_{key}_{optype}_{filename[-10:len(filename)]}',encoding=encoder,index=False)

# the main method to write ads data to file ads level
def ads_oped_data(filename:str,key:str,optype:str = None):
    '''
    params: filename : the dim filesnames
            optype : what has been down to the datas, grouped at the moment
            key: the columns to be pass to groupby count method
    return: the new df with groupby and count datas
    The function reads the dim file and do the counting to produce ads level files
    '''
    filepath= FILE_PATH_DIM
    df = read_csv(filename= filepath+filename)
    df_grouped = groupby_count(df,key)
    oped_write_csv(df_grouped,filename,'grouped',key)



# the generalized helper function 
def ads_concacted_two(filename:str,key):
    '''
    params: filename : the dim filesnames
            month_key : 
            key: the columns to be pass to groupby count method
    return: NA
    The general method writes the actual data and upgrade it to ads level files
    dim_type_audience_score_rotten.csv
    dim_month_type_rotten.csv
    '''
    col_name =''
    df_merge = read_csv(filename)
    list_str = filename.split('_')
    for i in list_str[1:-1]:
        col_name += i + '_'
    col_name = col_name[0:-1]
    col_one = []
    col_two = []
    for row in df_merge.itertuples():
        if key in getattr(row,col_name):
            col_one.append(getattr(row, col_name))
            col_two.append(getattr(row, 'counts'))
 
    df_new = pd.DataFrame({col_name:col_one,'counts':col_two})
    oped_write_csv(df = df_new,filename=filename,optype='counts',key=f'{col_name}_{key}')

# the extra method only for rotten tomato to sort the data based on month string
def sort_dict_rotten(file:str):
    '''
    params: file : the file that has the problem of unsorted month
    return: NA
    The methods sorts the month and rewrite them to the old file.
    '''
    # for the case rotten tomato data doesn't has a sorted months
    df = read_csv(file)
    dict_movie = {}
    result = []
    for i in range(0, len(df)):  
        for j in range(0, df.shape[1]):
            result.append(df.iloc[i][j])

    month = result[::2]
    value = result[1::2]

    for i in range(len(month)):
        dict_movie.update({month[i]:value[i]})
    result = {}
    for i in range(len(MONTH)):
        if MONTH[i] in dict_movie:
            value = dict_movie[MONTH[i]]
        else:
            value = 0
        result.update({MONTH[i]:value})
    
    df_sorted = pd.DataFrame({'month':result.keys(),'movie_num':result.values()})
    df_sorted.to_csv(file,encoding='utf-8',index=False)

# the function specifically for the case of type month to sort
def sort_ads_type_month(file:str,key:str):
    '''
    params: file : the file that has the problem of unsorted month
            key : the concactenated column names 
    return: NA
    The methods sorts the combined two keys and write them to the old file.
    '''
    df = read_csv(file)
    dict_movie = {}
    result = []
    for i in range(0, len(df)):  
        for j in range(0, df.shape[1]):
            result.append(df.iloc[i][j])

    month = result[::2]
    value = result[1::2]

    for i in range(len(month)):
        dict_movie.update({month[i]:value[i]})

    result = {}
    for i in range(len(MONTH)):
        dict_key = f'{key}_{MONTH[i]}'
        if dict_key in dict_movie:
            value = dict_movie[dict_key]
        else:
            value = 0
        result.update({dict_key:value})

    df_sorted = pd.DataFrame({'type_month':result.keys(),'counts':result.values()})
    print(df_sorted[0:1])
    df_sorted.to_csv(file,encoding='utf-8',index=False)

# the function that remove _ from headers
def remove_headers_dim(dataframe:pd.DataFrame):
    '''
    cases that there is _ in header which will interfernce with later operation
    '''
    header_ = ''
    new_header = []
    for header in list(dataframe.columns):
        if '_' in header:
            list_ = header.split('_')
            for i in range(len(list_)):
                
                if i > 0:
                    list_[i] = list_[i].capitalize()
                    header_ += list_[i]
                else:
                    header_ += list_[i]
            new_header.append(header_)
        else:
            new_header.append(header)
    return new_header
                
def dimension_general(web :str, type = 'audience_score' ):
    '''
    input : web : data source
            type : the dimension column
    write to the file created automatically
    '''
    def serach_headers(filename):
        '''
        the inner fuction that loops the header, search the matching columns
        and return the selected columns.
        '''
        columns = []
        filepath = FILE_PATH_ODS
        df = read_csv(FILE_PATH_ODS + filename)
        
        headers = list(df.columns)
        for i in headers:
            if type in i:
                columns.append(i)
        if len(columns) > 1:
            print(columns)
            num = int(input('which one do you mean, from 0: '))
            return columns[num] 
        else:
            return columns[0]

    if web == 'douban':
        filename = 'ods_etled_douban.csv'
        score = 'score'
    elif web == 'rotten' :
        filename = 'ods_etled_rotten.csv'
        score = 'aduience_score'
    else:
        # for future expansion
        pass
    key = serach_headers(filename)
    return dim_to_file(filename,key) , key




# the general version therotically this method should be able to deal with any two columns
def dimension_concact_ads(type_one,type_two,web,key_column = None):
    '''
    params: type one : the columns to be joined 
            type_two : the data source
            web : data source
            key_column : not implemented at the moment
    return: NA
    The methods will concactenate two columns and do the counting methods and upgrade to ads level
    '''
    df_type_one = read_csv(f'data/dim/dim_{type_one}_{web}.csv')
    df_type_two = read_csv(f'data/dim/dim_{type_two}_{web}.csv')
    df_merge = df_type_one.merge(df_type_two,how='inner',on='id')
    df_merge[f'{type_one}_{type_two}'] = df_merge.apply(lambda x:str(x[type_one])+"_"+str(x[type_two]) if ((x[type_one] != None) and (x[type_two] != None)) else np.nan,axis=1)    
    df_merge = groupby_count(df_merge,f'{type_one}_{type_two}')
    df_merge['counts'] = df_merge['id']
    df_merge = df_merge.drop(['id',type_one,type_two],axis=1)
    print(df_merge[0:1])
    df_merge.to_csv(f'data/dim/dim_{type_one}_{type_two}_{web}.csv',encoding='utf-8')
    ads_concacted_two(f'data/dim/dim_{type_one}_{type_two}_{web}.csv',key_column)
    


'''
1.0 methods that have been upgraded
# single key month , and group by it data
def dimension_month_ads():
    
    dim_to_file('ods_etled_douban.csv','month')
    dim_to_file('ods_etled_rotten.csv','month')
    ads_oped_data('dim_month_rotten.csv','grouped','month')
    ads_oped_data('dim_month_douban.csv','grouped','month')
    # the special function only for sorting the rotten 
    
# single key type , and group by it data, the reason why i didn' combine this with 
# the dimension_month_ads is simply because website douban is using a different colu
def dimension_type_ads():
   
    dim_to_file('ods_etled_douban.csv','primary_type')
    dim_to_file('ods_etled_rotten.csv','type')
    ads_oped_data('dim_type_rotten.csv','grouped','type')
    ads_oped_data('dim_primary_type_douban.csv','grouped','primary_type')

# the helper function to upgrade to ads level, will pick from apointed month to exract the data
def ads_monthType(filename:str,month_key):
    
    params: filename : the dim filesnames
            month_key : 
            key: the columns to be pass to groupby count method
    return: NA
    The method writes the actual data and upgrade it to ads level files
    
    df_merge = read_csv(filename)
    month = []
    value = []
    for row in df_merge.itertuples():
        if month_key in getattr(row,'month_type'):
            month.append(getattr(row, 'month_type'))
            value.append(getattr(row, 'counts'))
 
    df_new = pd.DataFrame({'month_type':month,'counts':value})
    oped_write_csv(df = df_new,filename=filename,optype='counts',key=f'monthType{month_key}')

# concact two of them to form new keys
def dimension_monthAndType_ads(month):
    
    params: month : the exact month of data to 
    return: NA
    The methods first creates dim files from ods files, and then creates ads files from dim files for type dimension
    
    # get the dataframe for pointed columns
    df_type = read_csv('data/dim/dim_type_rotten.csv')
    df_month = read_csv('data/dim/dim_month_rotten.csv')
    # join on id so that they will be in the same df
    df_merge = df_type.merge(df_month,how='inner',on='id')
    # create new columns and add none value if there is no value in it
    df_merge['month_type']=df_merge.apply(lambda x:str(x['month'])+"_"+x['type'] if (x['month'] != None) else np.nan,axis=1)
    # group by and count 
    df_merge = groupby_count(df_merge,'month_type')
    # rename the header
    df_merge['counts'] = df_merge['id']
    # drop unwanted duplicated columns 
    df_merge = df_merge.drop(['id','type','month'],axis=1)
    # write to file 
    df_merge.to_csv(f'data/dim/dim_montyAndType_rotten.csv',encoding='utf-8')
    # upgrade file to ads level
    ads_monthType('data/dim/dim_montyAndType_rotten.csv',month)
'''

if __name__ == "__main__" :
    osh.change_os_path()

    for i in ['douban','rotten']:
        for j in ['month','type','score']:
            created_filename , key = dimension_general(i,j)
            print(created_filename + ' created')
            ads_oped_data(created_filename,key)
            print('ads upgraded')
    
    #sort_ads_type_month('data/ads/ads_type_month_action_counts_rotten.csv','action')
    #dimension_concact_ads('month','type','rotten','Nov')
    #dimension_concact_ads('type','audience_score','rotten','action')
    
    
