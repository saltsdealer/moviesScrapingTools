"""
@Author      :   Tairan Ren , Yi Zheng , Xingyi Liu
@Time        :   2022/11/05 00:45:07
@Class       :   Fall2022 CS5001
@Description :   the functions to turn data to csv files, includes the functions to read from json files,
                 explode the nested json data, headers extraction, empty value to None and runs them to extract
                 for both website files.
"""

import json
import pandas as pd
import urlib_request as ulr
import os_helper as osh

DATA_PATH = 'data/csv/'
WEB_SITES = ['rotten','douban']
D_PAGES = 5
R_PAGES = 5

# return the loaded json data 
def read_json(file_name:str):
    # read from file and return  
    with open(file_name,'r',encoding='utf-8') as fp:
        content = json.load(fp)
    return content

# this function returns the part where all the value are stored, and explode the stucture to get
# the right movie lists
def movies_dicts_rotten(content:dict):
    '''
    params: the content from reading a file
    return: the extracted list of movies
    The function takes in the content from files and return the specific structure breaked 
    datas of movies
    '''
    #{grids:[{'id':xxx,'list':[{......}]}]}
    layerone = content.get('grids') # it is a nested structures with both dict and list
    list_movies = layerone[0]['list'] # it is another list with all movies in the given range 
    
    #for movie in list_movies: there cases that some movies doesn't align with others
    return list_movies 

#  this function returns the headers where all the value are stored
def movies_headers(web:str):
    '''
    params: the content from reading a file
    return: the headers of the movies 
    The function takes in the content from files and return the headers that was 
    key in orginal data source
    '''
    # it is a dict in list structure
    # cont = [{},{}]
    if web == WEB_SITES[1]:
        content = read_json(file_name='data/douban/_1_0.json')
        headers = []
        for key in content[0].keys():
            headers.append(key)
    
    if web == WEB_SITES[0]:
        content = read_json(file_name=f'data/rotten/{ulr.GENRE[0]}_0.json') 
        content = movies_dicts_rotten(content)
        headers = []
        for key in content[0].keys():
            headers.append(key)
        #headers.append('type')

    return sorted(headers) #with all the fields as dicts

# adding none value to the data's that has no value in it, return the data with none value added
def etl_rotten(c:list,headers:list):
    '''
    params: the content from reading a file with or without preprocessing, headers for looping
    return: data with all empty replaced by None
    The function loops by the headers and change all the empty value to None
    '''
    result = []
    for movie_dict in c:
        for key in headers:
            if not (key in movie_dict) :
                movie_dict.update({key:'None'})
        sorted_dict = dict(sorted(movie_dict.items(), key=lambda x: x[0]))
        result.append(sorted_dict)

    return result

# uses pandas to store the data in csv prepared format
def movies_data(content,type, web):
    '''
    params: the content from reading a file with or without preprocessing, 
            type: is the current movie type, only needed for rotten tomato
            web: which data source
    return: a two dimension list of each movie's data 
    The function switch the data to all movies into a list of 2 dimension, follow the orginial order
    '''
    movies = []
    for i in range(len(content)):
        values = content[i].values()
        movie = []
        for value in values:
            movie.append(value)
        if web == WEB_SITES[0]:
            movie[-1] = type
        movies.append(movie)
    
    return movies

def create_dataframe(movies):
    # uses pandas data frame
    return pd.DataFrame(data=movies)

# this function writes the passed in data to csv files
def write_csv(filename,headers,dataframe):
    '''
    params: filename : which file to write to
            headers: data header to form the first row of csv file
            dataframe: where all data are stored 
    return: NA
    The function check if the current df is empty, if not write to target file, with appending style
    '''
    # https://blog.csdn.net/toshibahuai/article/details/79034829



    if dataframe.empty:
        print('empty dataframe, skipping')
        return
    '''
    if osh.file_check(filename):
        print('file already exists, make sure you really wants it')
        inputs = input('enter y/n :')
        if inputs == 'y': 
            dataframe.to_csv(filename,header=headers,index = False,encoding='utf-8-sig',mode='a')
        else:
            print('file already exists system ending.')
            return
    else:
        '''
    dataframe.to_csv(filename,header=headers,index = False,encoding='utf-8-sig',mode='a')
    print('done')

# the main for douban 
def json_to_csv_douban(createTime = None):
    '''
    params: NA
    return: NA
    The main function calls all the other function to fullfill the process for douban
    '''
    headers_etl = sorted(movies_headers('douban'))
    
    for i in range(1,32):
        for j in range(D_PAGES):
            # read the file
            content = read_json(file_name=f'data/douban/_{i}_{j}.json') 
            # get the list of movies
            #etl
            content = etl_rotten(content,headers_etl)
            # trans form them to list of data
            list_data = movies_data(content,type='',web='douban')
            # create dataframe
            
            df = create_dataframe(list_data)
            # write to file
            print(f'{ulr.GENRE[i]} {j}')
            # prevent headers from multipling it self
            if i == 1 and j == 0:
                headers = headers_etl
            else:
                headers = None
            write_csv(filename=f'data/csv_douban/{createTime}ods_dou.csv',headers=headers,dataframe=df)
            #prevent headers from being imported multip
            

# the main for rotten tomato
def json_to_csv_rotten(createTime=None):
    '''
    params: NA
    return: NA
    The main function calls all the other function to fullfill the process for rotten
    '''
    # get the headers from each movie data
    headers_etl = movies_headers('rotten')

    for i in range(len(ulr.GENRE)):
        for j in range(R_PAGES):
            print(f'{ulr.GENRE[i]} {j}')
            # get the movie type, since the orginial didn't include that 
            file_type = ulr.GENRE[i]
            # read the file
            content = read_json(file_name=f'data/rotten/{ulr.GENRE[i]}_{j}.json') 
            # get the list of movies
            content = movies_dicts_rotten(content)
            # give none value to empty spot 
            content = etl_rotten(content,headers_etl)
            # trans form them to list of data for dataframe
            list_data = movies_data(content,type=file_type,web='rotten')
            # create dataframe
            # they some how changed the structure recently
            df = create_dataframe(list_data)
        
            # for testing 
            # print(df.shape[1])
            # print(df[1:3])

            # prevent headers from multipling it self
            if i == 0 and j == 0:
                headers = headers_etl
            else:
                headers = None
            print(headers)
            # write to file
            write_csv(filename=f'data/csv_rotten/{createTime}ods_rot.csv',headers=headers,dataframe=df)
            

if __name__ == "__main__" :
   
    osh.change_os_path()
    json_to_csv_rotten('7_')
    json_to_csv_douban('7_')
    '''
    sample columns a month ago, they've updated it recently
    [{'audienceScore': {'score': '100', 'sentiment': 'positive'}, 
    'criticsScore': {'certifiedAttribute': 'criticscertified', 'score': '91', 'sentiment': 'positive'}, 
    'fallbackPosterUrl': '//images.fandango.com/cms/assets/5d84d010-59b1-11ea-b175-791e911be53d--rt-poster-defaultgif.gif', 
    'mediaUrl': '/m/raging_fire', 'posterUri': 'https://resizing.flixster.com/_vyksJ9VR-Veuhv9JEYV_bi42Aw=/180x258/v2/https://resizing.flixster.com/-4X5YkKInP3RBB4qhuQ-QOOFudo=/ems.cHJkLWVtcy1hc3NldHMvbW92aWVzLzA1NDY3MTI1LTFkYzQtNDQ5My1hNGE2LTk3ZWVjMzdmNDE3Zi5qcGc=', 
    'title': 'Raging Fire', 
    'id': 'cc68d6bb-6d2c-388e-959f-d241b4bad34a', 
    'trailerUrl': 'https://player.theplatform.com/p/NGweTC/pdk6_y__7B0iQTi4P/embed/select/media/l3IC5C8m2dBO', 
    'isVideo': True, 
    'releaseDateText': 'Streaming Nov 23, 2021'},.......]
    '''
    
    


    
            

        