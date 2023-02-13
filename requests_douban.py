"""
@Author      :   Tairan Ren, Wenhu Cheng
@Time        :   2022/11/01 12:50:29
@Class       :   Fall2022 CS5001
@Description :   the requests from rotten douban, will create requests and store the data in files with relating names
"""
import time
import requests
import json
import os_helper as osh

FILEPATH = 'data/'

# create and return the request from pointed url
def create_request(page:str,genre):
    '''
    params: the pages wanted, the genre of the moveis, when doing full, here is numbers to 32
    return: the rsponse created, simulations of human surfing internet and with data requested
    The function creates and returns the data
    '''
    base_url = f'https://movie.douban.com/j/chart/top_list?type={genre}&interval_id=100:90&action=&'
    # the starting point and each scroll  
    param = {
        'start':page,
        'limit':20, 
    }
    # using UA to simulate  a user surfing internet
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'
    }
    # using timeout param to wait for connection, and retry when one request fails
    flag = True
    while flag :
        try:
            response = requests.get(url=base_url,params=param,headers=headers,timeout=2)
            flag = False
        except Exception as e:
            print(e.args)
            print('retrying')
            time.sleep(1)

    return response

# transform and return the data in json format 
def data_toJson(response):
    return response.json()

# create file and store the data in it, and will automaticly create file names
def download(data_json:str,filename:str):
    '''
    params: the content requested, and the file name where it should be stored
    return: NA
    The function uses io to store the data in json files
    '''
    filename = filename + '.json'
    with open(filename,'w',encoding='utf-8') as fp:
        json.dump(data_json,fp=fp,ensure_ascii=False)
    print('done')

# the main function to call all the other method
def request_all(pages = 5):
    '''
    params: page as in the scroll times, default at 5
    return: NA
    The major function calls all the other function to fullfill the process
    32 as the website is using numbers to mark types, and there are 32 in total
    '''
    
    for i in range(1,32):
        for j in range(pages):
            response = create_request(page=0,genre= i)
            list_data = data_toJson(response=response)
                #download(data_json=list_data,file_name=FILEPATH+GENRE[i]+f'_{i}'+'.json')
            download(data_json=list_data,filename= FILEPATH+'douban/' + f'_{i}_{j}')
                #time.sleep(1)   
        
    '''
    flag = True
        pages = 0 
        genres = 0
        counter = 0
        while flag :
            try:
                response = create_request(page=pages,genre= genres)
                list_data = data_toJson(response=response)
                #download(data_json=list_data,file_name=FILEPATH+GENRE[i]+f'_{i}'+'.json')
                download(data_json=list_data,file_name= FILEPATH+'douban/' + f'_{i}_{j}'+'.json')
                i += 20
                j += 1
            except Exception as e:
                print(e.args)
                print('rebooting')
    '''

if __name__ == "__main__" :
    osh.change_os_path()
    osh.create_data()
    request_all()