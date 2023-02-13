"""
@Author      :   Tairan Ren , Wenhu Cheng
@Time        :   2022/12/04 19:00:15
@Class       :   Fall2022 CS5001
@Description :   the urlibt version of requests for rotten tomato website, creates the request and store the data in files
"""
import os_helper as osh
import urllib.request as urr
import time

# extracted from the website
GENRE = ['action', 'adventure', 'animation', 'anime', 'biography', 'comedy', 'crime', 'documentary', 'drama', 'entertainment', 'faith_and_spirituality', 'fantasy', 'game_show', 'health_and_wellness', 'history', 'holiday', 'horror', 'house_and_garden', 'kids_and_family', 'lgbtq', 'music', 'musical', 'mystery_and_thriller', 'nature', 'news', 'reality', 'romance', 'sci_fi', 'short', 'soap', 'special_interest', 'sports', 'stand_up', 'talk_show', 'travel', 'variety', 'war', 'western']
# local store path
FILEPATH = 'data/'
# their weird paging system
PAGE = ['page=1','after=Mjk%3D','after=NTk%3D','after=ODk%3D','after=MTE5']

# create and return the request from url
def create_request(page:str,genre:str):
    '''
    params: the pages wanted, the genre of the moveis, when doing full, just use GENRE
    return: the request created, simulations of human surfing internet
    The function creates and returns the request
    '''


    base_url = f"https://www.rottentomatoes.com/napi/browse/movies_at_home/genres:{genre}~sort:audience_highest?"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'
    }

    url = base_url + page
    
    request = urr.Request(url=url,headers=headers)

    return request

# capture the request and fail prevention
def get_content(request):
    '''
    params: request for all the websites
    return: the content with from those requests
    The function uses the request to store and return the data
    '''

    flag = True
    while flag :
        try:
            response = urr.urlopen(request,timeout=2)
            flag = False
        except Exception as e:
            print(e.args)
            print('retrying')
            time.sleep(5)
    
    content = response.read().decode('utf-8')
    return content

# download the url content and write it to local files
def download(content,filename):
    '''
    params: the content requested, and the file name where it should be stored
    return: NA
    The function uses io to store the data in json files
    '''
    filename = filename + '.json'
    with open(filename,'w',encoding='utf-8') as fp:
        fp.write(content)

# the function loops to get all genre data
def requests_allgenre_pagefive_rotten():
    '''
    params: NA
    return: NA
    The major function calls all the other function to fullfill the process
    '''
    for i in range(len(GENRE)):
        for j in range(len(PAGE)):
            request = create_request(page=PAGE[j],genre=GENRE[i])
            content = get_content(request)
            download(content,FILEPATH+'rotten/'+GENRE[i]+f'_{j}')
            print(f'done {GENRE[i]} {PAGE[j]}')
            #time.sleep(1) # to prevent being recognized as robots， probably overthinking it
  
if __name__ =='__main__' :
    osh.change_os_path()
    osh.create_data()
    requests_allgenre_pagefive_rotten()
    
    
    
   
    