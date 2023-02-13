"""
@Author      :   Tairan Ren 
@Time        :   2022/12/06 21:36:23
@Class       :   Fall2022 CS5001
@Description :   This is the os helper function for os like, folder creation and 
                 if file existed
"""
import os

FILE_PATH = {'data':['csv_rotten','csv_douban','dim','ads','ods','rotten','douban']}

def create_data():
    '''
    the fucntion that creates the folders system
    '''
    keys = list(FILE_PATH.keys())
    
    if not os.path.exists(keys[0]):
        print('creating level 1 folder')
        os.makedirs(keys[0] + '/')

    for i in FILE_PATH[keys[0]]:
        if not os.path.exists(keys[0] + '/'+ i +'/'):
            print(f'creating level 2 {i+"/"}')
            try:
                os.makedirs(keys[0] + '/'+ i +'/')
            except Exception as e:
                print(e)

def file_check(filename):
    '''
    the fucntion check if the file exists
    '''
    return os.path.isfile(filename)

def change_os_path():
    '''
    the function change the current operating address
    '''
    path ,_ = os.path.split(os.path.realpath(__file__))
    print(path)
    if os.getcwd() != path:
        os.chdir(path)
        print('changed')