"""
@Author      :   Tairan Ren, Billie Liu, Wenhu Cheng, Yi Zheng
@Time        :   2022/11/16 14:07:13
@Class       :   Fall2022 CS5001
@Description :   this where the demo file were tested
"""
import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import dim_to_ads as tdm

#sys.path.insert

def read_csv(filename):
    return pd.read_csv(filename)

# one of the ads indicator of the data : month, draws the picture 
def graph_bar(filename,columns,barh = None, plot = None, sort = False):
    df = read_csv(f'data/ads/{filename}')
    if sort :
        df = df.sort_values(by='id',ascending=True)
    df_table = df.pivot_table(columns=columns)
    y_data = df[f'{columns}_'] = df.iloc[:,-1]
    x_data = df[columns]
    if barh == True:
        plt.barh(x_data,y_data,0.5,color = ['paleturquoise','mediumturquoise','lightseagreen','turquoise','aquamarine'])
    elif barh == False:
        plt.bar(x_data,y_data,0.5,color = ['darkslategray','teal','cadetblue','steelblue','darkcyan'])
    elif plot == True:
        plt.plot(x_data,y_data,0.5,marker='o',mfc='orange',ms=5,mec='c',lw=1.0,ls="-",c='green' )
        
    plt.xticks(rotation=270)
    plt.tight_layout()
    plt.show()
    return df_table

def multi_line(filenames:list,columns = None):
    
    color = ['darkgray','rosybrown','darkgoldenrod','darkslategray','midnightblue'] 
    for i in range(len(filenames)):
        nameList = filenames[i].split('_')
        columns = nameList[3]
        df = read_csv(f'data/ads/{filenames[i]}')
        y_data = df[f'{columns}_'] = df.iloc[:,-1]
        x_data = df[columns]
        flag = i
        if flag > len(color):
            flag -= len(color)
        plt.plot(x_data,y_data,label= columns,marker='o',mfc='orange',ms=5,mec='c',lw=1.0,ls="-",c= color[flag],  )

    plt.xticks(rotation=270)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__" :
    #graph_bar('ads_month_grouped_rotten.csv','month',barh = False)
    ## for the chinese website
    #graph_bar('ads_month_grouped_douban.csv','month')
    ## for the matplot to show Chinese has to set the fonts, this is the types for douban 
    #mpl.rcParams['font.family'] = 'SimHei'
    #graph_bar('ads_primary_type_grouped_douban.csv','primary_type') 
    # the types for rotten tomato
    #graph_bar('ads_type_grouped_rotten.csv','type',barh=True)
    #tdm.dimension_monthAndType_ads('Mar')
    #graph_bar('ads_monthTypeMar_counts_rotten.csv','month_type',barh=True)
    #tdm.dimension_concact_ads('type','month','action')
    #graph_bar('ads_type_month_action_counts_rotten.csv','type_month',plot=True)
    #tdm.sort_ads_type_month('data/ads/ads_type_month_action_counts_rotten.csv','action')
    #graph_bar('ads_type_month_action_counts_rotten.csv','type_month',plot=True)
    #tdm.dimension_concact_ads('type','month','sports')
    #tdm.sort_ads_type_month('data/ads/ads_type_month_sports_counts_rotten.csv','sports')
    #tdm.sort_ads_type_month('data/ads/ads_type_month_action_counts_rotten.csv','action')
    #multi_line(['ads_type_month_action_counts_rotten.csv','ads_type_month_sports_counts_rotten.csv'])
    pass
    #graph_bar('ads_month_grouped_rotten.csv','month',barh=False)
    #graph_bar('ads_month_grouped_douban.csv','month',barh=False)
    tdm.sort_ads_type_month('data/ads/ads_type_month_sports_counts_rotten.csv','sports')
# after sorting , the column names would be the same as the main key, this is only useful when month is joined
    #graph_bar('ads_type_month_sports_counts_rotten.csv','type_month',barh=True)