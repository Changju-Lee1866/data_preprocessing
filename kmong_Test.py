import pandas as pd
import numpy as np

pd.options.display.max_columns = 100

raw_log = pd.read_csv("data/kmong-conversion.csv")
raw_funnel = pd.read_csv("data/kmong-funnel.csv")
raw_category = pd.read_csv("data/kmong-category.csv")

def os_type(os):
    if 'iOS' in os:
        answer = 'iOS'
    elif 'Android' in os:
        answer = 'Android'
    return answer

def os_version(os):
    answer = os.replace('iOS','')
    answer = os.replace('Android','')
    return answer

def device_rename(devicemanufacturer):
    answer = ''
    if devicemanufacturer == 'samsung':
        answer = 'Samsung'
    elif devicemanufacturer == 'Apple':
        answer = 'Apple'
    elif devicemanufacturer == 'LGE' or devicemanufacturer == 'LG Electronics':
        answer = 'LG'
    else:
        answer = 'Other'
    return answer

def channel_rename(channel):
    answer = ''
    if  pd.isna(channel):
        return channel
    else:
        if 'google-play' in channel or 'google.adwords' in channel: # 'google'은 else에서 처리
            answer = 'google'
        elif 'WEB' in channel:
            answer = 'web'
        elif 'm_daum' in channel:
            answer = 'daum'
        elif 'm_naver' in channel or 'pc_naver' in channel or 'm_naverpowercontents' in channel:
            answer = 'naver'
        elif 'apple.searchads' in channel:
            answer = 'apple'
        else:
            answer = channel
    return answer

def viewcategory_rename(inappeventcategory):
    if pd.isna(inappeventcategory):
        return inappeventcategory

    if '_' in inappeventcategory:
        viewcategory = inappeventcategory.split('_')[0]
    else :
        viewcategory = inappeventcategory.split('.')[0]
    return viewcategory

def viewid_rename(inappeventcategory):
    if pd.isna(inappeventcategory):
        return inappeventcategory
    
    viewid = inappeventcategory.split('.')[0]
    return viewid

def viewaction_rename(inappeventcategory):
    if pd.isna(inappeventcategory):
        return inappeventcategory
    viewaction = inappeventcategory.split('.')[1]
    return viewaction

# (1) canonicaldeviceuuid column name change
raw_log.rename(columns = {'canonicaldeviceuuid' : 'userid'}, inplace=True)
# (2) 
raw_log['eventdatetime'] = pd.to_datetime(raw_log['eventdatetime'], format='%Y-%m-%d %H:%M:%S', errors='raise')
raw_log['eventdatetime_year'] = raw_log['eventdatetime'].dt.year
raw_log['eventdatetime_month'] = raw_log['eventdatetime'].dt.month
raw_log['eventdatetime_day'] = raw_log['eventdatetime'].dt.day
raw_log['eventdatetime_minute'] = raw_log['eventdatetime'].dt.minute
raw_log['eventdatetime_second'] = raw_log['eventdatetime'].dt.second
# (3) 
raw_log['ostype(clean)'] = raw_log['osversion'].apply(os_type)
raw_log['osversion(clean)'] = raw_log['osversion'].apply(os_version)
# (4) 
raw_log['devicemanufacturer'] = raw_log['devicemanufacturer'].apply(device_rename)
# (5) 
raw_log['channel'] = raw_log['channel'].apply(channel_rename)
# (6) 
raw_log['viewcategory'] = raw_log['inappeventcategory'].apply(viewcategory_rename)
raw_log['viewid'] = raw_log['inappeventcategory'].apply(viewid_rename)
raw_log['viewaction'] = raw_log['inappeventcategory'].apply(viewaction_rename)
# (7) 
raw_log.drop(['osversion','devicemanufacturer','channel','event_rank'],axis=1)

raw_log.columns