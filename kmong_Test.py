# Write your code here!
# Write your code here!
# Write your code here!
# Write your code here!
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

########################################### data preprocessing (raw_log)##########################################
# (1) canonicaldeviceuuid column name change
raw_log.rename(columns = {'canonicaldeviceuuid' : 'userid'}, inplace=True)
# (2) 
raw_log['eventdatetime'] = pd.to_datetime(raw_log['eventdatetime'], format='%Y-%m-%d %H:%M:%S', errors='raise')
raw_log['eventdatetime_year'] = raw_log['eventdatetime'].dt.year
raw_log['eventdatetime_month'] = raw_log['eventdatetime'].dt.month
raw_log['eventdatetime_day'] = raw_log['eventdatetime'].dt.day
raw_log['eventdatetime_hour'] = raw_log['eventdatetime'].dt.hour
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

raw_log.rename(columns = {'eventcategory' : 'event_category', 'isfirstactivity' : 'is_first_activity', 'apppackagename' : 'apppackage_name', 'appversion' : 'app_version',
       'devicetype' : "device_type", 'devicemanufacturer' : 'device_manufacturer', 'osversion' : 'os_version', 'userid' : 'user_id', 'sourcetype' : 'source_type',
       'inappeventcategory' : 'in_app_event_category', 'inappeventlabel' : 'in_app_event_label', 'eventdatetime' : 'event_datetime', 'rowuuid' : 'rowuu_id',
       'isfirstgoalactivity' : 'is_first_goal_activity', 'eventdatetime_year' : 'event_date_time_year',
       'eventdatetime_month' : 'event_date_time_month', 'eventdatetime_day' : 'event_date_time_day', 'eventdatetime_minute' : 'event_date_time_minute',
       'eventdatetime_second' : 'event_date_time_second', 'ostype(clean)' : 'os_type', 'osversion(clean)' : 'os_version',
       'viewcategory' : 'view_category', 'viewid' : 'view_id', 'viewaction' : 'view_action', 'eventdatetime_hour' : 'event_date_time_hour'},inplace= True)

# print(raw_log.columns)
raw_log = raw_log[['rowuu_id','apppackage_name','user_id','event_datetime','event_date_time_year','event_date_time_month','event_date_time_day','event_date_time_hour',
'event_date_time_minute','event_date_time_second','device_manufacturer','device_type','os_type','os_version','app_version','event_category','view_category',
'view_id','view_action','in_app_event_category','in_app_event_label','source_type','channel','params_campaign','params_medium','params_term','is_first_activity',
'is_first_goal_activity']]


########################################### data preprocessing (raw_funnel)##########################################
raw_funnel.rename(columns = {'Lv1' : 'lv1', 'Lv2' : 'lv2', 'viewid' : 'view_id', 'viewid desc' : 'view_desc', 'funnel name' : 'funnel_name', 'funnel desc' : 'funnel_desc'},inplace=True)
raw_funnel = raw_funnel[['lv1', 'lv2', 'view_id', 'view_desc', 'funnel_name', 'funnel_desc']]

########################################### data preprocessing (raw_category)##########################################
raw_category = pd.read_csv("data/kmong-category.csv")
raw_category.rename(columns = {'categoryid' : 'category_id', 'categoryname' : 'category_name', 'cat1_id' : 'category1_id', 'cat2_id' : 'category2_id', 'cat3_id' : 'category3_id',
'cat1' : 'category1', 'cat2' : 'category2', 'cat3' : 'category3'}, inplace = True)

########################################### data preprocessing merge##########################################
merge_data = raw_log.merge(raw_funnel, on = "view_id", how = 'left')
merge_data = merge_data.merge(raw_category, left_on = "in_app_event_label", right_on = "category_id", how = 'left')
merge_data.drop(['in_app_event_category', 'in_app_event_label', 'source_type'],axis=1)

merge_data = merge_data[['rowuu_id','apppackage_name','user_id','event_datetime','event_date_time_year','event_date_time_month','event_date_time_day',
'event_date_time_hour','event_date_time_minute','event_date_time_second','device_manufacturer','device_type','os_type','os_version','app_version',
'event_category','view_category','view_id','view_action','funnel_desc','view_desc','category_name','category1','category2','category3','channel',
'params_campaign','params_medium','params_term','is_first_activity','is_first_goal_activity']]
merge_data.head()

merge_data = merge_data.set_index("rowuu_id")
merge_data.head()