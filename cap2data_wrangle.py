import pandas as pd
import numpy as np

data = pd.read_csv('Demand_for_Los_Angeles_Department_of_Water_and_Power_(LDWP)_Hourly.csv',
                   header=4)
data1 = pd.read_csv('Day-ahead_demand_forecast_for_Los_Angeles_Department_of_Water_and_Power_(LDWP)_Hourly.csv',
                   header=4).iloc[16:,:]
total_data = data.merge(data1,right_on='Category',left_on='Category',how='left',suffixes=('left','right'))

#rename columns
total_data.columns = ['Date','Demand(MW)','Forecast(MW)']

#write initial data to file(first time only)
total_data.to_csv('Capstone2data.csv')

#-------------------------------------------------------- EXPLORATION 1
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import seaborn as sns
import holidays

data = pd.read_csv('Capstone2data.csv').iloc[:,1:]
data.info()
data['Date'] = data['Date'].astype(datetime)
data = data.set_index(pd.DatetimeIndex(data['Date'])).sort_index()

time_lapse = data.index[-1] - data.index[1]
time_span = []
for i in range(time_lapse.days*24 + 16):
    time_span.append(data.index[0] + timedelta(hours=i))


#grouped by day df
day1 = data.groupby(pd.Grouper(freq='1D')).apply(np.sum).iloc[:,1:]
day1.describe()
day1[day1['Demand(MW)']== int(day1['Demand(MW)'].max())]

#create day of the week feature
l=[]
for i in day1.index:
    d = i.isoweekday()
    l.append(d)
day1['DofWk'] = l
weekdays = pd.DataFrame(day1['Demand(MW)'].values,index=l,columns=['Demand(MW)'])
weekdays.index=weekdays.index.map(str)

#create holiday feature
holidayapi = dict(holidays.US(state='CA', years=[2015,2016,2017]))
holiday = pd.DataFrame.from_dict(holidayapi,orient='index')  
day1 = day1.merge(holiday,left_index=True,right_index=True,how='left'
                  ,suffixes=('left','right'))
l=[]
for i in day1[0].isnull():
    if i==True:
        l.append(0)
    else:
        l.append(1)
day1['holiday'] = l
day1[0] = day1[0].fillna('none')
day1.columns = ['Demand(MW)', 'Forecast(MW)', 'DofWk', 'name', 'holiday']

#grouped by week df
day7 = data.groupby(pd.Grouper(freq='7D')).apply(np.sum).iloc[:,1:]
day7.describe()
day7[day7['Demand(MW)']== int(day7['Demand(MW)'].max())]

#Visualization
titles= ['Weekly Counts','Daily Counts','Day of the Week Counts (7= Sun)',
         'Hourly Counts']
for i,t in zip([day7['Demand(MW)'],day1['Demand(MW)'],weekdays,data['Demand(MW)']],titles):
    try:
        i = i.to_frame('Demand(MW)')
        plt.scatter(i.index,i['Demand(MW)'])
        plt.xticks(rotation=45)
        plt.title(t)
        plt.ylabel('Demand(MW)')
        plt.xlabel('Time Interval')
        plt.show()
        print(i.describe())
    except:
        plt.scatter(i.index,i['Demand(MW)'])
        plt.xticks(rotation=45)
        plt.title(t)
        plt.ylabel('Demand(MW)')
        plt.xlabel('Time Interval')
        plt.show()
        print(i.describe())
        
        
plt.scatter(day1[day1['holiday']==0].index,day1[day1['holiday']==0]['Demand(MW)'],
            color='green')
plt.scatter(day1[day1['holiday']==1].index,day1[day1['holiday']==1]['Demand(MW)'],
            color='red')
plt.xticks(rotation=45)
plt.legend(['regular','holiday'])
plt.ylabel('Demand(MW)')
plt.xlabel('Time Interval')
plt.show()
print(day1[day1['holiday']==1].describe())
print(day1[day1['holiday']==1].sort_values(by='Demand(MW)',ascending=False).head(5))
        
#-------------------------------------------------WEBSCRAPPING
import requests
from bs4 import BeautifulSoup 

#---------------create weather feature      
d = {}
for i in range(len(day1)):
    try:
        r1=requests.get("https://www.wunderground.com/history/airport/KCQT/%s/%s/%s/DailyHistory.html?HideSpecis=1"%(day1.index[i].year,day1.index[i].month,day1.index[i].day))
        data1=r1.content
        soup1=BeautifulSoup(data1,"html.parser")
        finder = soup1.find_all("table",{"class":"obs-table responsive"})
        finder = finder[0].find_all("tr",{'class':'no-metars'})
        l = []
        for j in finder:
            time = j.find_all("td")
            if '7' in str(time[0].text):
                try:
                    l.append(time[1].find("span",{'class':'wx-value'}).text)
                except:
                    l.append(0.0)
        d[datetime(day1.index[i].year,day1.index[i].month,day1.index[i].day)] = l
        print(i)
    except:
        print('no data for',i)
        
#check for missing data 
for i in d:
    if len(d[i]) != 24:
        print(i,len(d[i]))
        
#check for error data 
for i in d:
    if len(d[i]) > 24:
        print(i,len(d[i]))

#save scraped weather data
weather = pd.DataFrame.from_dict(d,orient='index') 
weather = weather.fillna(0)
weather.to_csv('weather_data.csv')

#add weather feature to data
wdata = pd.read_csv('weather_data.csv',index_col = 0,parse_dates=True)
day1 = day1.merge(wdata,left_index=True,right_index=True,
                     how='outer',suffixes=('left','right')).fillna(0)

weather = []
for i in range(1007):
    if i != 998 or 999:
        w = day1.iloc[i,5:].tolist()
        weather.append(w)
    elif i == 998 or 999:
        weather.append([0]*24)
        print(i)    
weather = [element for list_ in weather for element in list_]

load_weather = pd.DataFrame(weather[:24160],index=time_span,columns=['Temp'])
growth1 = data.merge(load_weather,left_index=True,right_index=True,how='outer')
growth2 = growth1.merge(holiday,left_index=True,right_index=True,how='left').fillna(method='bfill')

