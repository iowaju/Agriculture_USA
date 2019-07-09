import os
import urllib
import numpy as np
import pandas as pd
import datetime
import gdal
import pyodbc
import time
from ftplib import FTP
from datetime import timedelta, date
from collections import OrderedDict
global i

print "starting"
today = datetime.datetime.strptime(str(date.today()), "%Y-%m-%d").strftime("%Y/%m/%d")
yyyymmdd = datetime.datetime.strptime(str(today), "%Y/%m/%d").strftime("%Y%m%d")
hour = datetime.datetime.fromtimestamp(time.time()).strftime('%H')
run = ""
print int(hour)

if int(hour) > -1 and int(hour) < 7:
    run = "18"
    today = datetime.date.today() - timedelta(days = 1)
    today.strftime('%Y/%m/%d')
    yyyymmdd = today.strftime('%Y%m%d')

if int(hour) > 6 and int(hour) < 13:
    run = "00"
if int(hour) > 12 and int(hour) < 19:
    run = "06"
if int(hour) > 18 and int(hour) < 25:
    run = "12"

#run ="{:02d}".format(int(int(hour) - int(hour) % 6))

lista_var = []
directory = 'C:/geospatial/variables/forecast/gfs/anl/'
croplist = ['soy',
            'corn',
            'cotton',
            'wheat']

filelist = ['024', '048', '072', '096',
            '120', '144', '168', '192',
            '216', '240', '264', '288',
            '312', '336', '360', '384']

vars_to_get = ['Pressure [Pa]',
               'Specific humidity [kg/kg]',
               'Relative humidity [%]',
               'Volumetric Soil Moisture Content [Fraction]',
               'Water runoff [kg/(m^2)]',
               'Maximum temperature [C]',
               'Minimum temperature [C]',
               'Precipitable water [kg/(m^2)]',
               'Potential Evaporation Rate [W/(m^2)]',
               'Precipitation rate [kg/(m^2 s)]',
               'Downward Short-Wave Rad. Flux [W/(m^2)]',
               'u-component of wind [m/s]',
               'v-component of wind [m/s]',
               'Wind speed (gust) [m/s]']

def downloading():
    for filename in os.listdir(directory):
        os.remove(directory + filename)
    for hours in filelist:
        down_file = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=gfs.t"+str(run)+"z.pgrb2.0p25.f" + str(hours) +"&all" \
                    "_lev=on&all_var=on&subregion=&leftlon=-118&rightlon=-81&toplat=51&bottomlat=29&dir=%2Fgfs." + str(yyyymmdd) + "/" + str(run)
        print down_file
        urllib.urlretrieve(down_file, directory + str(yyyymmdd) + str(run) + str(hours) + ".anl")

def processing():
    global i
    i = 1
    dataframes = ['df_concat_soy',
                  'df_concat_corn',
                  'df_concat_wheat',
                  'df_concat_cotton',
                  'df_concat_soy2',
                  'df_concat_corn2',
                  'df_concat_wheat2',
                  'df_concat_cotton2']

    for df in dataframes:
        exec('{} = pd.DataFrame()'.format(df))
    
    for filename in os.listdir(directory):
        hour = filename[10:13]
        if filename.endswith(".anl"):
            dataset = gdal.Open(directory + filename)

            while i < dataset.RasterCount:
                band = dataset.GetRasterBand(i)
                cols = dataset.RasterXSize
                rows = dataset.RasterYSize
                data_array = band.ReadAsArray(0, 0 ,cols,rows)
                metadata = band.GetMetadata()
                for n in metadata:
                    if n == 'GRIB_COMMENT':
                        a = metadata['GRIB_COMMENT']
                        for z in vars_to_get:                    
                            if z == a:
                                v = metadata['GRIB_SHORT_NAME']
                                if v == "0-SFC" or v == "100-ISBL" or v =="2-HTGL" or v == "0-0.1-DBLL":
                                    m, n = data_array.shape
                                    ma, na = int(m), int(n)
                                    
                                    lat_list = []
                                    lon_list = []
                                    num_list = []

                                    for L in range(1, ma):
                                        for A in range(1, na):
                                            pixel_val = data_array[L, A]
                                            lat_list.append(L)
                                            lon_list.append(A)
                                            num_list.append(pixel_val)
                                    
                                    df_lat = pd.DataFrame(lat_list) 
                                    df_lon = pd.DataFrame(lon_list) 
                                    num_list = pd.DataFrame(num_list)
                                    
                                    dfs = pd.concat([df_lat, df_lon, num_list], axis = 1, join = 'inner')
                                    
                                    dfs.columns = ['lat', 'lon', 'value']

                                    dfs['lat'] = dfs['lat'] * 0.25
                                    dfs['lon'] = dfs['lon'] * (-1)
                                    dfs['lon'] = dfs['lon'] * 0.25

                                    dfs['lat'] = dfs['lat'] + 29
                                    dfs['lon'] = dfs['lon'] - 81

                                    dfs['latlon'] = dfs['lat'].astype(str) + dfs['lon'].astype(str)                                                     
                                    dfs['latlon'] = dfs['latlon'].astype(str)                           
                                    dfs['value'] = dfs['value'].astype(np.float64)
                                    #print filename + " | " + a +  " | " + v

                                    for crop in croplist:

                                        df_open = pd.read_csv("C:/geospatial/variables/forecast/gis/" + crop + "_gfs_counties_fix.csv")
                                        df_joined = dfs.join(df_open.set_index('latlon'), on = 'latlon', lsuffix = '_caller', rsuffix = '_other')   
                                        df_avg = df_joined.groupby(['state','county'], as_index = False)['value'].mean()
                                        df_avg['day_forecasted'] = datetime.date.today() + timedelta(days = (int(hour) / 24))
                                        df_avg['hour'] = hour
                                        df_avg['variable'] = a
                                        df_avg['date'] = str(today)
                                        df_avg['crop'] = crop
                                        df_avg['run'] = run
                                        
                                        if crop == "soy":
                                            df_concat_soy = pd.concat([df_concat_soy, df_avg])    
                                        if crop == "corn":
                                            df_concat_corn = pd.concat([df_concat_corn, df_avg])    
                                        if crop == "wheat":
                                            df_concat_wheat = pd.concat([df_concat_wheat, df_avg])    
                                        if crop == "cotton":
                                            df_concat_cotton = pd.concat([df_concat_cotton, df_avg])                                               
                i += 1
            dataset = None
            os.remove(directory + filename)
        i = 1
        #df_concat.to_csv(directory + filename + "_" + z[0:5] + "_FCT.csv",index = False)
        df_concat_soy2 = pd.concat([df_concat_soy2, df_concat_soy]) 
        df_concat_corn2 = pd.concat([df_concat_corn2, df_concat_corn]) 
        df_concat_wheat2 = pd.concat([df_concat_wheat2, df_concat_wheat]) 
        df_concat_cotton2 = pd.concat([df_concat_cotton2, df_concat_cotton]) 

        df_concat_soy = pd.DataFrame()
        df_concat_corn = pd.DataFrame()
        df_concat_wheat = pd.DataFrame()
        df_concat_cotton = pd.DataFrame()

    df_concat_soy2.to_csv('C:/geospatial/variables/forecast/gfs/csv/' + "Soy_GFS_Variables.csv",index = False)
    df_concat_corn2.to_csv('C:/geospatial/variables/forecast/gfs/csv/' + "Corn_GFS_Variables.csv",index = False)
    df_concat_wheat2.to_csv('C:/geospatial/variables/forecast/gfs/csv/' + "Wheat_GFS_Variables.csv",index = False)
    df_concat_cotton2.to_csv('C:/geospatial/variables/forecast/gfs/csv/' + "Cotton_GFS_Variables.csv",index = False)

def publishing():
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=Geospatial;Trusted_Connection=Yes')
    cursor = conn.cursor()
    for crop in croplist: #loop thru all crops   
        dataframe = pd.read_csv("C:/geospatial/variables/forecast/gfs/csv/" + crop + '_GFS_Variables.csv')
        shape = dataframe.shape[0]
        index = 0
        while index < shape:
            state = dataframe.iloc[index]['state']
            county = dataframe.iloc[index]['county'].decode('utf8')
            value = dataframe.iloc[index]['value']
            d2 = dataframe.iloc[index]['day_forecasted']
            day_forecasted = d2.replace("/", "-")
            h = dataframe.iloc[index]['hour']
            hour = str(h)
            variable = dataframe.iloc[index]['variable']
            d = dataframe.iloc[index]['date']
            date = d.replace("/", "-")
            r = dataframe.iloc[index]['run']
            run = str(r)
            geoid = str(state) + "_" + county
            country = "US"
            index += 1

            cursor.execute("INSERT INTO dbo.forecast_("
                           "geoid, "
                           "state, "
                           "county, "
                           "value, "
                           "variable, "
                           "crop, "
                           "date, "
                           "hour, "
                           "run, "
                           "day_forecasted) "
                           "VALUES (?,?,?,?,?,?,?,?,?,?)",
                           geoid,
                           state,
                           county,
                           value,
                           variable,
                           crop,
                           date,
                           hour,
                           run,
                           day_forecasted)
            conn.commit() 
        print crop + " file published "
    print "All information has been published for the run " + str(run) + "for the date of " + str(date)
downloading()
print "Files downloaded!"
processing()
print "Files processed!"
publishing()
