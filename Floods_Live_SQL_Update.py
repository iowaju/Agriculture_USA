import urllib,  datetime, time,os, numpy, pandas as pd, csv, h5py, pyodbc, datetime, arcpy
from netCDF4 import Dataset
from ftplib import FTP
from datetime import time, timedelta, date
import schedule
import time
global geoid
from arcpy import env
from arcpy.sa import *
geoid = 0
arcpy.env.overwriteOutput = True

def declaring_variables():
    #declaring all used date variables
    global yesterday
    global yyyymmdd
    global yyyymmdd_slash
    global year

    yesterday = date.today() - timedelta(days = 3)
    yyyymmdd = datetime.datetime.strptime(str(yesterday), "%Y-%m-%d").strftime("%Y%m%d")
    yyyymmdd_slash = datetime.datetime.strptime(str(yesterday), "%Y-%m-%d").strftime("%Y/%m/%d")

    global year
    year = yesterday.year

    global month
    month = yesterday.month

    #variables for downloading and working dir
    global directory
    directory = "C:/geospatial/database/temp/"

    global lista
    lista = [ r"C:/geospatial/gis/aoi/Wheat_Counties.shp",r"C:/geospatial/gis/aoi/Soybeans_Counties.shp", r"C:/geospatial/gis/aoi/Corn_Counties.shp", r"C:/geospatial/gis/aoi/Cotton_Counties.shp"]

    global down_file
    down_file = "Flood_byStor_" + yyyymmdd + "00.bin"
    
    global df4
    df4 = pd.DataFrame()
    
    global crop_list
    crop_list = ('soybeans','corn','wheat','cotton')
    
    global var_list
    var_list = []    
    
def downloading():
    #downloading the file
    urllib.urlretrieve('http://eagle2.umd.edu/flood/download/' + str(year) + "/" + str(year) + "0" + str(month) + "/" + down_file, directory + yyyymmdd + ".bin")

def processing():
    #creating lists and dicts to loop in variables
    df_list = list()

    #creating list for empy dataframes
    for n in crop_list: df_list.append("df_concat_" + str(n))
    for i in range(0,4): exec '%s=%s'%(str(df_list[i]), 'pd.DataFrame()')
    crop_dict = {}

    #creating list of csv values
    for n in crop_list: crop_dict.update({"df_" + n + "_ref" : "C:/geospatial/gis/aoi/" + n + "_Georref.csv"})
    for k in crop_dict.keys(): globals()[k] = pd.read_csv("C:/geospatial/gis/aoi/" + k.split("_")[1] + "_Georref.csv", sep = ";")

    #creating  empty dfs for vars
    df_vars = pd.DataFrame()

    #loading the bin file
    x = numpy.fromfile(directory + yyyymmdd + ".bin", dtype = 'f')

    #creating the numpy array and applying the mask to remove invalid values
    nrows, ncols = 800, 2458
    k = x.reshape(nrows, ncols)
    mask = (k == -9999)
    ma = numpy.ma.masked_array(k, mask = mask, dtype = "float64")

    #load the dataframe
    df = pd.DataFrame(ma)
    #print df
    #df_to_numeric = df.apply(pd.to_numeric)

    #organizing the dataframe
    df_unstack = df.unstack()
    df_unstack2 = df_unstack.to_frame()
    df_unstack3 = pd.DataFrame(df_unstack2.to_records())
    df_unstack_num = df_unstack3
    df_unstack_num.columns = ['long','lat','value']

    #reassign lat long values
    df_unstack_num['lat'] = df_unstack_num['lat'] * 0.125
    df_unstack_num['lat'] = df_unstack_num['lat'] - 50
    df_unstack_num['lat'] = df_unstack_num['lat'] * (-1)
    df_unstack_num['long'] = df_unstack_num['long'] * 0.125
    df_unstack_num['long'] = df_unstack_num['long'] - 127.25
    df_unstack_num.fillna(0, inplace = True)

    #cleaning and clipping the dataframe        
    df = df_unstack_num.loc[df_unstack_num['value'] != 0 ]
    df = df[df.lat > 29] 
    df = df[df.long < -80.9]
    df = df[df.long > -112]
    df.to_csv(directory + yyyymmdd + "_fixed.csv")
    
    #geoprocessing    
    arcpy.arcpy.MakeXYEventLayer_management(directory + yyyymmdd + "_fixed.csv", "long", "lat" , yyyymmdd + "_feat", "", "value")
    arcpy.SaveToLayerFile_management(yyyymmdd + "_feat", directory + "shape/" + yyyymmdd + "feat.lyr")
    resolution = 1
    arcpy.PointToRaster_conversion(directory + "shape/" + yyyymmdd + "feat.lyr", "value", directory + "raster/" + yyyymmdd , "MEAN","", resolution )
    
    #generating one file per variable
    for f in lista:
        crop = f.split("/")[4].split("_")[0]
        
        #gettin the values
        ExtractValuesToPoints(f, directory + "raster/" + yyyymmdd , directory + "shape/" + yyyymmdd + "_" + crop + "_points.shp")
        
        arcpy.TableToTable_conversion(directory + "shape/" + yyyymmdd + "_" + crop + "_points.dbf", directory + "csv/" , yyyymmdd + "flood" + crop + ".csv")
        
        csv = directory + "csv/" + yyyymmdd + "flood" + crop + ".csv"
        df2 = pd.read_csv(csv, sep = ",")

        #Cleaning the dataframe
        #df2['RASTERVALU'] = df2['RASTERVALU'].str.replace(',', '.').astype(np.float64)
        df2 = df2.loc[df2['RASTERVALU'] != -9999]
        df2.drop(['Join_Count'],axis = 1,inplace = True)
        df2.drop(['OBJECTID'],axis = 1,inplace = True)
        df2.drop(['OBJECTID_1'],axis = 1,inplace = True)
        df2.drop(['OID'],axis = 1,inplace = True)
        df2.drop(['TARGET_FID'],axis = 1,inplace = True)
        df2.drop(['lat'],axis = 1,inplace = True)
        df2.drop(['long_'],axis = 1,inplace = True)

        
        df2["STATEFP"] = df2["STATEFP"].astype(str)
        df_avg = df2.groupby(['STATEFP','NAME'], as_index = False)['RASTERVALU'].mean()
        df_avg['s2'] = df_avg['STATEFP'].str.replace('56','Wyoming').str.replace('12','Florida').str.replace('13','Georgia').str.replace('17','Illinois').str.replace('18','Indiana').str.replace('19','Iowa').str.replace('20','Kansas').str.replace('21','Kentucky').str.replace('22','Louisiana').str.replace('23','Maine').str.replace('24','Maryland').str.replace('25','Massachusetts').str.replace('26','Michigan').str.replace('27','Minnesota').str.replace('28','Mississippi').str.replace('29','Missouri').str.replace('30','Montana').str.replace('31','Nebraska').str.replace('32','Nevada').str.replace('33','New Hampshire').str.replace('34','New Jersey').str.replace('35','New Mexico').str.replace('36','New York').str.replace('37','North Carolina').str.replace('38','North Dakota').str.replace('39','Ohio').str.replace('40','Oklahoma').str.replace('41','Oregon').str.replace('42','Pennsylvania').str.replace('44','Rhode Island').str.replace('45','South Carolina').str.replace('46','South Dakota').str.replace('47','Tennessee').str.replace('48','Texas').str.replace('49','Utah').str.replace('50','Vermont').str.replace('51','Virginia').str.replace('53','Washington').str.replace('54','West Virginia').str.replace('55','Wisconsin').str.replace('4','Arizona').str.replace('5','Arkansas').str.replace('6','California').str.replace('8','Colorado').str.replace('9','Connecticut').str.replace('1','Alabama').replace('16','Idaho')
        df_avg['date'] = yyyymmdd_slash
        df_avg.drop(['STATEFP'],axis = 1,inplace = True)
        df_avg.rename(columns = {'s2': 'State', 'NAME': 'County'}, inplace = True)

        #exporting the csv
        df_avg.to_csv(directory + "csv/" + yyyymmdd + crop + "_final.csv")

def publishing():
    global geoid
    concat_dict = {"1" : "df_concat_soybeans","2": 'df_concat_corn',"3": 'df_concat_wheat',"4": 'df_concat_cotton'}
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=Geospatial;Trusted_Connection=Yes')
    cursor = conn.cursor()
    crop = 0
    
    for f in crop_list:
        try:
            dataframe = pd.read_csv(directory + "csv/" + yyyymmdd + f + "_final.csv")
            shape = dataframe.shape[0]
            index = 0
            while index < shape:
                try:
                    date = dataframe.iloc[index]['date']
                    state = dataframe.iloc[index]['State']
                    county = dataframe.iloc[index]['County']
                    value = dataframe.iloc[index]['RASTERVALU']
                    variable = "Flood"
                    country = "US"
                    geoid = str(state) + "_" + str(county)
                    index += 1
                    cursor.execute("INSERT INTO dbo.allvariables(geoid, country, state, county, value, variable, crop, date) VALUES (?,?,?,?,?,?,?,?)", geoid, country, state,county, value, variable, f, date)
                    conn.commit()
                except:
                    print "Line with values " + geoid, state, county, value, variable, crop, date + " could not published!"
                    index += 1
                    continue
            index = 0
        except:
            print "File for crop " + f + "not worked"          
        
def main():
    print "Starting the update of Flood data at " + str(datetime.datetime.now())
    declaring_variables()
    downloading()
    processing()
    publishing()
    print "Updated latest Flood data at " + str(datetime.datetime.now())

#schedule.every().day.at("20:21").do(main)
main()
#while 1:
#    schedule.run_pending()
#    time.sleep(1)
