import urllib,  datetime, time as t, os, numpy as np, pandas as pd, csv, h5py, pyodbc, datetime, schedule, arcpy
from netCDF4 import Dataset
from ftplib import FTP
from datetime import time, timedelta, date
from bs4 import BeautifulSoup

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
    year = yesterday.year

    #variables for downloading and working dir
    global directory
    directory = "C:/geospatial/database/temp/"

    global down_file
    down_file = "early_gridmet_"+ yyyymmdd + '.nc'
    
    global df4
    df4 = pd.DataFrame()
    
    global crop_list
    crop_list = ('soybeans','corn','wheat','cotton')
    
    global var_list
    var_list = []

    global lista
    lista = [ r"C:/geospatial/gis/aoi/Wheat_Counties.shp", r"C:/geospatial/gis/aoi/Soybeans_Counties.shp", r"C:/geospatial/gis/aoi/Corn_Counties.shp", r"C:/geospatial/gis/aoi/Cotton_Counties.shp"]
    
def downloading():
    page = urllib.urlopen('https://www.ncei.noaa.gov/data/avhrr-land-leaf-area-index-and-fapar/access/' + str(year) + '/')
    soup = BeautifulSoup(page, 'html.parser')
    for a in soup.find_all('a', href = True):
            b = a['href']
            if b[44:52] == yyyymmdd:
                urllib.urlretrieve('https://www.ncei.noaa.gov/data/avhrr-land-leaf-area-index-and-fapar/access/' + str(year) + '/' + str(b), directory + yyyymmdd + "_fapar.nc")
    
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

    #loading the NC file
    
    dataset = Dataset(directory + yyyymmdd + "_fapar.nc")
    
    for v in dataset.variables: var_list.append(str(v))
    i = 0

    #creating a dataframe with all variables
    for variable in var_list:
        if variable == "FAPAR" or variable == "LAI":
            varb = (variable)
            b = str(varb)
            array_data = dataset.variables[b][:][:][:]
            array_data_clean = array_data[:,:1800,:3600]

            a, m, n = array_data_clean.shape
            R, C = np.mgrid[:m,:n]
            out = np.column_stack((R.ravel()[:], C.ravel()[:], array_data_clean.ravel()[:]))
            df = pd.DataFrame(out)    
            
            df.columns = ['lat', 'long', str(variable)]
            df = df.loc[df[str(variable)] != -100.000]
            df = df.loc[df[str(variable)] > 0]

            #adjusting lat long
            df['lat'] = df['lat'] * 0.05
            df['long'] = df['long'] * 0.05
            df['lat'] = df['lat'] - 90  
            df['lat'] = df['lat'] * (-1) 
            df['long'] = df['long'] - 180

            #Cleaning the file
            df = df.drop(df[df.lat < 24].index) #North
            df = df.drop(df[df.lat > 49].index) #South
            df = df.drop(df[df.long < -111].index) #East
            df = df.drop(df[df.long > -75.2].index) #West

            #exporting the csv
            df.to_csv(directory + "csv/" + yyyymmdd + "_" + variable + ".csv",index = False)
            df = pd.DataFrame()

            #cleaning the arrays
            array_data = []
            array_data_clean = []

def cleaning():
    for var in var_list:
        if var == "FAPAR" or var == "LAI":
            csv = directory + "csv/" + yyyymmdd + "_" + var + ".csv"
            
            #defining the resolution of the raster       
            arcpy.arcpy.MakeXYEventLayer_management(csv, "long", "lat" , yyyymmdd + var + "_feat", "", var)
            arcpy.SaveToLayerFile_management(yyyymmdd + var + "_feat", directory + "shape/" + yyyymmdd + var + "feat.lyr")
            arcpy.FeatureClassToShapefile_conversion( directory + "shape/" + yyyymmdd + var + "feat.lyr", directory + "shape/")
            shape = directory + "shape/" + yyyymmdd + var + "_feat.shp"
                                             
            for n in lista:
                     #creating crop variable
                     crop =  n.split("/")[4].split("_")[0]
                     outfc = directory + "shape/" + yyyymmdd + "_" + var + "_" + crop + "_feat.shp"

                     #create the field map
                     myMap = arcpy.FieldMappings()
                     myMap.addTable(n)
                     myMap.addTable(shape)
                     fIndex = myMap.findFieldMapIndex(var)

                     #create new field map with the "GRID_CODE" field
                     NewFieldMap = myMap.getFieldMap(fIndex)

                     #set the merge rule
                     NewFieldMap.mergeRule = 'Mean'
                     myMap.replaceFieldMap(fIndex,NewFieldMap)
                     arcpy.SpatialJoin_analysis(n, shape, outfc, "JOIN_ONE_TO_ONE", "", myMap, "CONTAINS")
                     arcpy.TableToTable_conversion(directory + "shape/" + yyyymmdd + "_" + var + "_" + crop + "_feat.dbf", directory + "shape/", yyyymmdd + "_" + var + "_" + crop + "_feat.csv")

                     #loading the shapefile's dataset
                     csv2 = directory + "shape/" + yyyymmdd + "_" + var + "_" + crop + "_feat.csv"
                     df2 = pd.read_csv(csv2, sep = ",")

                     #Removing invalid columns
                     df2.drop(['lat'],axis = 1, inplace = True)
                     df2.drop(['long'],axis = 1, inplace = True)
                     df2.drop(['long_'],axis = 1, inplace = True)
                     df2.drop(['OBJECTID'],axis = 1, inplace = True)
                     df2.drop(['TARGET_F_1'],axis = 1, inplace = True)
                     df2.drop(['Join_Cou_1'],axis = 1, inplace = True)
                     df2.drop(['OBJECTID_1'],axis = 1, inplace = True)                   
                     df2.drop(['OID'],axis = 1, inplace = True)
                     df2.drop(['Join_Count'],axis = 1, inplace = True)
                     df2.drop(['TARGET_FID'],axis = 1, inplace = True)
                     df2["STATEFP"] = df2["STATEFP"].astype(str)

                     #removing columns
                     df2.rename(columns = {'NAME': 'County'}, inplace = True)
                     df2.rename(columns = {'STATEFP': 'State'}, inplace = True)
                     df2['State'] = df2['State'].replace('1','Alabama').replace('4','Arizona').replace('5','Arkansas').replace('6','California').replace('8','Colorado').replace('9','Connecticut').replace('12','Florida').replace('13','Georgia').replace('17','Illinois').replace('18','Indiana').replace('19','Iowa').replace('20','Kansas').replace('21','Kentucky').replace('22','Louisiana').replace('23','Maine').replace('24','Maryland').replace('25','Massachusetts').replace('26','Michigan').replace('27','Minnesota').replace('28','Mississippi').replace('29','Missouri').replace('30','Montana').replace('31','Nebraska').replace('32','Nevada').replace('33','New Hampshire').replace('34','New Jersey').replace('35','New Mexico').replace('36','New York').replace('37','North Carolina').replace('38','North Dakota').replace('39','Ohio').replace('40','Oklahoma').replace('41','Oregon').replace('42','Pennsylvania').replace('44','Rhode Island').replace('45','South Carolina').replace('46','South Dakota').replace('47','Tennessee').replace('48','Texas').replace('49','Utah').replace('50','Vermont').replace('51','Virginia').replace('53','Washington').replace('54','West Virginia').replace('55','Wisconsin').replace('56','Wyoming')                    
                     df2 = df2.loc[df2[str(var)] != 0]
                     df_avg = df2.groupby(['State','County'], as_index = False)[var].mean()
                     df_avg['Date'] = yyyymmdd_slash

                     if df_avg.shape[0] > 10:
                        df_avg.to_csv(directory + "csv/" + crop + "_" + yyyymmdd + "_" + var + ".csv", index = False)
                        df_avg = pd.DataFrame()
def publishing():
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=INSTANCE-1\SQLEXPRESS;DATABASE=Geospatial;Trusted_Connection=Yes')
    cursor = conn.cursor()
    crop = 0
    
    for var in var_list:
            if var == "FAPAR" or var == "LAI":
                    for crop in crop_list:
                        try:
                            dataframe = pd.read_csv(directory + "csv/" + crop + "_" + yyyymmdd + "_" + var + ".csv")
                            shape = dataframe.shape[0]
                            index = 0
                            while index < shape:
                                date = dataframe.iloc[index]['Date']
                                state = dataframe.iloc[index]['State']
                                county = dataframe.iloc[index]['County']
                                value = dataframe.iloc[index][var]
                                geoid = str(state) + "_" + county
                                country = "US"
                                index += 1
                                #print geoid, country, state, county, value, var, crop, date
                                cursor.execute("INSERT INTO dbo.allvariables(geoid, country, state, county, value, variable, crop, date) VALUES (?,?,?,?,?,?,?,?)", geoid, country, state, county, value, var, crop, date)
                                conn.commit()   
                            index = 0
                        except:
                            print crop + " not worked!"
                            continue 
def main():
        print "Starting the update of FAPAR and LAI data at " + str(datetime.datetime.now())
        declaring_variables()
        downloading()
        processing()
        cleaning()
        publishing()
        #print "Updated latest FAPAR and LAI data at " + str(datetime.datetime.now())
        
schedule.every().day.at("21:20").do(main)
#main()
while 1:
    schedule.run_pending()
    t.sleep(1)
