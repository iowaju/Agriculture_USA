import datetime, time as t, os, pandas as pd, csv, pyodbc, schedule, arcpy
from ftplib import FTP
from datetime import time, timedelta, date
from arcpy import env
from arcpy.sa import *
arcpy.env.overwriteOutput = True

def declaring_variables():
    #declaring all used date variables
    global yesterday
    global yyyywww
    global yyyymmdd
    global yyyymmdd_slash
    global year

    yesterday = date.today() - timedelta(days = 8)
    yyyymmdd = datetime.datetime.strptime(str(yesterday), "%Y-%m-%d").strftime("%Y%m%d")
    yyyymmdd_slash = datetime.datetime.strptime(str(yesterday), "%Y-%m-%d").strftime("%Y/%m/%d")
    yyyywww = datetime.datetime.strptime(str(yesterday), "%Y-%m-%d").strftime("%Y0%W")
    year = yesterday.year

    #variables for downloading and working dir
    global directory
    directory = "C:/geospatial/database/temp/"
    
    global df4
    df4 = pd.DataFrame()
    
    global crop_list
    crop_list = ('soybeans','corn','wheat','cotton')

    global file_list
    file_list = []

    global csv_list
    csv_list = []
    
    global var_list
    var_list = ['VHI', 'SMN', 'SMT']

    global lista
    lista = [ r"C:/geospatial/gis/aoi/Wheat_Counties.shp", r"C:/geospatial/gis/aoi/Soybeans_Counties.shp", r"C:/geospatial/gis/aoi/Corn_Counties.shp", r"C:/geospatial/gis/aoi/Cotton_Counties.shp"]
    
def downloading():
    #defining the ftp configurations
    ftp = FTP("ftp.star.nesdis.noaa.gov") #adress
    ftp.login() # loggin in' 
    ftpdir = "/pub/corp/scsb/wguo/data/Blended_VH_4km/geo_TIFF/" #directory containing the files
    ftp.cwd(ftpdir) #navigating to the folder of interest

    #downloading
    for variable in var_list:
        down_file = "VHP.G04.C07.npp.P" + yyyywww + "." + variable[:2] + "." + variable + ".tif"
        file_list.append(str(directory) + yyyymmdd + "_" + variable + ".tif")
        with open(os.path.join(directory, yyyymmdd + "_" + variable + ".tif"), 'wb') as local_file:
            ftp.retrbinary('RETR '+ down_file, local_file.write)
   
def processing():
    #creating  empty dfs for vars
    df_vars = pd.DataFrame()

    for shapefile in lista: #loop thru all crops
        for raster in file_list: # loop thru all variables
          crop = shapefile.split("/")[4].split("_")[0]
          variable = raster.split("/")[4].split("_")[1].split(".")[0]

          #extracting raster values to gridded points
          ExtractValuesToPoints(shapefile, raster, directory + "shape/" + variable + "a_" + yyyymmdd + "_points.shp")
          arcpy.TableToTable_conversion(directory + "shape/" + variable + "a_" + yyyymmdd + "_points.dbf", directory + "csv/", yyyymmdd + "_" + variable + "_" + crop + ".csv")

          #loading the csv with the data extracted
          df = pd.read_csv(directory + "csv/" + yyyymmdd + "_" + variable + "_" + crop + ".csv",sep = ",")
          df = df.drop(df[df.RASTERVALU < 0].index)

          #dropping useless columns:
          df.drop(['OID'],axis = 1,inplace = True)
          df.drop(['TARGET_FID'],axis = 1,inplace = True)
          df.drop(['OBJECTID_1'],axis = 1,inplace = True)
          df.drop(['OBJECTID'],axis = 1,inplace = True)
          df.drop(['Join_Count'],axis = 1,inplace = True)
          df.drop(['lat'],axis = 1,inplace = True)
          df.drop(['long_'],axis = 1,inplace = True)
          
          df["STATEFP"] = df["STATEFP"].astype(str)
          
          #reshaping df
          df = df.groupby(['STATEFP','NAME'], as_index = False)['RASTERVALU'].mean()
          df['State'] = df['STATEFP'].str.replace('56','Wyoming').str.replace('12','Florida').str.replace('13','Georgia').str.replace('16','Idaho').str.replace('17','Illinois').str.replace('18','Indiana').str.replace('19','Iowa').str.replace('20','Kansas').str.replace('21','Kentucky').str.replace('22','Louisiana').str.replace('23','Maine').str.replace('24','Maryland').str.replace('25','Massachusetts').str.replace('26','Michigan').str.replace('27','Minnesota').str.replace('28','Mississippi').str.replace('29','Missouri').str.replace('30','Montana').str.replace('31','Nebraska').str.replace('32','Nevada').str.replace('33','New Hampshire').str.replace('34','New Jersey').str.replace('35','New Mexico').str.replace('36','New York').str.replace('37','North Carolina').str.replace('38','North Dakota').str.replace('39','Ohio').str.replace('40','Oklahoma').str.replace('41','Oregon').str.replace('42','Pennsylvania').str.replace('44','Rhode Island').str.replace('45','South Carolina').str.replace('46','South Dakota').str.replace('47','Tennessee').str.replace('48','Texas').str.replace('49','Utah').str.replace('50','Vermont').str.replace('51','Virginia').str.replace('53','Washington').str.replace('54','West Virginia').str.replace('55','Wisconsin').str.replace('4','Arizona').str.replace('5','Arkansas').str.replace('6','California').str.replace('8','Colorado').str.replace('9','Connecticut').str.replace('1','Alabama').replace('16','Idaho')
          df.drop(['STATEFP'],axis = 1,inplace = True)
          df.rename(columns = {'NAME': 'County'}, inplace = True)          
          df['Date'] = yyyymmdd_slash

          #exporting the dataframe adjusted
          df.to_csv(directory + "csv/" + yyyymmdd + "_" + variable + "_" + crop + "_cleaned.csv", index = False)
          csv_list.append(directory + "csv/" + yyyymmdd + "_" + variable + "_" + crop + "_cleaned.csv")

def publishing():
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=Geospatial;Trusted_Connection=Yes')
    
    cursor = conn.cursor()
    for variable in var_list:
        for shapefile in lista: #loop thru all crops
            crop = shapefile.split("/")[4].split("_")[0]      
            dataframe = pd.read_csv(directory + "csv/" + yyyymmdd + "_" + variable + "_" + crop + "_cleaned.csv")
            shape = dataframe.shape[0]
            index = 0
            while index < shape:
                date = dataframe.iloc[index]['Date']
                state = dataframe.iloc[index]['State']
                county = dataframe.iloc[index]['County'].decode('utf8')
                value = dataframe.iloc[index]['RASTERVALU']
                geoid = str(state) + "_" + county
                country = "US"
                index += 1
                #print geoid, state, county, value, variable, crop, date
                cursor.execute("INSERT INTO dbo.allvariables(geoid, country, state, county, value, variable, crop, date) VALUES (?,?,?,?,?,?,?,?)", geoid, country, state, county, value, variable, crop, date)
                conn.commit()   
            index = 0
        
def main():

      print "Starting the update of VHI, NDVI and Brith Temp data at " + str(datetime.datetime.now())
      declaring_variables()
      downloading()
      processing()
      publishing()
      print "Updated latest VHI, NDVI and Brith Temp data at " + str(datetime.datetime.now())

schedule.every().saturday.at("00:15").do(main)
main()
#while 1:
#    schedule.run_pending()
#    t.sleep(1)
