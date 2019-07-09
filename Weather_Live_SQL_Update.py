import urllib
import time
import os
import numpy as np
import pandas as pd
import csv
import h5py
import pyodbc
from netCDF4 import Dataset
from ftplib import FTP
from datetime import time, timedelta, date
import datetime
import schedule

global geoid
geoid = 0

def declaring_variables():
    # declaring all used date variables
    global yesterday
    global yyyymmdd
    global yyyymmdd_slash
    global year

    yesterday = date.today() - timedelta(days=3)
    # print yesterday
    yyyymmdd = datetime.datetime.strptime(str(yesterday), "%Y-%m-%d").strftime("%Y%m%d")
    yyyymmdd_slash = datetime.datetime.strptime(str(yesterday), "%Y-%m-%d").strftime("%Y/%m/%d")
    year = yesterday.year

    # variables for downloading and working dir
    global directory
    directory = "C:/geospatial/database/temp/"

    global down_file
    down_file = "early_gridmet_" + yyyymmdd + '.nc'

    global df4
    df4 = pd.DataFrame()

    global crop_list
    crop_list = ('soybeans', 'corn', 'wheat', 'cotton')  # type: Tuple[str, str, str, str]

    global var_list
    var_list = []


def downloading():
    # downloading the file
    urllib.urlretrieve('https://www.northwestknowledge.net/metdata/data/early/' + str(year) + '/' + down_file,
                       directory + down_file[14:])


def processing():
    # creating lists and dicts to loop in variables
    df_list = list()

    # creating list for empy dataframes
    for n in crop_list:
        df_list.append("df_concat_" + str(n))
    for i in range(0, 4):
        exec '%s=%s' % (str(df_list[i]), 'pd.DataFrame()')
    crop_dict = {}

    # creating list of csv values
    for n in crop_list:
        crop_dict.update({"df_" + n + "_ref": "C:/geospatial/gis/aoi/" + n + "_Georref.csv"})
    for k in crop_dict.keys():
        globals()[k] = pd.read_csv("C:/geospatial/gis/aoi/" + k.split("_")[1] + "_Georref.csv",
                                   sep=";")

    # creating  empty dfs for vars
    df_vars = pd.DataFrame()

    # loading the NC file
    dataset = Dataset(directory + down_file[14:])

    # filling a list with variables available
    for i in dataset.variables:
        var_list.append(str(i))
    del var_list[0:4]
    i = 0

    # creating an intial DF to serve as reference
    a = dataset.variables['precipitation_amount'][:]
    m, n = a.shape
    r, c = np.mgrid[:m, :n]
    out = np.column_stack((r.ravel()[:], c.ravel()[:], a.ravel()[:]))
    df_latlon = pd.DataFrame(out)
    df_latlon.columns = ['lat', 'long', 'precipitation_amount']
    df_latlon.drop(['precipitation_amount'], axis=1, inplace=True)

    # creating a dataframe with all variables
    for variable in var_list:
        # extracting the values from the NC file
        var = (variable)
        b = str(var)
        a = dataset.variables[b][:]
        m, n = a.shape
        R, C = np.mgrid[:m, :n]
        out = np.column_stack((R.ravel()[:], C.ravel()[:], a.ravel()[:]))

        # creating the dataframe
        df = pd.DataFrame(out)
        df.columns = ['lat', 'long', b]
        df_cut = df[b]
        df_cut.columns = [b]
        df_vars = pd.concat([df_vars, df_cut], axis=1)

        # cleaning the DF with all variables on it
    df_variables = pd.concat([df_latlon, df_vars], axis=1)
    df_variables['index1'] = df_variables.index

    # removing useless areas
    df2 = df_variables.loc[df_variables['long'] > 300]
    df3 = df2.loc[df2['long'] < 1050]

    # reshaping the x,y values
    df3['lat'] = df3['lat'] * 0.0416666666
    df3['long'] = df3['long'] * 0.0416666666
    df3['lat'] = df3['lat'] * (-1)
    df3['lat'] = df3['lat'] + 49.40000000000000
    df3['long'] = df3['long'] - 124.7666666333333

    # removing useless latitudes
    df4 = df3.loc[df3['lat'] > 29.5]

    # inserting a new column with date
    df4['date'] = yyyymmdd_slash
    global df4


def cleaning():
    crop_dict = {}
    df_concat = pd.DataFrame()

    for n in crop_list:
        crop_dict.update({"df_" + n + "_ref": "C:/geospatial/gis/aoi/" + n + "_Georref.csv"})
    for k in crop_dict.keys():
        globals()[k] = pd.read_csv("C:/geospatial/gis/aoi/" + k.split("_")[1] + "_Georref.csv",
                                   sep=";")
    column_list = ['GeoId', 'lat_caller', 'long_caller', 'lat_other', 'long_other']

    for dataframe in crop_dict.values():
        crop = dataframe.split("/")[4].split("_")[0]
        df = pd.read_csv(dataframe, sep=";")
        df_joined = df.join(df4.set_index('index1'), on='GeoId', lsuffix='_caller', rsuffix='_other')
        df_avg = df_joined.groupby("County").mean()
        df_avg['date'] = yyyymmdd_slash

        # removing useless columns
        for column in column_list:
            df_avg.drop([column], axis=1, inplace=True)

        # final dataframe organized
        df_concat = pd.concat([df_concat, df_avg])
        df_concat['test'] = df_concat.index
        df_concat[['State', 'County']] = df_concat['test'].str.split('_', expand=True)
        df_concat['State'] = df_concat['State'].replace('01', 'Alabama') \
            .replace('04', 'Arizona') \
            .replace('05', 'Arkansas') \
            .replace('06', 'California') \
            .replace('08', 'Colorado') \
            .replace('09', 'Connecticut') \
            .replace('12', 'Florida') \
            .replace('13', 'Georgia') \
            .replace('17', 'Illinois') \
            .replace('18', 'Indiana') \
            .replace('19', 'Iowa') \
            .replace('20', 'Kansas') \
            .replace('21', 'Kentucky') \
            .replace('22', 'Louisiana') \
            .replace('23', 'Maine') \
            .replace('24', 'Maryland') \
            .replace('25', 'Massachusetts') \
            .replace('26', 'Michigan') \
            .replace('27', 'Minnesota') \
            .replace('28', 'Mississippi') \
            .replace('29', 'Missouri') \
            .replace('30', 'Montana') \
            .replace('31', 'Nebraska') \
            .replace('32', 'Nevada') \
            .replace('33', 'New Hampshire') \
            .replace('34', 'New Jersey') \
            .replace('35', 'New Mexico') \
            .replace('36', 'New York') \
            .replace('37', 'North Carolina') \
            .replace('38', 'North Dakota') \
            .replace('39', 'Ohio') \
            .replace('40', 'Oklahoma') \
            .replace('41', 'Oregon') \
            .replace('42', 'Pennsylvania') \
            .replace('44', 'Rhode Island') \
            .replace('45', 'South Carolina') \
            .replace('46', 'South Dakota') \
            .replace('47', 'Tennessee') \
            .replace('48', 'Texas') \
            .replace('49', 'Utah') \
            .replace('50', 'Vermont') \
            .replace('51', 'Virginia') \
            .replace('53', 'Washington') \
            .replace('54', 'West Virginia') \
            .replace('55', 'Wisconsin') \
            .replace('56', 'Wyoming') \
            .replace('16', 'Idaho')
        df_concat.drop('test', axis=1, inplace=True)
        df_concat.to_csv(directory + "csv/" + yyyymmdd + "_" + "weather" + "_" + crop + ".csv", index=False)

        # cleaning_dfs
        df_concat = pd.DataFrame()
        df_avg = pd.DataFrame()
        df_joined = pd.DataFrame()


def publishing():
    global geoid
    concat_dict = {"1": "df_concat_soybeans", "2": 'df_concat_corn', "3": 'df_concat_wheat', "4": 'df_concat_cotton'}
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=Geospatial;Trusted_Connection=Yes')

    cursor = conn.cursor()

    for dataframe in concat_dict.values():
        try:
            # loop trhu the dataframes
            crop = dataframe.split("_")[2]
            dataframe = pd.read_csv(directory + "csv/" + yyyymmdd + "_" + "weather" + "_" + crop + ".csv")
            shape = dataframe.shape[0]
            index = 0

            # looping in each variable
            while index < shape:
                for n in var_list[:17]:
                    try:
                        date = dataframe.iloc[index]['date']
                        state = dataframe.iloc[index]['State']
                        county = dataframe.iloc[index]['County']
                        value = dataframe.iloc[index][n]
                        country = "US"
                        geoid = str(state) + "_" + county
                        index += 1
                        # print geoid, country, state, county, value, n, crop, date
                        cursor.execute(
                            "INSERT INTO dbo.allvariables(geoid, country, state, county, value, variable, crop, date) VALUES (?,?,?,?,?,?,?,?)",
                            geoid, country, state, county, value, n, crop, date)

                        conn.commit()
                    except:
                        # print "Values *" + str(geoid), str(country), str(state), str(county), str(value), str(n), str(crop), str(date) + " could not be published!"
                        index = + 1
                        continue
            index = 0
            continue
        except:
            # print "File not worked for crop " + dataframe + "!"
            continue


def main():
    print "Starting the update of Weather data at " + str(datetime.datetime.now())
    declaring_variables()
    downloading()
    processing()
    cleaning()
    publishing()
    print "Updated latest Weather data at " + str(datetime.datetime.now())


# schedule.every().day.at("21:10").do(main)
main()
# while 1:
#    schedule.run_pending()
#    time.sleep(1)
