import numpy as np
import os
from netCDF4 import num2date, date2num
from netCDF4 import Dataset
import pandas as pd
import json

config_file_location = './process_grids_config.json'

def find_nearest(array,target_values):
    idx = np.zeros_like(target_values).astype(int)
    for target in range(len(target_values)):
        idx[target] = (np.abs(array-target_values[target])).argmin()
    return idx

def read_grid(var_name,filename,lat_select,lon_select,time_select=None):
    #Open netCDF file connection
    nc = Dataset(filename, 'r')

    #Read in data
    lat_ref = nc.variables['latitude'][:]
    lon_ref = nc.variables['longitude'][:]
    time = nc.variables['time'][:]
    data = nc.variables[var_name][:]
    #print(np.shape(time))

    #Close connection to file
    nc.close()

    # If it doesn't exist yet, create the indexes
    lat_idx = find_nearest(lat_ref,lat_select)
    lon_idx = find_nearest(lon_ref,lon_select)

    #Check if we want a single timestamp or multiple
    if time_select==None:
        data_select=data[:,lat_idx,lon_idx]
    else:
        data_select=data[time_select,lat_idx,lon_idx]

    return data_select, time

def grid_to_dict(field,grids_dict,target_loc,ref_time):
    try:
        fn = '/'.join([grid_dir,[s for s in grids if field[0] in s][0]])
    except:
        print(('Error loading %s, check files exist and are correct in config json'  % field[0]))
    data, datenum = read_grid(field[0],
                     fn,
                     target_loc[:,1],
                     target_loc[:,2],
                     )
    if np.array_equal(ref_time,datenum):
        print(('Retrieving ' + field[1] + ' from grids.'))
        grids_dict[field[1]] = np.ravel(data)
    else:
        print(('Retrieving ' + field[1] + ' from grids.'))
        print((field[1] + ' is shorter than Temperature field. Filling forward...'))
        infill_time = pd.merge(pd.DataFrame(ref_time),pd.DataFrame(np.column_stack((datenum,datenum))),how='left').copy()
        if(pd.isnull(infill_time[1][0])):
            infill_time[1][0] = datenum[0]
            print(('Start times of Temperature and ' + field[1] +
                  ' do not match. Filling forward from first row of ' + field[1]))
        infill_time[1] = infill_time[1].fillna(method='ffill')
        infill_time[1] = infill_time[1].astype(np.int64)

        data_out = np.zeros([len(infill_time[1]),np.shape(data)[1]])
        for i in range(len(infill_time[1])):
            data_out[i,:] = data[np.where(datenum == infill_time[1][i]),:]
        grids_dict[field[1]] = np.ravel(data_out)

if __name__ == '__main__':

    # Load config
    config = json.load(open(config_file_location))

    grid_dir = config['filepaths']['grid_dir']
    target_locations_file = config['filepaths']['target_locations_file']
    output_file = config['filepaths']['output_file']

    #field_names is a list of lists: Each list
    field_names = config['fieldnames']

    timezone_offset = int(config['timezone_offset'])

    grids = os.listdir(grid_dir)

    target_locations = np.loadtxt(target_locations_file,delimiter=',')

    tz_s = timezone_offset*60*60

    #Search for "T_SFC" in grid directory
    try:
        fn = os.sep.join([grid_dir,[s for s in grids if "T_SFC" in s][0]])
    except Exception as e:
        print("Temperature by the name T_SFC not found in grids! Error:")
        print(e)
        print("Files pointed to include:")
        print(grids)

    #Read grid of T_SFC, sampling only the required lat/lon's as in Location.txt
    print('Retrieving Temperature from grids.')
    t_sfc, time_sfc = read_grid('T_SFC',fn,target_locations[:,1],target_locations[:,2])

    #Convert timestamps from the netCDF to strings via python datetime objects
    timestamps_formatted = [num2date(ts+tz_s,'seconds since 1970-01-01 00:00:00').strftime("%Y-%d-%m %H:%M") for ts in time_sfc]

    #Create an empty array of strings of the same size of the sampled T_SFC, where each cell is the right number of characters for the string timestamps
    times_out = np.chararray(np.shape(t_sfc),itemsize=len(timestamps_formatted[0]))
    geo_id = np.zeros(np.shape(t_sfc))



    #Loop through Location.txt geo_id's provided
    for i in range(len(t_sfc[:,0])):
        #Fill in geo_ids
        geo_id[i,:] = target_locations[:,0].astype(int)

    for i in range(len(t_sfc[0,:])):
        #Fill in timestamps
        times_out[:,i] = timestamps_formatted # timestamps

    grids_out_dictionary = {}

    grids_out_dictionary['Location'] = np.ravel(geo_id).astype(int)
    grids_out_dictionary['Date'] = np.ravel(times_out.astype(str))
    grids_out_dictionary['Temperature'] = np.ravel(t_sfc)

    for field, outname in field_names.items():
        names = [field,outname]
        grid_to_dict(names,grids_out_dictionary,target_locations,time_sfc)

    df = pd.DataFrame(data=grids_out_dictionary)

    #Convert Windspeed from kts to km/hr
    df['WindSpeed'] = df['WindSpeed']*1.852

    df = df[['Location','Date','Temperature','RH','WindDir','WindSpeed','DroughtFactor','Curing','CloudCover']]
    df = df.sort_values(by=['Location','Date'])

    print(('Saving output to ' + output_file))
    df.to_csv(output_file,float_format='%.1f',index=False)
