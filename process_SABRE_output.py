

import numpy as np
import os
import pandas as pd
import geopandas as gpd
import ConfigParser


# functions


def ConfigSectionMap(section):
    dict1 = {}
    options = Config.options(section)
    for option in options:
        try:
            dict1[option] = Config.get(section, option)
            if dict1[option] == -1:
                DebugPrint("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1


if __name__ == '__main__':

    Config = ConfigParser.ConfigParser()

    Config.read('sabre_out_config.ini')
    sabre_out_dir = ConfigSectionMap('directories')['sabre_out_dir']
    output_dir = ConfigSectionMap('directories')['output_dir']
    output_text = ConfigSectionMap('outputs')['output_text']
    output_shp = ConfigSectionMap('outputs')['output_shp']

    sabre_out_fls = os.listdir(sabre_out_dir)
    sabre_ITS_path = os.sep.join([sabre_out_dir,[s for s in sabre_out_fls if "_ITS.txt" in s][0]])
    sabre_ITS = pd.read_csv(sabre_ITS_path,sep=',')

    best_est_name = sabre_ITS.Trial_Name[sabre_ITS.Trial_Name.str.contains("BestEstimate")][0]
    print('Loading Best Estimate')
    SABRE_all_trails = gpd.read_file(os.sep.join([sabre_out_dir,'Results',best_est_name+'_grid.shp']))
    SABRE_all_trails['Trial_Name'] = best_est_name
    SABRE_all_trails['Time'] = pd.to_datetime(sabre_ITS.Ignition_Time[sabre_ITS.Trial_Name.str.contains("BestEstimate")][0])

    #Make a copy of dataframe where spotting exists
    spot = SABRE_all_trails.dropna(subset = ['HOUR_SPOT']).copy()
    spot['Grid_Type'] = 'Spot'
    spot['Time'] = spot['Time'] + [pd.Timedelta(hours=ts) for ts in spot['HOUR_SPOT']]

    #Trim trial
    SABRE_all_trails = SABRE_all_trails.dropna(subset = ['HOUR_BURNT'])
    SABRE_all_trails['Grid_Type'] = 'Fire'
    SABRE_all_trails['Time'] = SABRE_all_trails['Time'] + [pd.Timedelta(hours=ts) for ts in SABRE_all_trails['HOUR_BURNT']]

    #Recombine
    SABRE_all_trails = pd.concat([SABRE_all_trails,spot])

    del spot

    for trial_name in sabre_ITS.Trial_Name[sabre_ITS.Trial_Name.str.contains("Trial")]:
        SABRE_trial = gpd.read_file(os.sep.join([sabre_out_dir,'Results',trial_name+'_grid.shp']))
        SABRE_trial['Time'] = pd.to_datetime(sabre_ITS.Ignition_Time[sabre_ITS.Trial_Name == trial_name])

        #Make a copy of dataframe where spotting exists
        spot = SABRE_trial.dropna(subset = ['HOUR_SPOT']).copy()
        spot['Grid_Type'] = 'Spot'
        spot['Time'] = spot['Time'] + [pd.Timedelta(hours=ts) for ts in spot['HOUR_SPOT']]

        #Trim trial
        SABRE_trial = SABRE_trial.dropna(subset = ['HOUR_BURNT'])
        SABRE_trial['Grid_Type'] = 'Fire'
        SABRE_trial['Time'] = SABRE_trial['Time'] + [pd.Timedelta(hours=ts) for ts in SABRE_trial['HOUR_BURNT']]

        SABRE_trial = pd.concat([SABRE_trial,spot])
        SABRE_trial['Trial_Name'] = trial_name
        #print(SABRE_trial['Trial_Name'].shape)
        print('Joining '+ trial_name)
        #print(SABRE_trial.shape[0])
        del spot
        SABRE_all_trails = pd.concat([SABRE_all_trails,SABRE_trial])
        #print(SABRE_all_trails.shape[0])
        del SABRE_trial

    SABRE_all_trails['Time_obj'] = SABRE_all_trails['Time']
    SABRE_all_trails['Time'] = SABRE_all_trails['Time'].astype(str)

    if output_shp:

        print('Saving shapefile as ' + os.path.basename(sabre_out_dir) + 'all_trials.shp')
        if 'Time_obj' in SABRE_all_trails:
            trials_out = SABRE_all_trails.drop('Time_obj', axis=1)
        else:
            trials_out = SABRE_all_trails
        trials_out.to_file(os.sep.join([output_dir, os.path.basename(sabre_out_dir)]) + 'all_trials.shp',driver='ESRI Shapefile')

    if output_text:
        SABRE_all_trails['Centriods'] = SABRE_all_trails.centroid.to_crs({'init': 'epsg:4326'})
        df = SABRE_all_trails.drop('geometry', axis=1)  # df is a DataFrame, not GeoDataFrame after the drop
        df['Latitude'] = SABRE_all_trails['Centriods'].apply(lambda p: p.x)
        df['Longitude'] = SABRE_all_trails['Centriods'].apply(lambda p: p.x)
        if 'Time_obj' in df:
            df = df.drop('Time_obj', axis=1)
        df = df.drop('Centriods', axis=1)
        df = df.drop('WIND_MOD', axis=1)
        print('Saving csv as ' + os.path.basename(sabre_out_dir) + 'all_trials.txt')
        df.to_csv(os.sep.join([output_dir, os.path.basename(sabre_out_dir)]) + 'all_trials.txt',index=False) #,float_format='%.1f'
