import shutil
import os
from datetime import datetime, timedelta

class Clean:
    
    def __init__(self, basin_data_dir):
        self.basin_data_dir = basin_data_dir
        pass

    def clean_pre_processing(self):
        try:
            pre_processing_path = os.path.join(self.basin_data_dir,'pre_processing','')
            shutil.rmtree(pre_processing_path)
        except:
            print("No pre_processing folder to delete")

    def clean_metsim(self):
        try:
            metsim_path = os.path.join(self.basin_data_dir,'metsim','metsim_outputs','')
            shutil.rmtree(metsim_path)
        except:
            print("No metsim_outputs folder to delete")
    
    def clean_vic(self):
        try:
            vic_inputs_path = os.path.join(self.basin_data_dir,'vic','vic_inputs','')
            shutil.rmtree(vic_inputs_path)
        except:
            print("No vic_inputs folder to delete")
        
        try:
            vic_outputs_path = os.path.join(self.basin_data_dir,'vic','vic_outputs','')
            shutil.rmtree(vic_outputs_path)
        except:
            print("No vic_outputs folder to delete")
        
        try:
            vic_init_states_dir_path = os.path.join(self.basin_data_dir,'vic','vic_init_states','') 
            days_old = 15 #n max of days

            time_interval = datetime.now() - timedelta(days_old)
            file_namelist = os.listdir(vic_init_states_dir_path)
            
            # deleting init_state_file path if it is older than days_old 
            for file in file_namelist:
                path = os.path.join(vic_init_states_dir_path, file)
                filetime = datetime.fromtimestamp(os.path.getctime(path))
                if filetime < time_interval:
                    os.remove(path)

        except:
            print("No vic init_state file to delete")
    
    def clean_routing(self):
        try:
            rout_inputs_path = os.path.join(self.basin_data_dir,'ro','in','')
            shutil.rmtree(rout_inputs_path)
        except:
            print("No rout_inputs folder to delete")
        
        try:
            rout_outputs_path = os.path.join(self.basin_data_dir,'ro','ou','')
            shutil.rmtree(rout_outputs_path)
        except:
            print("No rout_outputs folder to delete")

        try:
            rout_init_states_dir_path = os.path.join(self.basin_data_dir,'ro','rout_state_file','') 
            days_old = 15 #n max of days

            time_interval = datetime.now() - timedelta(days_old)
            file_namelist = os.listdir(rout_init_states_dir_path)
            
            # deleting init_state_file path if it is older than days_old 
            for file in file_namelist:
                path = os.path.join(rout_init_states_dir_path, file)
                filetime = datetime.fromtimestamp(os.path.getctime(path))
                if filetime < time_interval:
                    os.remove(path)

        except:
            print("No rout init_state file to delete")

    def clean_gee(self):
        try:
            l8_scratch_path = os.path.join(self.basin_data_dir,'gee','gee_sarea_tmsos','l8','_scratch')
            shutil.rmtree(l8_scratch_path)
        except:
            print("No _scratch folder to delete for landsat-8 based reserevoir area extraction")

        try:
            s2_scratch_path = os.path.join(self.basin_data_dir,'gee','gee_sarea_tmsos','s2','_scratch')
            shutil.rmtree(s2_scratch_path)
        except:
            print("No _scratch folder to delete for sentinel-2 based reserevoir area extraction")

    def clean_altimetry(self):
        try:
            raw_altimetry_path = os.path.join(self.basin_data_dir,'altimetry','raw',)
            shutil.rmtree(raw_altimetry_path)
        except:
            print("No raw folder to delete in altimetry")

    def clean_previous_outputs(self):
        try:
            rat_outputs_path = os.path.join(self.basin_data_dir,'rat_outputs')
            shutil.rmtree(rat_outputs_path)
        except:
            print("No previous rat_outputs folder to delete")

        try:
            gee_sarea_path = os.path.join(self.basin_data_dir,'gee','gee_sarea_tmsos')
            shutil.rmtree(gee_sarea_path)
        except:
            print("No previous gee extracted surface area data folder to delete")
        
        try:
            altimetry_timeseries_path = os.path.join(self.basin_data_dir,'altimetry','altimetry_timeseries')
            shutil.rmtree(altimetry_timeseries_path)
        except:
            print("No previous altimetry_timeseries folder to delete")
        
        try:
            altimetry_extracted_path = os.path.join(self.basin_data_dir,'altimetry','extracted')
            shutil.rmtree(altimetry_extracted_path)
        except:
            print("No previous altimetry_extracted folder to delete")
        
        try:
            final_outputs_path = os.path.join(self.basin_data_dir,'final_outputs')
            shutil.rmtree(final_outputs_path)
        except:
            print("No final_outputs folder to delete with previous outputs")
