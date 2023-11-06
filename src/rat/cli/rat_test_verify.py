from pathlib import Path
import pandas as pd
import traceback


class Verify_Tests:
    
    def __init__(self, true_dir, estimated_dir):
        self.true_dir = true_dir
        self.estimated_dir = estimated_dir

    def verify_test_results(self):
        print("############################## Test-1 (AEC) ##############################",end="\n")
        self._verify_test_results_for_var('aec','AEC')

        print("############################## Test-2 (∆S) ##############################",end="\n")
        self._verify_test_results_for_var('dels','∆S')

        print("############################## Test-3 (Evaporation) ##############################",end="\n")
        self._verify_test_results_for_var('evaporation','Evaporation')

        print("############################## Test-4 (Inflow) ##############################",end="\n")
        self._verify_test_results_for_var('inflow','Inflow')

        print("############################## Test-5 (Outflow) ##############################",end="\n")
        self._verify_test_results_for_var('outflow','Outflow')

        print("############################## Test-6 (Surface Area) ##############################",end="\n")
        self._verify_test_results_for_var('sarea_tmsos','Surface Area')

    # Comparing true and estimated files for a variable
    def _verify_test_results_for_var(self, var_to_observe, var_to_display):
        print("Matching "+var_to_display+" files ...")
        try:
            # Getting directory path for true and estimated directories
            var_true_dir = Path(self.true_dir, var_to_observe)
            var_estimated_dir = Path(self.estimated_dir, var_to_observe)
            # Getting filenames from true directory
            var_true_file_paths = list(var_true_dir.glob('**/*.csv'))
            var_file_names = [f.name for f in var_true_file_paths]
            # Comparing the true and estimated directory
            var_matched, var_unmatched, var_failed = self._round_and_compare_files(var_true_dir,var_estimated_dir,var_file_names,5)     
            # Displaying results
            self._display_results_for_var(var_to_display,var_file_names,var_matched,var_unmatched,var_failed)
        except Exception as error:
            print("Error in verification of "+var_to_display+" files.")
            print(error)

    def _round_and_compare_files(self,var_true_dir,var_estimated_dir,var_file_names,threshold):
        # Initialising variables to output
        matched_file_names=[]
        unmatched_file_names=[]
        incorrect_file_names=[]
        
        # For each file
        for file_name in  var_file_names:
            try:
                # Get the true and estimated path
                var_true_file_path = Path(var_true_dir,file_name)
                var_estimated_file_path = Path(var_estimated_dir,file_name)

                # check if file exists; if not, put the file_name in incorrect list
                if(not var_true_file_path.is_file()):
                    incorrect_file_names.append(file_name)
                    continue
                if(not var_estimated_file_path.is_file()):
                    incorrect_file_names.append(file_name)
                    continue
                
                # Get the difference between the files after reading pandas 
                true_df=pd.read_csv(var_true_file_path)
                estimated_df=pd.read_csv(var_estimated_file_path)

                diff_cols = []
                # Take only float columns
                true_df_float = true_df.select_dtypes(include=['floating'])
                estimated_df_float=estimated_df.select_dtypes(include=['floating'])
                
                # % Difference between any value of each column
                for col_no in range(0,len(true_df_float.columns)):
                    diff_col =(estimated_df_float.iloc[:,col_no]-true_df_float.iloc[:,col_no])*100/true_df_float.iloc[:,0]
                    diff_cols.append(diff_col)

                # Checking difference against the threshold
                for col_no in range(0,len(true_df_float.columns)):
                    condn_to_pass = (len(diff_cols[col_no][diff_cols[col_no]>threshold])==0)
                    if(condn_to_pass):
                        status = 'match'
                    else:
                        status = 'unmatch'
                        break
                                    
                if(status == 'match'):
                    matched_file_names.append(file_name)
                elif(status == 'unmatch'):
                    unmatched_file_names.append(file_name)
            except Exception as e:
                incorrect_file_names.append(file_name)
                # print(traceback.format_exc())
                # print(e)
                
        return(matched_file_names,unmatched_file_names,incorrect_file_names)

    # Display matching results for a variable given comparison of true and estimated files
    def _display_results_for_var(self, var, files, correct, incorrect, failed):
        pass_percent = (len(correct)/len(files))*100
        if(pass_percent==100):
            print("Tested "+ var +" files successfully. All files matched. Test Passed-100%.")
        else:
            print("Testing of "+ var +" files done. Test Passed-"+str(pass_percent)+"%.")
            if(len(correct)!=0):
                print("File(s) that were matched: ", ', '.join(correct))
            if(len(incorrect)!=0):
                print("File(s) that were not matched: ", ', '.join(incorrect))
            if(len(failed)!=0):
                print("File(s) that could not be verified: ", ', '.join(failed))