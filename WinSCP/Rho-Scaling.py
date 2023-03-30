import pandas as pd
import os
print("The WinSCP RHO Fixture Program is Runing.......")


extracted_file_path = "Data/Previous-Data/"
dir_list = os.listdir(extracted_file_path)

for file in dir_list:
    if(file.endswith(".csv")):

        df = pd.read_csv(extracted_file_path + file)
        df['rho'] = df['rho'].apply(lambda x: x/100)
        df.to_csv(extracted_file_path + file, index = False)
        
print("INFO: Program completed Successfully.")


