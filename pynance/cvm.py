#!/usr/bin/python3

import requests
import zipfile
import os
import pandas as pd
import numpy as np
from io import BytesIO

def download_cia_aberta_ITR(years, folder = "./"):
    """
    This function downloads ITR historical for the range of years inserted.
    The individual files are saved in the folder provided as variable.
    
    Args:
        years: range of years to download the information
        folder: a path to the desired folder in which the files will be downloaded into. If folder doesn't exist, will be created
    """

    base_url = "http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/ITR_CIA_ABERTA_"

    for i in years:
        request_itr = requests.get(base_url + str(i) + ".zip")
        zip_itr = zipfile.ZipFile(BytesIO(request_itr.content))
        for j in zip_itr.namelist():
            zip_itr.extract(j, folder)
            
            new_file_name = j.lower()
            os.rename(folder + j, folder + new_file_name)

def download_cia_aberta_DFP(years, folder = "./"):
    """
    This function downloads DFP historical for the range of years inserted.
    The individuala files are saved in the folder provided as variable.
    
    Args:
        years: range of years to download the information
        folder: a path to the desired folder in which the files will be downloaded into.  If folder doesn't exist, will be created
    """

    tipos = ["BPA","BPP","DFC_MD","DFC_MI","DMPL","DRE","DVA"]
    for i in years:
        for j in tipos:
            url = "http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/%s/DADOS/%s_cia_aberta_%s.zip" % (j, j, str(i))
            request_dfp = requests.get(url)
            zip_dfp = zipfile.ZipFile(BytesIO(request_dfp.content))
            for k in zip_dfp.namelist():
                zip_dfp.extract(k, folder)

                new_file_name = "dfp_" + k.lower()
                os.rename(folder + k, folder + new_file_name)


def group_yearly_csv_files( values,
                            column_name,
                            files_identifier= "",
                            remove_from_filename= "",
                            files_path = "./",
                            save_path= "./group/"):
    """
    This function groups multiples CSV files with equal structure and something commom in their name.
    The year must be at the end of each file name and have the YYYY format.
    The consolidated files are then saven to the path.
    ISO5549-1 encoding is use to save and read the files since they are in portuguese language.

    Args:
        values: list of values to filter and group files by.
        column_name: column to be used to filter and group files.
        files_identifier: value present in the name of all files, will be used to distinguish which files to group.
        remove_from_filename: string to be removed from the filename. May be used when there is a repated and non-informative string in the name.
        files_path: path of the folder containing the ITR documents.
        save_path: path of the folder in which the documents will be saved into.
    """
    print("oi1")
    dir_files = os.listdir(files_path)
    print("oi2")
    dir_files = [x for x in dir_files if (files_identifier in x)]
    print(dir_files)
    print("oi3")
    files_unique = np.unique([x[:-9] for x in dir_files])
    print("oi4")
    years_unique = np.unique([x.replace(".csv", "")[-5:] for x in dir_files])
    print("oi5")
        
    if not os.path.exists(save_path):
        os.mkdir(save_path)

    print("oi6")
    print(years_unique)
    for year in years_unique:
        print(year)
        for file in files_unique:
            print(file)
            file_df = pd.read_csv(files_path + file + year + ".csv",
                 sep= ";",
                 decimal= ".",
                 encoding= "iso8859-1")
            print(file)
            print(year)
#            file_df = file_df.loc[file_df["ORDEM_EXERC"] == "ÚLTIMO"]
            for i in values:
                folder_file_path = save_path + i.replace("/", "-") + "/"
                
                if not os.path.exists(folder_file_path):
                    os.mkdir(folder_file_path)

                filepath = folder_file_path + file.replace(remove_from_filename, "") + ".csv"
                company_df = file_df.loc[file_df["CNPJ_CIA"] == i]
                
                if not os.path.isfile(filepath):
                    header = True
                else:
                    header = False

                company_df.to_csv(filepath, 
                    mode= "a",
                    encoding= "iso8859-1",
                    index= False,
                    header= header)
