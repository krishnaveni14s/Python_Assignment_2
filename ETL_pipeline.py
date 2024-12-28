import pandas as pd
import json 
import xml.etree.ElementTree as et
import datetime

# Function to print messages to log file
def log_message(log_text, log_path):
    try:
        with open(log_path, 'a') as log_file:
            log_file.write(f"{datetime.datetime.now()} | {log_text}\n")
    except FileNotFoundError:
        print(f"{datetime.datetime.now()} | Log file not found")


#Defining functions to read the data from different files
#Function to read the data from csv file
def extract_from_csv(file_path,csv_file_name,log_path):
    csv_file = f"{file_path}{csv_file_name}"
    try:
        print(f"{datetime.datetime.now()} | Reading csv file data")
        log_message("Reading csv file data",log_path)
        df = pd.read_csv(csv_file)
        #print("Data read successfully")
        #print(df.head(5))
        return df
    except Exception as e:
        print(f"{datetime.datetime.now()} | Error reading the csv file: {e}")
        log_message(f"Error reading CSV file: {e}", log_path)
        return None

#Function to read the data from json file
def extract_from_json(file_path,json_file_name,log_path):
    json_file = f"{file_path}{json_file_name}"
    try:
        print(f"{datetime.datetime.now()} | Reading json file data")
        log_message("Reading JSON file data", log_path)
        data = []
        with open(json_file,'r') as file:
            for line in file:
                json_obj=json.loads(line.strip())
                data.append(json_obj)
        df = pd.DataFrame(data)
        return df
    except Exception as e:
        print(f"{datetime.datetime.now()} | Error reading the json file: {e}")
        log_message(f"Error reading JSON file: {e}", log_path)
        return None

#Function to read the data from xml file
def extract_from_xml(file_path,xml_file_name,log_path):
    xml_file = f"{file_path}{xml_file_name}"
    try:
        print(f"{datetime.datetime.now()} | Reading xml file data")
        log_message("Reading XML file data", log_path)
        tree = et.parse(xml_file)
        root = tree.getroot()
        data = []
        for level1 in root:
            strt = {}
            for sublevel in level1:
                strt[sublevel.tag] = sublevel.text
            data.append(strt)
        df = pd.DataFrame(data)
        return df
    except Exception as e:
        print(f"{datetime.datetime.now()} | Error reading the xml file: {e}")
        log_message(f"Error reading XML file: {e}", log_path)
        return None



#main function

def main():
    # Source file path
    file_path  = '/home/krishna/Documents/Guvi Assignments/ETL using python/'
    #source filenames
    csv_file_name = 'source1.csv'
    json_file_name = 'source1.json'
    xml_file_name = 'source1.xml'
    log_file = 'log_file.txt'
    target_file = 'transformed_data.csv'
    target_path = f"{file_path}{target_file}"
    exe_date = datetime.datetime.now().strftime("%Y%m%d")
    #log_path = f"{file_path}{log_file}_{exe_date}"
    log_path = f"{file_path}{log_file}"


    start_time = datetime.datetime.now()
    print(f"{start_time} | Starting ETL process")
    log_message("Starting ETL process:", log_path)
    log_message(f"Source file path: {file_path}", log_path)
    log_message(f"Source filenames: {csv_file_name}, {json_file_name}, {xml_file_name}", log_path)
    log_message(f"Log file: {log_file}", log_path)
    log_message(f"Target file: {target_file}", log_path)
    #Extracting data from csv file
    try:
        csv_df = extract_from_csv(file_path,csv_file_name,log_path)
        if csv_df is not None:
            #Reading first 5 rows of the csv file
            #print(csv_df.head(5))
            #log_message(f"\n{csv_df.head(5)}", log_path)
            log_message(f"CSV file extraction successfully completed", log_path)
        else:
            print(f"{datetime.datetime.now()} | No data extracted from CSV file.")
            log_message("No data extracted from CSV file", log_path)
    except Exception as e:
        print(f"{datetime.datetime.now()} | Error processing CSV file: {e}")
        log_message(f"Error processing CSV file: {e}", log_path)

    #Extracting data from json file
    try:
        json_df = extract_from_json(file_path,json_file_name,log_path)
        if json_df is not None:
            #Reading first 5 rows of the json file
            #print(json_df.head(5))
            #log_message(f"\n{json_df.head(5)}", log_path)
            log_message(f"JSON file extraction successfully completed", log_path)
        else:
            print(f"{datetime.datetime.now()} | No data extracted from JSON file.")
            log_message("No data extracted from JSON file", log_path)
    except Exception as e:
        print(f" Error processing JSON file: {e}")
        log_message(f"Error processing JSON file: {e}", log_path)

    #Extracting data from xml file
    try:
        xml_df = extract_from_xml(file_path,xml_file_name,log_path)
        if xml_df is not None:
            #Reading first 5 rows of the xml file
            #print(xml_df.head(5))
            #log_message(f"\n{xml_df.head(5)}", log_path)
            log_message(f"XML file extraction successfully completed", log_path)
        else:
            print(f"{datetime.datetime.now()} | No data extracted from XML file.")
            log_message("No data extracted from XML file", log_path)
    except Exception as e:
        print(f"{datetime.datetime.now()} | Error processing XML file: {e}")
        log_message(f"Error processing XML file: {e}", log_path)
    
    #Combining the extracted data from different sources
    dataframes = [df for df in [csv_df, json_df, xml_df] if df is not None]
    if dataframes:
        df_combined = pd.concat(dataframes, ignore_index=True)
        print(f"{datetime.datetime.now()} | Data Extracted successfully")
        log_message("Data extracted successfully", log_path)
        #print(df_combined)
        #log_message(f"\n{df_combined}", log_path)

        # Transforming heights from inches to meters (1 inch = 0.0254 meters)
        if 'height' in df_combined.columns:
            df_combined['height'] = df_combined['height'].astype(float) * 0.0254
            df_combined['height'] = df_combined['height'].round(2)

        # Transforming weights from pounds to kilograms (1 pound = 0.453592 kilograms)
        if 'weight' in df_combined.columns:
            df_combined['weight'] = df_combined['weight'].astype(float) * 0.453592
            df_combined['weight'] = df_combined['weight'].round(2)

        #print(f"{datetime.datetime.now()} | Transformed DataFrame:")
        #log_message("Transformed DataFrame:", log_path)
        #print(df_combined)
        #log_message(f"\n{df_combined}", log_path)
        print(f"{datetime.datetime.now()} | Data Transformation completed successfully.")
        log_message("Data Transformation completed successfully", log_path)
        print(f"{datetime.datetime.now()} | Data Load : Writing the data to the destination file")
        log_message("Data Load : Writing the data to the destination file", log_path)
        # Writing the data to the csv file
        try:
            df_combined.to_csv(target_path, index=False)
            print(f"{datetime.datetime.now()} | Data written to the destination file successfully")
            log_message("Data written to the destination file successfully", log_path)
        except Exception as e:
            print(f"{datetime.datetime.now()} | Error writing data to the destination file: {e}")
            log_message(f"Error writing data to the destination file: {e}", log_path)
        
    else:
        print(f"{datetime.datetime.now()} | No data extracted from any source.")
        log_message("No data extracted from any source", log_path)

    end_time_extract = datetime.datetime.now()
    print(f"{datetime.datetime.now()} | ETL process completed , Total Execution time : {end_time_extract - start_time}")
    log_message(f"ETL process completed , Total Execution time : {end_time_extract - start_time}\n", log_path)


main()
