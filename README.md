# Python_Assignment_2
A Comprehensive ETL Workflow with Python for Data Engineers

Overview : 
The ETL process follows this sequence:
Extraction Phase:
The project extracts data from all CSV, JSON, and XML files located in the project directory.
Each file type is processed, and the results are combined into one DataFrame.
Transformation Phase:
The extracted data undergoes transformation to convert the measurements to standard units (e.g., height to meters, weight to kilograms).
Loading Phase:
The transformed data is written into a CSV file, which can be imported into a database for further use.
Logging:
The start and end of each phase (Extraction, Transformation, Loading) are logged to track progress and ensure everything runs smoothly.


Tools Used :
VS Code 

Python Libraries:
pandas
json
xml.etree.ElementTree

Files :
1. Source Data files : source1.csv , source1.json , source1.xml
2. ETL Script : ETL_pipeline.py
3. Log File : log_file.txt
4. Output File : transformed_data.csv

Approaches and Insights
1.Extraction : Read input files and logs messages for file process status ( success and also for missing or invalid files)
              Combines data from all file formats into a single data frame
2.Transformation : Converts combined dataframe data for column height from inches to meters and for column weight from pounds to kilograms
3.Loading : Saved the transformed data to transformed_data.csv
Logging : Logs start and end for each step and also if any error encountered
