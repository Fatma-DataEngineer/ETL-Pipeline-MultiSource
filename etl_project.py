# Import essential libraries for data handling and file operations

import pandas as pd
import os.path
import glob
import logging
from datetime import datetime
import xml.etree.ElementTree as ET
import requests as RQ
import sqlite3

# Define the base directory for the ETL project
Data_Path=r"D:project_ETL"

# Set paths for each data source type
Path_csv=os.path.join(Data_Path,"csv_project")        # Directory for CSV files
Path_json=os.path.join(Data_Path,"json_project")      # Directory for json files
Path_xml=os.path.join(Data_Path,"xml_project")        # Directory for xml files

# Define API URLs for external data sources
URL_list=["https://raw.githubusercontent.com/Fatma-DataEngineer/API-ETL-Project/main/order1.json",
          "https://raw.githubusercontent.com/Fatma-DataEngineer/API-ETL-Project/main/order1.json",
          "https://raw.githubusercontent.com/Fatma-DataEngineer/API-ETL-Project/main/order3.json"]
# Initialize lists to store extracted data from XML and API sources
xml_list=[]   # Stores parsed XML data
API_list=[]   # Stores data fetched from API endpoints

# Configure logging to track ETL process steps and issues
# Logs are saved to 'etl_project.txt' with timestamp, log level, function name, and message
logging.basicConfig(filename="Logs.txt",filemode="w",level=logging.INFO,
                    format="%(asctime)s |%(levelname)s | %(filename)s |%(funcName)s |%(message)s")

# Record the start time of the ETL process for logging purposes
start_time=datetime.now().strftime("%H:%M:%S || %Y-%m-%d")

""" Extracts and validates CSV files from a specified directory.
    Returns:
        pd.DataFrame: A concatenated DataFrame containing all valid CSV files."""
def extract_csv_files():
    logging.info(f"Start extract_csv_files at time {start_time}")
    # Locate all CSV files in the specified directory
    csv_files=glob.glob(os.path.join(Path_csv,"*.csv"))
    logging.info(f"\nCSV_files :{len(csv_files)}")
    if not csv_files:
        logging.warning(f"{csv_files} is not found in specified path")
        return pd.DataFrame()
    read_file=[]
    # Loop through all CSV files in the specified path
    for file in csv_files:
        try:
            # Read the current CSV file using UTF-8 encoding
            files =pd.read_csv(file,encoding="UTF-8")
            # Check if the file is empty
            if files.empty:
               logging.warning(f"{files} is empty")
               continue
            # Define required columns for validation
            csv_columns={"order_id","customer_name","customer_email","product_name",
                         "category","price","quantity","order_date"}
            # Verify that all required columns are present
            if not csv_columns.issubset(files.columns):
               logging.error(f" csv_columns is missing {csv_columns - set(files.columns)}")
            # Append the valid file to the list
            read_file.append(files)
        except Exception as e:
            # Log any error encountered during file reading
            logging.error(f"Error reading file {read_file}:{e}")
            # If no valid files were found, return an empty DataFrame
            if not read_file:
               logging.warning("No valid CSV files were read.")
               return pd.DataFrame()
    # Concatenate all valid CSV files into a single DataFrame
    all_files=pd.concat(read_file,ignore_index=True)
    # Record the end time of the extraction process
    end_time=datetime.now()
    duration = end_time - datetime.strptime(start_time, "%H:%M:%S || %Y-%m-%d")
    logging.info(f"Finish extract_csv_files at time {end_time} , duration({duration})")
    return all_files
""" Extracts and validates JSON files from a specified directory.
    Returns:
        pd.DataFrame: A concatenated DataFrame containing all valid JSON files. """
def extract_json_files():
    logging.info(f"Start extract_json_files at {start_time}")
    # Locate all JSON files in the specified directory
    json_files=glob.glob(os.path.join(Path_json,"*.json"))
    logging.info(f"\njson_files :{len(json_files)}")
    if not json_files:
        logging.warning(f"{json_files} is not found in specified path")
        return pd.DataFrame()
    read_files=[]
    # Loop through all JSON files in the specified path
    for file in json_files:
        try:
           # Read JSON file with column orientation
           files=pd.read_json(file,orient="records")
           # Check if the file is empty
           if files.empty:
              logging.warning(f"{files} is empty.")
              continue
           # Define required columns for validation
           json_columns={"order_id","customer_name","customer_email","product_name",
                         "category","price","quantity","order_date"}
           # Check for missing columns
           missing = json_columns - set(files.columns)
           if missing:
               logging.error(f"{file} is missing columns: {missing}")
               continue
           # Clean and convert order_date
           files["order_date"] = files["order_date"].astype(str).str.strip().str.replace(r"\s+","",regex=True)
           files["order_date"] = pd.to_datetime(files["order_date"], format="%Y/%m/%d", errors="coerce")
           # Log rows with invalid dates
           invalid = files[files["order_date"].isna()]
           files["order_date"] = files["order_date"].fillna("MISSING_DATE")
           if not invalid.empty:
               logging.warning(f"{file} contains invalid order_date values in rows: {invalid.index.tolist()}")
           read_files.append(files)
        except Exception as e:
            # Log any error encountered during file reading
            logging.warning(f"Error reading file {file}:{e}")
            # If no valid files were found, return an empty DataFrame
    if not read_files:
                logging.error("No valid json files were read.")
                return pd.DataFrame()
    # Concatenate all valid DataFrames
    all_files=pd.concat(read_files,ignore_index=True)
    # Record the end time of the extraction process
    end_time = datetime.now()
    duration = end_time - datetime.strptime (start_time,"%H:%M:%S || %Y-%m-%d")
    # Log completion time and duration
    logging.info(f"Finish extract_json_files at {end_time}, duration {duration}")
    return all_files
""" Extracts and validates XML files from a specified directory.
    Returns:
        pd.DataFrame: A concatenated DataFrame containing all valid XML files. """
def extract_xml_files():
    logging.info(f"Start extract_xml_files at time {start_time}")
    # Locate all XML files in the specified directory
    xml_files=glob.glob(os.path.join(Path_xml,"*.xml"))
    logging.info(f"\nxml_files:{len(xml_files)}")
    if not xml_files:
        logging.warning(f"{xml_files} is not found in specified path")
        return pd.DataFrame()
    # Loop through all XML files in the specified path
    for file in xml_files:
        try:
            # Attempt to parse the XML file
            parse_file=ET.parse(file)
            read_file=parse_file.getroot()
        except ET.ParseError as e:
            logging.error(f"Failed to parse XML file {file}: {e}")
            continue
        for orders in read_file:
            # Extract each required field from the order element
            order_id = orders.findtext("order_id", "").strip()
            customer_name = orders.findtext("customer_email", "").strip()
            customer_email = orders.findtext("product_name", "").strip()
            product_name = orders.findtext("category", "").strip()
            category = orders.findtext("category", "").strip()
            price = orders.findtext("price", "").strip()
            quantity = orders.findtext("quantity", "").strip()
            order_date =  orders.findtext("order_date", "").strip()
            if "" in [order_id, customer_name, customer_email, product_name, category, price, quantity, order_date]:
                logging.warning(f"Skipping incomplete order in file {file}")
                continue
            #convert price from string to integer
            try:
                price = int(price)
            except ValueError:
                logging.warning(f"Invalid price in file {file}: {price}")
                continue
            #convert date from YYY/mm/dd  to YYY-mm-dd
            order_date = pd.to_datetime(order_date, errors="coerce")
            if pd.isna(order_date):
                logging.warning(f"Invalid date format in file {file}: {order_date}")
                continue
            xml_list.append({"order_id":order_id ,"customer_name":customer_name ,
                            "customer_email": customer_email,"product_name":product_name ,
                            "category":category ,"price":price ,"quantity":quantity ,
                            "order_date":order_date})
    # Final check: if no valid orders were found, return an empty DataFrame
    if not xml_list:
        logging.warning("No valid orders extracted from XML files.")
        return pd.DataFrame()
    # Convert the list of dictionaries into a pandas DataFrame
    all_file=pd.concat([pd.DataFrame(xml_list)],ignore_index=True)
    ###all_file.to_csv(r"D:\Pyth-Project\ETL\project_ETL\all_files.csv", index=False)
    # Record the end time of the extraction process
    end_time = datetime.now()
    duration = end_time - datetime.strptime(start_time, "%H:%M:%S || %Y-%m-%d")
    # Log completion time and duration
    logging.info(f"Finish extract_xml_files at {end_time},duration {duration}")
    return all_file
""" Extracts and validates API files from a specified directory.
    Returns:
        pd.DataFrame: A concatenated DataFrame containing all valid API files. """
def extract_API_files():
    logging.info(f"Start extract_API_files at time {start_time}")
    # Columns expected in each API response
    required_columns = {'order_id', 'customer_name', 'customer_email', 'product_name', 'category', 'price', 'quantity', 'order_date'}
    # Expected data types for each column
    expected_types = {'order_id': int, 'customer_name': str, 'customer_email':str,'product_name':str,"price":int,'quantity': int}
    for URL in URL_list:
        try:
            # Send GET request to the API
            get_file=RQ.get(URL, timeout=10)
            get_file.raise_for_status()      # Raise error if response status is not 200
            # Parse JSON content from the response
            load_file=get_file.json()
            # Skip if the response contains no data
            if not load_file:
                logging.warning(f"No data returned from {URL}")
                continue
            # Convert JSON data to a DataFrame
            read_file=pd.DataFrame(load_file)
            logging.info(f"\nAPI_files :{len(read_file)}")
            # Check if all required columns are present
            if not required_columns.issubset(read_file.columns):
               missing = required_columns - set(read_file.columns)
               logging.warning(f"Missing columns {missing} in response from {URL}")
               continue
            read_file['order_date'] = pd.to_datetime(read_file['order_date'], errors='coerce')
            # Validate data types for each required column
            for col, expected_type in expected_types.items():
               if not read_file[col].map(lambda x: isinstance(x, expected_type)).all():
                  logging.warning(f"Column '{col}' has invalid types in response from {URL}")
                  continue
            # Add the validated DataFrame to the list
            API_list.append(read_file)
        except RQ.exceptions.RequestException as e:
            logging.error(f"Request failed for {URL}: {e}")
        except ValueError as e:
            logging.error(f"Invalid JSON from {URL}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error with {URL}: {e}")
    # If no valid data was collected, return an empty DataFrame
    if not API_list:
        logging.error("No valid API responses were collected.")
        return pd.DataFrame()
    # Concatenate all validated DataFrames into one
    all_file=pd.concat(API_list,ignore_index=True)
    # Record the end time of the extraction process
    end_time = datetime.now()
    duration = end_time - datetime.strptime(start_time, "%H:%M:%S || %Y-%m-%d")
    # Log completion time and duration
    logging.info(f"Finish extract_API_files at {end_time},duration {duration}")
    return all_file
""" Validate and transform input DataFrame by converting types and calculating totals.
    Parameters:
        data (pd.DataFrame): Raw input data containing 'price' , 'quantity' and 'order_date' columns.
    Returns:
        pd.DataFrame: Cleaned and transformed data with a new 'total' column. """
def transform_files(data):
    logging.info(f"Start transform_files at time {start_time}")
    #Validate presence of data columns
    data_columns = ["price", "quantity","order_date"]
    # Restore original date where conversion failed
    for col in data_columns:
        if col not in data.columns:
            logging.error(f"Missing required column: '{col}'")
            raise ValueError(f"Column '{col}' is missing from input data.")
        # Preserve original 'order_date' before conversion
        data["order_date_raw"] = data["order_date"]
    # Convert 'price' and 'quantity' to numeric types
    data["price"]= pd.to_numeric(data["price"] ,errors="coerce")
    data["quantity"]= pd.to_numeric(data["quantity"] ,errors="coerce")
    # Convert 'order_date' to datetime format using raw version
    data["order_date"] = pd.to_datetime(data["order_date_raw"].astype(str).str.strip(),errors="coerce",dayfirst=True)
    # Restore original date where conversion failed
    data["order_date"] = data["order_date"].fillna(data["order_date_raw"])
    #Check for missing or invalid numeric values after conversion
    if data[["price", "quantity"]].isnull().any().any():
        logging.warning("Some rows contain invalid or missing numeric values in 'price', 'quantity', or 'order_date'.")
    #Calculate total value
    data["total"]=(data["price"]*data["quantity"]).round (2)
    # Record the end time of the extraction process
    end_time = datetime.now()
    duration = end_time - datetime.strptime(start_time, "%H:%M:%S || %Y-%m-%d")
    # Log completion time and duration
    logging.info(f"Finish transform_files at {end_time},duration {duration}")
    return data
def load_files():
    logging.info(f"start load_files at time {start_time}")
    # Extract and transform each source
    load_csv=transform_files(extract_csv_files())
    load_json=transform_files(extract_json_files())
    load_xml=transform_files(extract_xml_files())
    load_API=transform_files(extract_API_files())
    # Add source column for traceability
    load_csv["source"] = "CSV"
    load_json["source"] = "JSON"
    load_xml["source"] = "XML"
    load_API["source"] = "API"
    # Validate each DataFrame
    sources = {
        "CSV": load_csv,
        "JSON": load_json,
        "XML": load_xml,
        "API": load_API }
    for name, df in sources.items():
        logging.info(f"{name} columns: {df.columns.tolist()}")
        if df.empty:
            logging.warning(f"{name} data is empty after transformation.")
        if "order_date" in df.columns:
            logging.info(f"{name} 'order_date' dtype: {df['order_date'].dtype}")
        else:
            logging.error(f"{name} missing 'order_date' column.")
    # Ensure all DataFrames have expected columns
    expected_cols = ["price", "quantity", "order_date", "order_date_raw", "total", "source"]
    for df in [load_csv, load_json, load_xml, load_API]:
        for col in expected_cols:
            if col not in df.columns:
                    df[col] = None
    # Concatenate all transformed data
    load_all_files = pd.concat([load_csv, load_json, load_xml, load_API], ignore_index=True)
    # Final validation
    if load_all_files.empty:
        logging.error("Final DataFrame is empty. Aborting save.")
        return None
    # drop raw date column before saving
    if "order_date_raw" in load_all_files.columns:
        load_all_files.drop(columns=["order_date_raw"], inplace=True)
    # Log missing dates
    missing_dates = load_all_files["order_date"].isna().sum()
    total_rows = len(load_all_files)
    logging.info(f"Rows with missing 'order_date': {missing_dates} out of {total_rows}")
    logging.info("Sample rows with missing 'order_date':")
    logging.info(load_all_files[load_all_files["order_date"].isna()].head())
    # Save to CSV and SQL
    load_all_files.to_csv(r"D:project_ETL\load_all_files.csv", index=False)
    conn = sqlite3.connect("DB_files.db")
    load_all_files.to_sql("DB_files", conn, if_exists="replace", index=False)
    conn.close()
    missing_dates = load_all_files['order_date'].isna().sum()
    total_rows = len(load_all_files)
    logging.info(f"Rows with missing 'order_date': {missing_dates} out of {total_rows}")
    # Record the end time of the extraction process
    end_time = datetime.now()
    duration = end_time - datetime.strptime(start_time, "%H:%M:%S || %Y-%m-%d")
    # Log completion time and duration
    logging.info(f"Finish load_all_files at {end_time},duration {duration}")
    return load_all_files
if __name__=="__main__":
    print(load_files())