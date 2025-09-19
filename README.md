🛠️ Multi-Source ETL Pipeline Project

📌 Overview
This project demonstrates a complete ETL (Extract, Transform, Load) pipeline that collects data from four different sources:
- CSV files  
- JSON files  
- XML files  
- Public APIs  
The data is validated and merged into unified outputs in both **CSV** and **SQL** formats.

🚀 License
This project is licensed under the MIT License - see the [LICENSE] file for details.

📂 Project Structure

| File Name         | Description                                                         |
|-------------------|---------------------------------------------------------------------|
| data sources      | # Input files (CSV, JSON, XML, API)                                 |
| scripts           | # Python scripts for extraction, validation, merging, and exporting |
| logs              | # Execution logs and validation reports                             |
| requirements.txt  | # Required Python libraries                                         |
| output            | # Final CSV and SQL datasets                                        |
| README.md         | # Project documentation                                             |
| LICENSE           | # Project license                                                   |

🚀 ETL Workflow

1. **Extract**  
   - Reads data from local files and external URLs  
   - Handles encoding issues, missing fields, and nested structures  
   - Modular and reusable extract functions  
  
2. **Transform**  
   - Cleans nulls, fixes types, standardizes formats  
   - Applies validation rules:  
    - Column type checks  
    - Duplicate removal  
    - Custom logic per source  
    - Logs errors and warnings for review  

3. **Load**  
   - Saves final output to load_all_files.csv and DB_files.db  
   - Includes summary report and validation log in logs 

4. **Validate**  
   - Ensures required columns, correct data types, and no nulls  

5. **Merge**  
   - Combines all sources into a unified dataset  

6. **Export**  
   - Outputs results in CSV and SQL formats  

🧪 Technologies Used

- Python  
- os.path
- glob
- logging
- datetime
- pandas  
- requests  
- sqlite3  
- xml.etree.ElementTree  

📈 Output Files

| File Name          | Description                               |
|--------------------|-------------------------------------------|
| load_all_files.csv | Unified dataset ready for analysis        |
| DB_files.db        | SQL-ready version of the dataset          |
| README.md          | Project documentation                     |
| requirements.txt   | Required Python libraries                 |
| LICENSE            | Project license                           |
| logs.txt           | Execution logs and validation reports     |

⚙️ Main Functions
-🧩 extract_csv_files()   – Extracting data from multiple CSV files located in a specified directory, validates their structure, and aggregates clean data into a unified DataFrame.
-🧩 extract_json_files()  – Extracting data from multiple JSON files located in a specified directory, validates their structure, and aggregates clean data into a unified DataFrame.
-🧩 extract_xml_files()   – Extracting data from multiple XML files located in a specified directory, validates their structure, and aggregates clean data into a unified DataFrame.
-🧩 extract_API_files()   – Extracting data from multiple API files located in a specified directory, retrieves JSON, validates their structure, and aggregates clean data into a unified DataFrame.
-🧩 transform_files(data) – Cleans and transforms a raw dataset by validating required columns, converting data types, and calculating a total value per row.
-🧩 load_files()          – Validates, merges, and saves final output

📥 Required Columns for CSV - XML- json - API files:
- order_id
- customer_name
- customer_email
- product_name
- category
- price
- quantity
- order_date

📤 Output all_files
- A single DataFrame containing all valid rows from the JSON,CSV,API,XML files.
- Returns an empty DataFrame if no valid files are found.
- A cleaned and validated DataFrame containing all valid orders.
- Ready for merging, transformation, or export to CSV/SQL.

⚠️ Error Handling
- Logs a warning if a file is empty.
- Logs a warning if required columns are missing.
- Logs an error if the file cannot be read.
- Invalid files are skipped without interrupting the process. 
 
✅ Validation Steps

- **Empty Check**: Logs warnings if any source is empty after transformation
- **Column Consistency**: Compares column names across sources
- **Final Output Check**: Aborts saving if merged DataFrame is empty
- **Shape Logging**: Logs final DataFrame shape for traceability
⚙️ Logging Configuration

The project uses Python's built-in logging module to record all ETL execution events with timestamps.
📄 Log File:
- log.txt is created fresh on each run (`filemode="w"`).
- Logs include timestamp, log level, function name, and message.

🕒 Start Time:
- Execution start time is recorded using datetime.now() and stored in start_time.
- This timestamp is used in log messages to trace when each function begins.
 🛠️ duration Time
- End time and total duration are logged after the extraction completes.
🧾 Logging Example
2025-09-16 18:42:56,404 |INFO | etl_project.py |extract_csv_files |Start extract_csv_files at time 18:42:56 || 2025-09-16

🌍 Language and Documentation
- Inline comments within the code: In English, in a professional style
- External documentation: Available in English
- Code is standardized and clearly documented for global customers
📝
Dates are saved in YYYY-MM-DD format. If Excel shows a different style,
change the column format to YYY-mm-dd using Ctrl + 1 → Custom.

👩‍💻 Author 
Developed by Fatma, a data engineer passionate about building robust ETL pipelines that unify data from diverse sources. Focused on validation logic, clean code structure, and professional documentation
GitHub: [Fatma-DataEngineer](https://github.com/Fatma-DataEngineer)