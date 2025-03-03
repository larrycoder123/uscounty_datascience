import requests
import csv
import os 
import pandas as pd
import re

# Specify the folder where the file will be saved
data_folder = "data_uscensus"

# Specify the text file where the US Census API is stored
api_file = "apikey_uscensus.txt"

# Different data series require different api requests 
# https://censusreporter.org/topics/table-codes/
# check api link for specific table 
series_dictionary = {
    "B": "",
    "S": "/subject",
    "DP": "/profile"
}

# Specify the year of the data
year = "2023"

def uscensus_checkseries(variables):

    series = None    

    for var in variables:

        # extract letters until first number
        match = re.match(r"([A-Za-z]+)", var)
        if match: 
            beginningstring = match.group(1)
        else:
            raise ValueError(f"Variable {var} does not start with letters, cant detect series.")

        if series is None: 
            series = beginningstring
        else: 
            if beginningstring != series: 
                raise Exception("Import variables must be of the same series.")

    print(f"All variables belong to {series} series, API link will be adjusted accordingly.")

    return series

def uscensus_importcsv(column_dictionary, year, output_file_name):

    # Import API key from api_file
    with open(api_file, "r") as file:
        api_key = file.read()

    # Extract all specified variables as list from the dictionary
    specific_variables = list(column_dictionary.keys())

    # Add NAME as additional variable which contains county and state name  
    all_variables = ["NAME"] + specific_variables

    # Format variables as string for the link
    link_variables = ",".join(all_variables)

    # Use predefined function to check the series
    series = uscensus_checkseries(specific_variables)

    dataseries = series_dictionary.get(series, None)

    if dataseries is None: 
        raise Exception(f"Series not recognized, please check if {series} is included in series_dictionary.")

    # Construct the URL with the specified variables
    url = f"https://api.census.gov/data/{year}/acs/acs5{dataseries}?get={link_variables}&for=county:*&key={api_key}"

    #
    ### Check files
    #

    # Check if data_folder exists, if not create the folder
    if not os.path.exists(data_folder):
        print(f"{data_folder} does not exist, creating...")
        os.makedirs(data_folder)

    # Define the output file path
    output_file_path = os.path.join(data_folder, output_file_name)

    # Check if file already exists, if yes delete
    if os.path.exists(output_file_path):
        print("Existing file found, removing...")
        os.remove(output_file_path)

    #
    ### Make request to api
    #

    # Make the request
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON data
        data = response.json()
        
        # Headers in the original data response
        original_headers = data[0]
        
        # Reorder the headers 
        reordered_headers = ["state", "county"] + all_variables

        # If header (column name) is in the dictionary, the variable should be renamed
        # If header is not in the dictionary the variable should be capitalized (first letter uppercase, remaining one's lowercase)
        descriptive_headers = []
        for header in reordered_headers:
            if header in column_dictionary:
                    descriptive_headers.append(column_dictionary[header])
            else:
                descriptive_headers.append(header.capitalize())

        imported_variables = []
        for header in descriptive_headers:
            if header in column_dictionary.values():
                imported_variables.append(header)
        print(f"Imported Variables:{imported_variables}")

        # Write data to CSV with reordered and renamed headers
        with open(output_file_path, "w", newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(descriptive_headers)  # Write header row with descriptive names
            
            # Reorder each row based on the specified order and write to CSV
            for row in data[1:]:
                reordered_row = [row[original_headers.index(col)] for col in reordered_headers]
                writer.writerow(reordered_row)
        
        print(f"Data saved to {output_file_path}")
    else:
        print(f"Request failed with status code {response.status_code}")

    return output_file_path, imported_variables

# Universal modify for csv

def uscensus_modify(output_file_path, specific_variables):
    # Load the CSV file into a pandas DataFrame
    # Latin Encoding (Puerto Rico County Names with special characters)
    # State / County as string to keep leading 0 
    df = pd.read_csv(f"{output_file_path}", encoding="latin-1", dtype={"State": str, "County": str})

    # 1. Merge the "State" Code and "County" Code to create the "FIPS Code" column
    df["FIPS Code"] = df["State"].astype(str).str.zfill(2) + df["County"].astype(str).str.zfill(3)

    # 2. Split the Name into "County Name" and "State Name"
    df[["County Name", "State Name"]] = df["Name"].str.split(', ', expand=True)

    # 3. Specify columns to keep
    # Start with additional columns to keep
    additional_columns = ["FIPS Code", "County Name", "State Name"]
    # Add the imported columns specified in the dictionary, except the first one ("NAME")
    imported_columns = specific_variables
    # Combine both lists
    columns_to_keep = additional_columns + imported_columns
    # Check if all specified columns are in the df
    for col in columns_to_keep: 
        if col not in df.columns:
            raise Exception(f"{col} not in df, check again")
    # Keep all the desired columns
    df = df[columns_to_keep]

    # 4. Remove all counties in Puerto Rico 
    df = df[df["State Name"] != "Puerto Rico"]

    # 5. Check if df contains 3144 counties 
    if df.shape[0] != 3144: 
        raise Exception(f"{df.shape[0]} instead of 3144 counties in df, check again")

    # 6. Overwrite the modified DataFrame back to the original file
    df.to_csv(f"{output_file_path}", index=False)
    print(f"{output_file_path} has been modified and saved")

    df.head()