import pandas as pd



AUDIT_FILE = r"C:\Users\darle\Downloads\AuditLogs_2024 (1)\Southwest_AuditLogs_2024.csv"

#"Topo_QC_Sheet_Phase1.xlsx","Topo_QC_Sheet_Phase2.xlsx","Topo_QC_Sheet_Phase3.xlsx",

def get_file_data():

    """
    Extracts data from specified QC spreadsheets and compiles it into a list of DataFrames.
    This function processes a list of QC spreadsheets, extracts data from specified sheets
    and columns, and compiles the data into a list of DataFrames. The extracted data is
    dependent on the phase specified in the spreadsheet names.
    Returns:
        list: A list of DataFrames containing the extracted data from each specified sheet.
    Example:
        file_data_dfs = get_file_data()
        for df in file_data_dfs:
            print(df)
    Notes:
        - This function assumes that the spreadsheets and sheet names are structured as
          defined in the `qc_sheets_maps` dictionary.
        - If any of the specified files are not read successfully, the function will
          terminate and print "File not Read!".
    """

    file_data_dfs = [] # holds all dfs for each sheet file names for the columns
                       # "Baseline XYZ.txt", "Raster Grid", "Structure XYZ.txt","Profile XYZ.txt"

    qc_spreadsheets = ["Topo_QC_Sheet_Phase4.xlsx","Topo_QC_Sheet_Phase1.xlsx","Topo_QC_Sheet_Phase2.xlsx","Topo_QC_Sheet_Phase3.xlsx"]

    qc_sheets_maps={
                    "Phase_1": ["TSW1 - Cartographical","TSW2&3 - Longdin & Browning", "TSW4 - Land & Sea", "TSW5 - Titan", "TSW6 - Cartographical", "Isles Of Scilly - PCO"],
                    "Phase_2": ["TSW01", "TSW_PCO","TSW02","TSW03", "TSW04","Isles Of Scilly - PCO", "External - PCO"],
                    "Phase_3-4": ["TSW01", "TSW_PCO","TSW02","TSW03", "TSW04", "TSWIOS (PCO)", "External - PCO"],}

    file_columns = ["Baseline XYZ.txt", "Raster Grid", "Structure XYZ.txt","Profile XYZ.txt"]

    for spreadsheet in qc_spreadsheets:
        print(f"Processing {spreadsheet}...")

        if spreadsheet == "Topo_QC_Sheet_Phase1.xlsx":
            qc_sheet_names  = qc_sheets_maps.get("Phase_1")
            header = 2

        elif spreadsheet == "Topo_QC_Sheet_Phase2.xlsx":
            qc_sheet_names = qc_sheets_maps.get("Phase_2")
            header =2

        else:
            qc_sheet_names = qc_sheets_maps.get("Phase_3-4")
            header = 1

        for sheet in qc_sheet_names:

            print(f"Processing {spreadsheet} {sheet}...")
            df = pd.read_excel(spreadsheet,
                               header=header,
                               sheet_name=sheet,
                               usecols=file_columns)
            if len(df) ==0:
                print("File not Read!")
                quit()

            file_data_dfs.append(df)

    return file_data_dfs


def format_qc_file_data(df):
    """
        Formats the QC file data by melting the DataFrame and saving the result to a CSV file.

        This function takes a DataFrame, melts it to create a long-format DataFrame where
        columns are unpivoted into rows, and saves the melted DataFrame to a CSV file.

        Args:
            df (pd.DataFrame): The input DataFrame containing the QC file data.

        Returns:
            pd.DataFrame: The melted DataFrame with columns 'All_Files' and 'Original_Column'.

        Example:
            df = pd.DataFrame({
                'Baseline XYZ.txt': ['A', 'B', 'C'],
                'Raster Grid': ['D', 'E', 'F'],
                'Structure XYZ.txt': ['G', 'H', 'I'],
                'Profile XYZ.txt': ['J', 'K', 'L']
            })
            df_melted = format_qc_file_data(df)
            print(df_melted)

        Notes:
            - The melted DataFrame is saved to a file named 'melted_csv.csv'.
            - The function prints the number of rows in the melted DataFrame and a
              completion message.

        """

    print(f"Formatting QC FIle Data..")

    df_melted = df.melt(value_name="Original_Column", var_name="All_Files")
    print(len(df_melted))
    df_set_melt = pd.DataFrame(df_melted).reset_index()


    print(f"Formatting QC FIle Data Complete: {len(df_set_melt)}")

    return df_melted


def run_audit_checks(df, audit_file):
    """
       Runs audit checks on QC file data and compares it with the national audit file.

       This function processes the QC file data, reads the national audit file, and performs
       several checks to compare the two datasets. It records the number of matching files
       and missing files, and saves intermediate and final results to CSV files.

       Args:
           df (pd.DataFrame): The DataFrame containing QC file data.
           audit_file (str): The path to the national audit file (CSV format).

       Returns:
           None

       Example:
           file_data = pd.concat(get_file_data())
           formatted_file_data = format_qc_file_data(file_data)
           run_audit_checks(formatted_file_data, "national_audit.csv")

       Notes:
           - This function assumes the national audit file is in CSV format with a
             'Files' column.
           - It splits the 'Files' column into a list, explodes the lists into separate rows,
             and adds an 'Original_Index' column to record the original index.
           - The function performs a simple location check on file names and records
             the number of matching and missing files.
           - Intermediate results are saved to 'Audit_CSV.csv', 'QC_Files_CSV.csv',
             and 'Matched_Columns.csv'.

       """

    qc_files_number = len(df)
    print(qc_files_number)

    audit_df = pd.read_csv(audit_file, delimiter=",")  # Read in National Audit File

    # Filter for topo only
    audit_df =  audit_df[audit_df['Type'] == 'topographic']
    audit_df_before_explode = audit_df.copy() # Make a copy of it before we run the explode

    # Split the 'file_paths' column into a list
    audit_df['Files'] = audit_df['Files'].str.split(';')

    # Add the original index as a new column
    audit_df['Original_Index'] = audit_df.index

    # Explode the lists into separate rows and remove any empty strings
    audit_df = audit_df.explode('Files').reset_index(drop=True)
    audit_df = audit_df[audit_df['Files'] != '']

    print(audit_df)
    df = df.dropna()
    df=df[df["Original_Column"] != "Filename"]
    audit_df.to_csv("Audit_CSV.csv")
    df.to_csv("All_QC_Sheet_Extracted_Files.csv")

    df["In_Audit"] = df["Original_Column"].isin(audit_df['Files'])

    import re
    years =[]
    # Define the regex pattern to match the year (4 consecutive digits)
    pattern = r"[-_](\d{4})"

    for x in df["Original_Column"]:
        # Search for the pattern in the filename
        match = re.search(pattern, x)
        if match:
            years.append(match.group(0).replace("_",""))
        else:
            years.append("None")



    df["Year"] = years

    df.to_csv("Files From QC sheet not in National Audit.csv")

    ## attempt simple loc on file names
    #matching_df = audit_df.loc[audit_df["Files"].isin(df["Original_Column"])]
    #matching_df.to_csv("Matched_Columns.csv")
#
    ## attempt simple loc on file names
    #non_matching_df = audit_df.loc[~audit_df["Files"].isin(df["Original_Column"])]
    #non_matching_df.to_csv("All_Files_Not_Found_In_National_Log.csv")
#
    ##  Assuming if they received any of the files for a row in national audit csv (one row contains multiple files)
    ##  Then the whole data set was received:
#
    #indexes = audit_df["Original_Index"].unique()
    #selected_rows = audit_df_before_explode.loc[indexes]
#
    #print(f"Found {len(matching_df)} Topo Files " )
    #print(f"Found {len(audit_df_before_explode) - len(selected_rows)} Missing From National")






file_data =pd.concat(get_file_data())

formatted_file_data  = format_qc_file_data(file_data)
print(len(formatted_file_data))


run_audit_checks(formatted_file_data, AUDIT_FILE)
















