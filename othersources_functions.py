
import pandas as pd

def othersources_transformfips(df, fips_column):

    # Rename the fips_column to fips_code
    df = df.rename(columns={fips_column: "fips_code"})

    # Fill all rows with less than 5 characters with leading 0's
    df["fips_code"] = df["fips_code"].astype(str).str.zfill(5)

    if (df["fips_code"].str.len() == 5).all():
        print("All FIPS Codes are now length 5.")
    else: 
        raise Exception("Not all FIPS Codes are length 5, check again.")

    return df  


def othersources_mapdf(df, columns_dictionary, county_df):

    if "fips_code" not in df.columns:
        raise Exception(f'No "fips_code" column found.')

    columns_to_keep = ["fips_code"] + list(columns_dictionary.keys())
    
    df = df[columns_to_keep].copy()

    df.rename(columns = columns_dictionary, inplace=True)
    
    # Merges only the rows where the df FIPS Code matches that of the county_df, otherwise NaN
    merged_df = pd.merge(county_df, df, on=["fips_code"], how ="left")

    missing_in_df = county_df[~county_df["fips_code"].isin(df["fips_code"])]

    if missing_in_df.shape[0] != 0:
        print(f"Some counties were missing in the dataframe, {missing_in_df.shape[0]} NaN values were introduced.")
        missing_fips = missing_in_df["fips_code"].tolist()
        print("Missing FIPS Codes:")
        print(missing_fips)

    missing_counts = merged_df.isnull().sum()
    print("Number of missing values in each column:")
    print(missing_counts)
    
    return merged_df 