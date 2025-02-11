import pandas as pd
from sqlalchemy import create_engine

host = "localhost"
port = "5432"
database = "Birds_db"
user = "postgres"
password = "1418"

# Create the connection string (URL format) -> postgres

engine_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"

engine = create_engine(engine_string)

# Birds Foreset Observation

# File Path

file_path = 'D:\Guvi Projects\Mini Project - 2\Bird_Monitoring_Data_FOREST.XLSX'

# Load all sheets into a dictionary of DataFrames

sheets = pd.read_excel(file_path,sheet_name=None)

# Combine all sheet into single dataframe

df_list = []

for sheet_name, df in sheets.items():
    df["Admin_Unit_Code"] = sheet_name  
    df_list.append(df)

df = pd.concat(df_list, ignore_index=True)

# Display basic info
print("Original Data Shape:", df.shape)
print("Missing Values Per Column:\n", df.isnull().sum())

# ----- Handle Missing Values -----
# Fill missing temperature/humidity with the median
df["Temperature"].fillna(df["Temperature"].median(), inplace=True)
df["Humidity"].fillna(df["Humidity"].median(), inplace=True)

# Drop rows with critical missing values (e.g., missing species name)
df.dropna(subset=["Common_Name", "Scientific_Name"], inplace=True)

# Fix Date column
df["Date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=False).dt.strftime("%Y-%m-%d")

# Fix Year column
df["Year"] = pd.to_datetime(df["Date"], errors="coerce").dt.year

# Ensure Year is an integer (remove commas if they exist)
df["Year"] = df["Year"].astype(str).str.replace(",", "").astype(int)

# ----- Convert Data Types -----
df["Temperature"] = df["Temperature"].astype(float)
df["Humidity"] = df["Humidity"].astype(float)

# Convert categorical values to lowercase for consistency
categorical_columns = ["Location_Type", "Sky", "Wind", "Disturbance"]
for col in categorical_columns:
    df[col] = df[col].astype(str).str.lower().str.strip()

# ----- Filter Relevant Columns -----
columns_to_keep = [
    "Admin_Unit_Code", "Sub_Unit_Code", "Site_Name", "Plot_Name", "Location_Type",
    "Year", "Date", "Start_Time", "End_Time", "Observer", "Visit", "Interval_Length",
    "ID_Method", "Distance", "Flyover_Observed", "Sex", "Common_Name", "Scientific_Name",
    "AcceptedTSN", "NPSTaxonCode", "AOU_Code", "PIF_Watchlist_Status",
    "Regional_Stewardship_Status", "Temperature", "Humidity", "Sky", "Wind",
    "Disturbance", "Initial_Three_Min_Cnt"
]

df = df[columns_to_keep]

# ----- Save Cleaned Data -----
output_path = "Cleaned_Bird_Observation_Data.xlsx"
df.to_excel(output_path, index=False)
print(f"Cleaned dataset saved as {output_path}")

# ----- Summary -----
print("Cleaned Data Shape:", df.shape)
print("Sample Data:\n", df.head())

# Save to MySQL database
df.to_sql("BirdObservationsForest", con=engine, if_exists="replace", index=False)

print("Data saved successfully to MySQL database!")

# ***************************************************************************************

# Birds Grassland Observation

# File Path

file_path_g = 'D:\Guvi Projects\Mini Project - 2\Bird_Monitoring_Data_GRASSLAND.XLSX'

# Load all sheets into a dictionary of DataFrames

sheets_g = pd.read_excel(file_path_g,sheet_name=None)

# Combine all sheet into single dataframe

df_list_g = []

for sheet_name_g, df_g in sheets_g.items():
    df_g["Admin_Unit_Code"] = sheet_name_g  
    df_list_g.append(df_g)

df_g = pd.concat(df_list_g, ignore_index=True)

# ----- Handle Missing Values -----
# Fill missing temperature/humidity with the median
df_g["Temperature"].fillna(df_g["Temperature"].median(), inplace=True)
df_g["Humidity"].fillna(df_g["Humidity"].median(), inplace=True)

# Drop rows with critical missing values (e.g., missing species name)
df_g.dropna(subset=["Common_Name", "Scientific_Name"], inplace=True)

# Fix Date column
df_g["Date"] = pd.to_datetime(df_g["Date"], errors="coerce", dayfirst=False).dt.strftime("%Y-%m-%d")

# Fix Year column
df_g["Year"] = pd.to_datetime(df_g["Date"], errors="coerce").dt.year

# Ensure Year is an integer (remove commas if they exist)
df_g["Year"] = df_g["Year"].astype(str).str.replace(",", "").astype(int)

# ----- Convert Data Types -----
df_g["Temperature"] = df_g["Temperature"].astype(float)
df_g["Humidity"] = df_g["Humidity"].astype(float)

# Convert categorical values to lowercase for consistency
categorical_columns = ["Location_Type", "Sky", "Wind", "Disturbance"]
for col in categorical_columns:
    df_g[col] = df_g[col].astype(str).str.lower().str.strip()

# ----- Filter Relevant Columns -----
columns_to_keep_g = [
    "Admin_Unit_Code", "Sub_Unit_Code", "Plot_Name", "Location_Type",
    "Year", "Date", "Start_Time", "End_Time", "Observer", "Visit", "Interval_Length",
    "ID_Method", "Distance", "Flyover_Observed", "Sex", "Common_Name", "Scientific_Name",
    "AcceptedTSN", "AOU_Code", "PIF_Watchlist_Status",
    "Regional_Stewardship_Status", "Temperature", "Humidity", "Sky", "Wind",
    "Disturbance", "Initial_Three_Min_Cnt"
]

df_g = df_g[columns_to_keep_g]

# ----- Save Cleaned Data -----
output_path_g = "Cleaned_Bird_Observation_Grassland_Data.xlsx"
df_g.to_excel(output_path_g, index=False)
print(f"Cleaned dataset saved as {output_path_g}")

# Save to MySQL database
df_g.to_sql("BirdObservationGrassland", con=engine, if_exists="replace", index=False)

print("Data saved successfully to MySQL database!")



