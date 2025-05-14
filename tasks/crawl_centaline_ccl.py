import requests
import os
import pandas as pd
from src.PostgresConnector import PostgresConnector

import dotenv
dotenv.load_dotenv()
from datetime import datetime, timedelta


__INDEX_TYPE = (
    "CCL_INDEX", # CCL 中原城市領先指數
    "CCL_LU", # CCL 中原城市(大型單位)領先指數
    "CCL_SMU", # CCL 中原城市(中小型單位)領先指數
    "CCL_MASS_MASSEST", # CCL 中原城市大型屋苑領先指數
    "CCL_MASS_HK", # CCL 中原城市大型屋苑領先指數(港島)
    "CCL_MASS_KLN", # CCL 中原城市大型屋苑領先指數(九龍)
    "CCL_MASS_NTE", # CCL 中原城市大型屋苑領先指數(新界東)
    "CCL_MASS_NTW", # CCL 中原城市大型屋苑領先指數(新界西)
)

conn = PostgresConnector(
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT")
)

end_date = datetime.now()
period = 365

if __name__ == "__main__":
    for index_type in __INDEX_TYPE:

        start_date_str = (end_date - timedelta(period)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        end_date_str = end_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        print(start_date_str, end_date_str, index_type)

        # Define the URL and payload
        url = "https://hk.centanet.com/CCI/api/Index/HistoricalDataSearch"
        payload = {
            "DateTimeRange": {
                "Start": start_date_str,
                "End": end_date_str
            },
            "IndexType": index_type,
            "Offset": 0,
            "Size": 30
        }

        # Make the POST request
        response = requests.post(url, json=payload)

        # Check the response status
        if response.status_code == 200:
            print("Request successful!")
            data = response.json()
            
            # Convert the data to a pandas DataFrame
            data_list = data.get("data", [])
            df = pd.DataFrame(data_list)
            
            # Expand the dateRange field into separate columns
            df["start_date"] = df["dateRange"].apply(lambda x: x.get("start") if isinstance(x, dict) else None)
            df["end_date"] = df["dateRange"].apply(lambda x: x.get("end") if isinstance(x, dict) else None)
            df.drop(columns=["dateRange"], inplace=True)
            
            # Add the index_type as a column
            df["index_type"] = index_type

            print(df.head())
            
            # Save the DataFrame to PostgreSQL
            conn.write_data(df, "centaline_ccl_index", 
                    if_exists="upsert", 
                    conflict_columns=["index_type", "publishDate"],
                    index=False)
            print(f"Data saved to table 'centaline_ccl_index' for index type {index_type}.")
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(response.text)

