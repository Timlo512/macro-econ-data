from src.PostgresConnector import PostgresConnector

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
import dotenv
dotenv.load_dotenv()

conn = PostgresConnector(
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT")
)

def fetch_ib_margin_page():
    url = "https://www.interactivebrokers.com.hk/en/trading/margin-rates.php"
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching page: {e}")
        return None

def extract_limits(tier_str):
    if tier_str.strip() == 'All':
        return (0, float('inf'))
    
    tier_str = tier_str.replace(',', '')
    
    if '≤' in tier_str:
        parts = tier_str.split('≤')
        if len(parts) == 2:
            return (float(parts[0].strip()), float(parts[1].strip()))
        else:
            return (float(parts[0].strip()), float(parts[1].strip()))
    elif '>' in tier_str:
        limit = float(tier_str.replace('>', '').strip())
        return (limit, float('inf'))
    
    return (None, None)

def extract_rate(rate_str):
    match = re.search(r'([\d.]+)%', rate_str)
    if match:
        return float(match.group(1))
    return None

def parse_margin_table(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.find('table')
    
    # Extract headers
    headers = []
    for th in table.find('thead').find_all('th'):
        headers.append(th.text.strip())
    
    # Extract rows
    rows = []
    for tr in table.find('tbody').find_all('tr'):
        row = []
        for td in tr.find_all('td'):
            row.append(td.text.strip())
        rows.append(row)
    
    # Create DataFrame
    df = pd.DataFrame(rows, columns=headers)
    
    # Add limit columns
    df['Lower_Limit'] = None
    df['Upper_Limit'] = None
    
    # Process limits
    for idx, row in df.iterrows():
        lower, upper = extract_limits(row['Tier'])
        df.at[idx, 'Lower_Limit'] = lower
        df.at[idx, 'Upper_Limit'] = upper
    
    # Extract rate numbers
    df['Rate_pct'] = df['Rate Charged:'].apply(extract_rate)
    
    return df

def process_dataframe(df):
    # Forward fill the Currency column to propagate values down
    current_currency = None
    processed_rows = []
    
    for idx, row in df.iterrows():
        # If Currency is not empty, update current_currency
        if row['Currency'] and not pd.isna(row['Currency']) and row['Currency'].strip():
            current_currency = row['Currency'].strip()
        
        # Create new row with current currency
        new_row = row.copy()
        new_row['Currency'] = current_currency
        processed_rows.append(new_row)
    
    # Create new DataFrame with processed rows
    processed_df = pd.DataFrame(processed_rows)
    
    # Rename columns to be PostgreSQL compatible
    column_mapping = {
        'Currency': 'currency',
        'Tier': 'tier',
        'Rate Charged:': 'rate_charged',
        'Lower_Limit': 'lower_limit',
        'Upper_Limit': 'upper_limit',
        'Rate_pct': 'rate_pct'
    }
    
    processed_df = processed_df.rename(columns=column_mapping)
    return processed_df

def main():
    # Fetch HTML content
    html_content = fetch_ib_margin_page()
    
    if html_content:
        # Parse the table and create DataFrame
        df = parse_margin_table(html_content)
        
        # Process DataFrame to fill currencies
        df = process_dataframe(df)
        
        # Save to CSV
        df.to_csv('ib_margin_rates.csv', index=False)
        print("Data saved to 'ib_margin_rates.csv'")
        
        # Load to postgres database
        conn.write_data(df, "market_data.ib_margin_rates", 
                        if_exists="upsert", 
                        conflict_columns=["currency", "tier"], 
                        index=False)


        return df
    else:
        print("Failed to fetch data")
        return None

if __name__ == "__main__":
    df = main()
    if df is not None:
        print("\nFirst few rows of the data:")
        print(df.head())
