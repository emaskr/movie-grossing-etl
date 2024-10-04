import pandas as pd
import psycopg2  # Import the PostgreSQL library
from sqlalchemy import create_engine

# Configuration
INPUT_FILE = '../movie-grossing-etl/data/raw/random_movie_grossing_data.csv'

# PostgreSQL database configuration
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'movies'  # Replace with your database name
DB_USER = 'yourusername'  # Replace with your PostgreSQL username
DB_PASSWORD = 'yourpassword'  # Replace with your PostgreSQL password

def extract():
    """Extract data from the CSV file."""
    df = pd.read_csv(INPUT_FILE)
    print(f"Extracted {len(df)} rows from {INPUT_FILE}.")
    return df

def transform(df):
    """Transform the movie data."""
    # Data Cleaning and Transformation steps
    df = df.copy()  # Create a copy to avoid SettingWithCopyWarning
    df.drop_duplicates(inplace=True)

    print(f"Data after removing duplicates: {len(df)} rows.")

    # Convert budget and revenue to numeric, coercing errors to NaN
    df['budget'] = pd.to_numeric(df['budget'], errors='coerce')
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')

    # Calculate profit
    df['profit'] = df['revenue'] - df['budget']

    # Fill NaN values for budget, revenue, and profit
    df.fillna({'budget': 0, 'revenue': 0, 'profit': 0}, inplace=True)

    print(f"Data after filling NaN values: {len(df)} rows.")

    # Print a sample of the data before filtering
    print(df[['title', 'budget', 'revenue']].head(20))

    # Filter out rows where both budget and revenue are 0
    df = df[(df['budget'] != 0) | (df['revenue'] != 0)]

    print(f"Data after filtering out 0 budget and revenue: {len(df)} rows.")

    if df.empty:
        print("Warning: The DataFrame is empty after filtering.")
        return df

    # Convert release_date to datetime
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')

    # Extract release year
    df['release_year'] = df['release_date'].dt.year

    # Determine profitability
    df['is_profitable'] = (df['profit'] > 0).astype(int)  # Use 0 or 1 for Boolean

    # Drop the 'overview' column, if it exists
    df.drop(columns=['overview'], inplace=True, errors='ignore')

    print(f"Transformed data to {len(df)} rows.")
    return df


def load(df):
    """Load the transformed data into PostgreSQL database."""
    try:
        # Create connection string for PostgreSQL
        connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        
        # Create SQLAlchemy engine
        engine = create_engine(connection_string)

        # Load data into PostgreSQL database
        df.to_sql('movies', engine, if_exists='replace', index=False)
        print(f"Loaded data into {DB_NAME} database on {DB_HOST}.")
    
    except Exception as e:
        print(f"Error: {e}")

def etl_pipeline():
    """Run the ETL pipeline."""
    df = extract()
    transformed_df = transform(df)
    load(transformed_df)

if __name__ == '__main__':
    etl_pipeline()
