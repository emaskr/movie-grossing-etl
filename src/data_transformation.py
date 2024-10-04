import pandas as pd
import os

# Configuration
INPUT_FILE = '../movie-grossing-etl/data/raw/random_movie_grossing_data.csv'
OUTPUT_DIR = '../movie-grossing-etl/data/processed/'  # Directory to save the processed data
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'transformed_movie_data.csv')

# Ensure the output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def transform_data():
    """Load, clean, and transform movie data."""
    # Load the data
    df = pd.read_csv(INPUT_FILE)

    # Display the first few rows before transformation
    print("Original Data:")
    print(df.head())

    # Data Cleaning: Remove duplicates if any
    df.drop_duplicates(inplace=True)

    # Convert budget and revenue to numeric (in case they are strings)
    df['budget'] = pd.to_numeric(df['budget'], errors='coerce')
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')

    # Calculate profit (revenue - budget)
    df['profit'] = df['revenue'] - df['budget']

    # Handle missing values
    df.fillna({'budget': 0, 'revenue': 0, 'profit': 0}, inplace=True)

    # Filter out movies with zero budget and revenue, if not valid
    df = df[(df['budget'] > 0) | (df['revenue'] > 0)]

    # Extract the year from release_date and create a new column
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
    df['release_year'] = df['release_date'].dt.year

    # Create a flag for profitable movies
    df['is_profitable'] = df['profit'] > 0

    # Drop unnecessary columns (if any, customize this as needed)
    df.drop(columns=['overview'], inplace=True, errors='ignore')  # Example: dropping 'overview'

    # Display the transformed data
    print("Transformed Data:")
    print(df.head())

    # Save the transformed data to a new CSV file
    df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')

    print(f"Transformed data saved to '{OUTPUT_FILE}'.")

if __name__ == '__main__':
    transform_data()
