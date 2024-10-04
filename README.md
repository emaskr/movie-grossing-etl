# movie-grossing-etl

## Overview

This project implements an ETL pipeline to extract, transform, and load movie data into a PostgreSQL database. The data is sourced from The Movie Database (TMDb) API, focusing on the grossing status of movies from various years.

## Project Structure

```
movie-grossing-etl/
│
├── config/
│   └── config.json        # Configuration file for API key and database credentials
│
├── data/
│   ├── raw/               # Folder for raw data CSV files
│   └── processed/         # Folder for processed data CSV files
│
├── env/                   # Virtual environment files
│   ├── include/
│   ├── lib/
│   ├── scripts/
│   └── pyvenv.py
│
└── src/                   # Source code files
    ├── api_retrieval.py   # Script to retrieve movie data from TMDb API
    └── etl_script.py       # Main ETL script to transform and load data into PostgreSQL
```

## Requirements

To install the necessary dependencies, run:

```bash
pip install -r requirements.txt
```

### Dependencies

- **pandas**: For data manipulation and analysis.
- **requests**: For making HTTP requests to the TMDb API.
- **pyspark**: For large-scale data processing.
- **sqlalchemy**: For SQL database integration.
- **psycopg2-binary**: PostgreSQL adapter for Python.

## Setup

### 1. Set Up Virtual Environment

Create a virtual environment to manage your project dependencies:

```bash
python -m venv env
```

Activate the virtual environment:

- On macOS/Linux:
    ```bash
    source env/bin/activate
    ```
- On Windows:
    ```bash
    .\env\Scripts\activate
    ```

### 2. Configure API Key and Database Credentials

Edit the `config/config.json` file to add your TMDb API key and PostgreSQL database credentials.

### 3. Run the ETL Pipeline

To execute the ETL process, run the following command:

```bash
python src/etl_script.py
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
