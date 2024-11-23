import pandas as pd
import mysql.connector
from mysql.connector import Error
import re
from datetime import datetime
import decimal
from datetime import date

# Load data from CSV file
csv_file_path = 'final_cleaned.csv'
df = pd.read_csv(csv_file_path, sep=';', encoding='latin1')

# Drop rows with missing data in important columns
df = df.dropna(subset=['user_score', 'genres', 'categories'])

# Clean user_score: remove non-numeric values and ensure no nulls
def clean_user_score(value):
    if pd.isna(value) or value > 100:  # Cap user_score at 100
        return 100
    return float(value) if isinstance(value, (int, float)) else None

# Clean playtime columns: convert to integers and handle non-numeric values
def clean_playtime(value):
    return 0 if pd.isna(value) else int(value)  # No nulls allowed

# Parse date format to YYYY-MM-DD for MySQL
def parse_date(value):
    try:
        return datetime.strptime(value, '%Y-%m-%d').strftime('%Y-%m-%d')
    except ValueError:
        try:
            return datetime.strptime(value, '%b %d, %Y').strftime('%Y-%m-%d')
        except ValueError:
            return None

# Apply cleaning functions
df['user_score'] = df['user_score'].apply(clean_user_score)
df['average_playtime_forever'] = df['average_playtime_forever'].apply(clean_playtime)
df['average_playtime_2weeks'] = df['average_playtime_2weeks'].apply(clean_playtime)
df['median_playtime_forever'] = df['median_playtime_forever'].apply(clean_playtime)
df['median_playtime_2weeks'] = df['median_playtime_2weeks'].apply(clean_playtime)
df['release_date'] = df['release_date'].apply(parse_date)

# Drop rows where release_date is None
df = df.dropna(subset=['release_date'])

# Normalize genres and categories
def normalize_name(name):
    return re.sub(r'[^a-zA-Z0-9_]', '_', name.strip().lower())

def get_unique_entries(df, column):
    unique_entries = set()
    for entry in df[column].dropna():
        entries = entry.split(',')
        for item in entries:
            unique_entries.add(item.strip())
    return list(unique_entries)

# Extract and normalize unique genres and categories
unique_genres = get_unique_entries(df, 'genres')
unique_categories = get_unique_entries(df, 'categories')

# Normalize and check for duplicates
def normalize_and_check_duplicates(names):
    normalized = [normalize_name(name) for name in names]
    duplicates = {x for x in normalized if normalized.count(x) > 1}
    if duplicates:
        print(f"Found duplicates: {duplicates}")
        normalized = list(dict.fromkeys(normalized))  # Remove duplicates
    return normalized

normalized_genres = sorted(normalize_and_check_duplicates(unique_genres))
normalized_categories = sorted(normalize_and_check_duplicates(unique_categories))

# Synchronize IDs across dim tables (same ID for each game)
try:
    connection = mysql.connector.connect(
        host='localhost',
        port=3300, 
        database='MCO1_games_datawarehouse',
        user='root',
        password='12345678'
    )

    if connection.is_connected():
        cursor = connection.cursor()

        # Drop tables if they exist
        cursor.execute("SET FOREIGN_KEY_CHECKS=0")
        cursor.execute("DROP TABLE IF EXISTS fact_games")
        cursor.execute("DROP TABLE IF EXISTS dim_genre_set")
        cursor.execute("DROP TABLE IF EXISTS dim_category_set")
        cursor.execute("SET FOREIGN_KEY_CHECKS=1")

        # Create dim_genre_set table
        genre_columns = ', '.join([f"`{genre}` TINYINT NOT NULL DEFAULT 0" for genre in normalized_genres])
        cursor.execute(f"""CREATE TABLE dim_genre_set (
            id INT PRIMARY KEY AUTO_INCREMENT,
            {genre_columns}
        )""")

        # Create dim_category_set table
        category_columns = ', '.join([f"`{category}` TINYINT NOT NULL DEFAULT 0" for category in normalized_categories])
        cursor.execute(f"""CREATE TABLE dim_category_set (
            id INT PRIMARY KEY AUTO_INCREMENT,
            {category_columns}
        )""")

        # Create fact_games table
        cursor.execute("""CREATE TABLE fact_games (
            id INT PRIMARY KEY auto_increment,
            category_id INT,
            genre_id INT,
            release_date DATE,
            price DECIMAL(10, 2),
            positive_reviews INT,
            negative_reviews INT,
            user_score DECIMAL(5,2),
            metacritic_score INT,
            average_playtime_forever INT,
            average_playtime_2weeks INT,
            median_playtime_forever INT,
            median_playtime_2weeks INT,
            FOREIGN KEY (category_id) REFERENCES dim_category_set(id),
            FOREIGN KEY (genre_id) REFERENCES dim_genre_set(id)
        )""")

        # Insert into dim_genre_set and dim_category_set, keeping IDs in sync
        for idx, row in df.iterrows():
            genre_flags = {}
            genres_in_row = row['genres'].split(',')
            normalized_genres_in_row = sorted(normalize_and_check_duplicates(genres_in_row))
            for genre in normalized_genres:
                genre_flags[genre] = 1 if genre in normalized_genres_in_row else 0
            genre_values = ', '.join(str(genre_flags[genre]) for genre in normalized_genres)

            category_flags = {}
            categories_in_row = row['categories'].split(',')
            normalized_categories_in_row = sorted(normalize_and_check_duplicates(categories_in_row))
            for category in normalized_categories:
                category_flags[category] = 1 if category in normalized_categories_in_row else 0
            category_values = ', '.join(str(category_flags[category]) for category in normalized_categories)
            if row['release_date'] != None:
                cursor.execute(f"INSERT INTO dim_genre_set (id, {', '.join(f'`{genre}`' for genre in normalized_genres)}) VALUES ({idx+1}, {genre_values})")
                cursor.execute(f"INSERT INTO dim_category_set (id, {', '.join(f'`{category}`' for category in normalized_categories)}) VALUES ({idx+1}, {category_values})")

                # Insert into fact_games
                cursor.execute("""INSERT INTO fact_games
                                (category_id, genre_id, release_date, price, positive_reviews, negative_reviews,
                                user_score, metacritic_score, average_playtime_forever, average_playtime_2weeks,
                                median_playtime_forever, median_playtime_2weeks)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                            (idx+1, idx+1, row['release_date'], row['price'], row['positive_reviews'],
                                row['negative_reviews'], row['user_score'], row['metacritic_score'],
                                row['average_playtime_forever'], row['average_playtime_2weeks'],
                                row['median_playtime_forever'], row['median_playtime_2weeks']))

        # Commit the transaction
        connection.commit()
        print("Data inserted successfully.")

        # AUTOMATE SQL COMMANDS HERE

        # Add new columns for release year and release month
        cursor.execute("""
            ALTER TABLE fact_games
            ADD COLUMN release_year INT,
            ADD COLUMN release_month INT;
        """)

        # Update release_year and release_month based on release_date
        cursor.execute("""
            UPDATE fact_games
            SET release_year = YEAR(release_date),
                release_month = MONTH(release_date);
        """)

        # Create indexes on fact_games
        cursor.execute("""
            CREATE INDEX idx_games_user_playtime_id ON fact_games (user_score, average_playtime_forever, id);
        """)
        cursor.execute("""
            CREATE INDEX idx_games_release_year_month ON fact_games (release_year, release_month);
        """)

        # Create indexes on all columns in dim_genre_set
        for genre in normalized_genres:
            index_name = f"idx_genre_{genre}"
            cursor.execute(f"CREATE INDEX {index_name} ON dim_genre_set (`{genre}`)")

        # Create indexes on all columns in dim_category_set
        for category in normalized_categories:
            index_name = f"idx_category_{category}"
            cursor.execute(f"CREATE INDEX {index_name} ON dim_category_set (`{category}`)")

        # Commit these changes to the database
        connection.commit()
        print("Columns added, updated, and indexes created successfully.")



    # Exclude rows where release_date is None
    df_filtered = df.dropna(subset=['release_date'])
    # Convert DataFrame columns to match database types
    df_filtered['release_date'] = pd.to_datetime(df_filtered['release_date']).dt.strftime('%Y-%m-%d')

    # Validation: Query the tables and compare with the filtered DataFrame
    print("\nValidating data insertion...")

    # Query fact_games table
    cursor.execute("SELECT id, release_date, price, positive_reviews, negative_reviews, user_score, "
                "metacritic_score, average_playtime_forever, average_playtime_2weeks, "
                "median_playtime_forever, median_playtime_2weeks FROM fact_games")
    fact_games_records = cursor.fetchall()

    # Helper function to convert values for comparison
    def convert_db_value(value):
        if isinstance(value, decimal.Decimal):
            return float(value)
        elif isinstance(value, date):
            return value.strftime('%Y-%m-%d')
        else:
            return value

    # Flag to check if all rows match
    all_match = True
    i = 0
    # Compare each row from the database to the corresponding row in the filtered DataFrame
    for db_row in fact_games_records:
        row_id = db_row[0] - 1  # Convert 1-based index to 0-based
        df_row = df_filtered.iloc[row_id]
        i = i+1
        db_data = (
            convert_db_value(db_row[1]),  # release_date as string
            convert_db_value(db_row[2]),  # price as float
            int(db_row[3]),               # positive_reviews as integer
            int(db_row[4]),               # negative_reviews as integer
            convert_db_value(db_row[5]),  # user_score as float
            int(db_row[6]),               # metacritic_score as integer
            int(db_row[7]),               # average_playtime_forever as integer
            int(db_row[8]),               # average_playtime_2weeks as integer
            int(db_row[9]),               # median_playtime_forever as integer
            int(db_row[10])               # median_playtime_2weeks as integer
        )
        
        df_data = (
            df_row['release_date'],       # release_date as string
            round(float(df_row['price']), 2),       # price as float
            int(df_row['positive_reviews']),  # positive_reviews as integer
            int(df_row['negative_reviews']),  # negative_reviews as integer
            float(df_row['user_score']),  # user_score as float
            int(df_row['metacritic_score']),  # metacritic_score as integer
            int(df_row['average_playtime_forever']),  # average_playtime_forever as integer
            int(df_row['average_playtime_2weeks']),   # average_playtime_2weeks as integer
            int(df_row['median_playtime_forever']),   # median_playtime_forever as integer
            int(df_row['median_playtime_2weeks'])     # median_playtime_2weeks as integer
        )

        if db_data != df_data:
            print(f"Mismatch found in row {row_id + 1}:")
            print(f"Database: {db_data}")
            print(f"DataFrame: {df_data}")
            # Flag to check if all rows match
            all_match = False
            i = i-1
    # Print summary statement
    if all_match:
        print(f"All {i} rows match successfully.")
    else:
        print("Some rows do not match.")

    # Check for NULL values in the inserted data
    cursor.execute("SELECT COUNT(*) FROM fact_games WHERE release_date IS NULL OR price IS NULL OR "
                "positive_reviews IS NULL OR negative_reviews IS NULL OR user_score IS NULL OR "
                "metacritic_score IS NULL OR average_playtime_forever IS NULL OR "
                "average_playtime_2weeks IS NULL OR median_playtime_forever IS NULL OR "
                "median_playtime_2weeks IS NULL")
    null_counts = cursor.fetchone()[0]

    if null_counts > 0:
        print(f"There are {null_counts} NULL values in the database.")
    else:
        print("No NULL values found in the database.")

    # Check for duplicates in the fact_games table including foreign keys
    cursor.execute("""
        SELECT COUNT(*) 
        FROM fact_games 
        GROUP BY category_id, genre_id, release_date, price, positive_reviews, negative_reviews, 
        user_score, metacritic_score, average_playtime_forever, average_playtime_2weeks, 
        median_playtime_forever, median_playtime_2weeks 
        HAVING COUNT(*) > 1
    """)
    duplicate_counts = cursor.fetchall()

    if duplicate_counts:
        print(f"Found {len(duplicate_counts)} duplicate entries in the database.")
    else:
        print("No duplicate entries found in the database.")

    





    def count_column_ones(table_name, columns, connection):
        cursor = connection.cursor()
        counts = {}
        for column in columns:
            query = f"SELECT COUNT(*) FROM {table_name} WHERE `{column}` = 1"
            cursor.execute(query)
            count = cursor.fetchone()[0]
            counts[column] = count
        return counts

    if connection.is_connected():
        print("Counting '1s' in dim_genre_set columns:")
        genre_one_counts = count_column_ones("dim_genre_set", normalized_genres, connection)
        for genre, count in genre_one_counts.items():
            print(f"{genre}: {count}")

        print("\nCounting '1s' in dim_category_set columns:")
        category_one_counts = count_column_ones("dim_category_set", normalized_categories, connection)
        for category, count in category_one_counts.items():
            print(f"{category}: {count}")

        print("\nDataFrame Info:")
        print(df.info())
    



    


except Error as e:
    print("Error while connecting to MySQL", e)

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed.")
