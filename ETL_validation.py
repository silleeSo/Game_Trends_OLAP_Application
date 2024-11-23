import pandas as pd
import mysql.connector
from mysql.connector import Error  # Import the Error class to handle exceptions

try:
    connection = mysql.connector.connect(
        host='localhost',
        port=3300, 
        database='MCO1_games_datawarehouse',
        user='root',
        password='12345678'
    )

    def count_column_ones(table_name, columns, connection):
        cursor = connection.cursor()
        counts = {}
        for column in columns:
            query = f"SELECT COUNT(*) FROM {table_name} WHERE `{column}` = 1"
            cursor.execute(query)
            count = cursor.fetchone()[0]
            counts[column] = count
        cursor.close()  # Close the cursor after use
        return counts

    if connection.is_connected():
        print("Counting '1s' in dim_genre_set columns:")
        
        # Assuming 'normalized_genres' is defined somewhere before this point
        normalized_genres = []  # Define this list with your actual columns
        genre_one_counts = count_column_ones("dim_genre_set", normalized_genres, connection)
        for genre, count in genre_one_counts.items():
            print(f"{genre}: {count}")

        print("\nCounting '1s' in dim_category_set columns:")
        
        # Assuming 'normalized_categories' is defined somewhere before this point
        normalized_categories = []  # Define this list with your actual columns
        category_one_counts = count_column_ones("dim_category_set", normalized_categories, connection)
        for category, count in category_one_counts.items():
            print(f"{category}: {count}")

        # Assuming 'df' is defined somewhere before this point
        df = pd.DataFrame()  # Define or load your DataFrame here
        print("\nDataFrame Info:")
        print(df.info())

except Error as e:
    print("Error while connecting to MySQL:", e)

finally:
    if connection.is_connected():
        connection.close()
        print("MySQL connection is closed.")
