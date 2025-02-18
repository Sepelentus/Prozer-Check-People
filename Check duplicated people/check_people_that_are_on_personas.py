import os
import json
import psycopg2
from urllib.parse import urlparse

# Conexión a la base de datos
try:
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
except psycopg2.Error as e:
    print(f"Error connecting to the database: {e}")
    raise

def get_linkedin_id(linkedin_url):
    path = urlparse(linkedin_url).path
    return path.split('/')[-1]

def check_existing_people(linkedin_ids):
    try:
        with conn.cursor() as cursor:
            query = """
                SELECT linkedin 
                FROM roket.personas 
                WHERE linkedin = ANY(%s)
            """
            cursor.execute(query, (linkedin_ids,))
            results = cursor.fetchall()
        return {row[0] for row in results}
    except psycopg2.Error as e:
        print(f"Error executing query: {e}")
        raise

def separate_people():
    try:
        with open("fields_of_all_found_people.json", "r", encoding="utf-8") as f:
            all_people = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error reading JSON file: {e}")
        raise

    linkedin_ids = [get_linkedin_id(person["linkedin"]) for person in all_people]
    BATCH_SIZE = 1000
    existing_people = set()

    for i in range(0, len(linkedin_ids), BATCH_SIZE):
        print(f"Checking batch {i // BATCH_SIZE + 1} of {len(linkedin_ids) // BATCH_SIZE + 1}...")
        batch_ids = linkedin_ids[i:i + BATCH_SIZE]
        existing_people.update(check_existing_people(batch_ids))

    existing_people_data = [person for person in all_people if get_linkedin_id(person["linkedin"]) in existing_people]
    new_people_data = [person for person in all_people if get_linkedin_id(person["linkedin"]) not in existing_people]

    try:
        with open("existing_people.json", "w", encoding="utf-8") as f:
            json.dump(existing_people_data, f, indent=4)

        with open("new_people.json", "w", encoding="utf-8") as f:
            json.dump(new_people_data, f, indent=4)
    except IOError as e:
        print(f"Error writing JSON file: {e}")
        raise
    
    print(f"Existing people: {len(existing_people_data)}")
    print(f"New people: {len(new_people_data)}")

    print("Data separated into existing_people.json and new_people.json")

if __name__ == "__main__":
    try:
        separate_people()
    finally:
        conn.close()