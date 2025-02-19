import os
import dotenv
import psycopg2
import json
import unidecode

dotenv.load_dotenv()

# Conexión a la base de datos
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)

c = conn.cursor()

def replace_https_with_http(url):
    if url.startswith("https://"):
        return url.replace("https://", "http://")
    return url


def get_fields_of_all_found_people():
    query = """
        SELECT name, experience, position, url, id_company, fecha_insert, city, country_code
        FROM roket.linkedin_people
        WHERE status = 'FOUND';
    """
    try:
        c.execute(query)
        print("Query executed successfully")
        results = c.fetchall()
        all_people_fields = []

        for result in results:
            experience = json.loads(json.dumps(result[1]))
            current_titles = [exp['title'] for exp in experience if exp.get('end_date') == 'Present' and 'title' in exp]
            with open('country_codes.json', 'r') as f:
                country_codes = json.load(f)
            
            country_name = next((item['name'] for item in country_codes if item['code'] == result[7]), None)
            
            name_parts = result[0].split(' ', 1)
            first_name = name_parts[0]
            last_name = name_parts[1] if len(name_parts) > 1 else ''
            
            last_name = ' '.join(name_parts[1:]).strip()
            with open('comunas-regiones.json', 'r', encoding='utf-8') as f:
                comunas_regiones = json.load(f)
            
            comuna_name = None
            if result[6] and result[7] == 'CL':
                city_parts = result[6].split(',')[0].replace('-', ' ').split()
                for part in city_parts:
                    words = part.split()
                    for i in range(len(words)):
                        normalized_city = unidecode.unidecode(words[i])
                        for region in comunas_regiones['regiones']:
                            if any(normalized_city == unidecode.unidecode(comuna) for comuna in region['comunas']):
                                comuna_name = words[i]
                                break
                        if comuna_name:
                            break
                    if comuna_name:
                        break
                if not comuna_name:
                    for i in range(len(city_parts)):
                        for j in range(i + 1, len(city_parts) + 1):
                            combined_parts = ' '.join(city_parts[i:j])
                            normalized_city = unidecode.unidecode(combined_parts)
                            for region in comunas_regiones['regiones']:
                                if any(normalized_city == unidecode.unidecode(comuna) for comuna in region['comunas']):
                                    comuna_name = combined_parts
                                    break
                            if comuna_name:
                                break
                        if comuna_name:
                            break
            else:
                comuna_name = 'Chile'
            
            fields = {
                "ext_persona_id": None,
                "nombre": unidecode.unidecode(first_name),
                "apellido": unidecode.unidecode(last_name),
                "titulo": current_titles[0] if current_titles else None,
                "headline": unidecode.unidecode(result[2]) if result[2] else None,
                "seniority": None,
                "email": None,
                "email_status": "unguessed",
                "telefono": None,
                "linkedin": replace_https_with_http(result[3]),
                "company_id": result[4],
                "cargo_id": None,
                "roket_status": "ING",
                "fecha_ingreso": result[5].strftime("%Y-%m-%d %H:%M:%S.%f") if result[5] else None,
                "fecha_update": None,
                "state": None,
                "ciudad": unidecode.unidecode(comuna_name) if comuna_name else (unidecode.unidecode(country_name) if country_name else None),
                "pais": country_name,
                "fuera_oficina": None,
                "last_mail": None,
                "ingresado_por": "LinkedIn",
                "parent_company_id": result[4],
                "actualizado_por": "LinkedIn",
            }
            all_people_fields.append(fields)
        
        with open('fields_of_all_found_people.json', 'w') as f:
            json.dump(all_people_fields, f, indent=4)
        print("Data exported to fields_of_all_found_people.json")
    except Exception as e:
        print(f"Error during query execution: {e}")
    finally:
        c.close()
        conn.close()

if __name__ == "__main__":
    get_fields_of_all_found_people()