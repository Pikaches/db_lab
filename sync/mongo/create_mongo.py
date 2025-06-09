import psycopg2
from pymongo import MongoClient
# from env import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER

DB_NAME = "postgres_db"
DB_USER = "postgres_user"
DB_PASSWORD = "postgres_password"
DB_HOST = "localhost"
DB_PORT = "5430"


def sync_postgres_to_mongo(mongo_uri='mongodb://localhost:27017/', db_name='university_db'):
    """
    Synchronize data from PostgreSQL to MongoDB with the specified schema

    Args:
        pg_conn_params (dict): PostgreSQL connection parameters
        mongo_uri (str): MongoDB connection URI
        db_name (str): Name of the MongoDB database
    """

    pg_conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    pg_cur = pg_conn.cursor()

    mongo_client = MongoClient(mongo_uri,  username='admin', password='secret')
    mongo_db = mongo_client[db_name]

    mongo_db.drop_collection('lecture_materials')
    mongo_db.create_collection('lecture_materials', validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['material_id', 'file_path', 'type', 'uploaded_at', 'session_id'],
            'properties': {
                'material_id': {'bsonType': 'int'},
                'file_path': {'bsonType': 'string'},
                'type': {'bsonType': 'string'},
                'uploaded_at': {'bsonType': 'date'},
                'session_id': {'bsonType': 'int'},
            }
        }
    })

    lecture_materials_col = mongo_db['lecture_materials']

    try:
        pg_cur.execute(
            "SELECT material_id, session_id, file_path, type, uploaded_at FROM Lecture_Materials")
        materials = pg_cur.fetchall()

        for mat_id, sess_id, file_path, type, uploaded_at in materials:
            lectures_doc = {
                'material_id': mat_id,
                'session_id': sess_id,
                'file_path': file_path,
                'type': type,
                'uploaded_at': uploaded_at
            }

            lecture_materials_col.insert_one(lectures_doc)

        print(
            f"Успешно синхронизировано {len(materials)} LectureMaterials to MongoDB")

    except Exception as e:
        print(f"Error during synchronization: {e}")
    finally:
        pg_cur.close()
        pg_conn.close()
        mongo_client.close()


def main():
    sync_postgres_to_mongo()


if __name__ == "__main__":
    main()
