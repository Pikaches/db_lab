import psycopg2
import redis
from typing import Dict, List

DB_NAME = "postgres_db"
DB_USER = "postgres_user"
DB_PASSWORD = "postgres_password"
DB_HOST = "localhost"
DB_PORT = "5430"


def sync_session_types_to_redis(redis_host: str = 'localhost', redis_port: int = 6379) -> None:
    """Синхронизация типов сессий из PostgreSQL в Redis"""
    print("""Синхронизация типов сессий из PostgreSQL в Redis""")
    pg_conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    pg_cur = pg_conn.cursor()
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

    try:
        for key in r.scan_iter("session_type:*"):
            r.delete(key)
        for key in r.scan_iter("index:session_type:*"):
            r.delete(key)

        pg_cur.execute("""
            SELECT session_type_id, name
            FROM Session_Types
        """)
        session_types = pg_cur.fetchall()

        for session_type_id, name in session_types:
            session_key = f"session_type:{session_type_id}"
            r.hset(session_key, mapping={
                'id': session_type_id,
                'name': name
            })

            # Создание индексов
            r.sadd(f"index:session_type:name:{name.lower()}", session_type_id)

        print(
            f"Успешно синхронизировано {len(session_types)} типов сессий в Redis")

    except Exception as e:
        print(f"Ошибка синхронизации: {e}")
        raise
    finally:
        pg_cur.close()
        pg_conn.close()
        r.close()


class SessionTypeSearch:
    def __init__(self, redis_host='localhost', redis_port=6379):
        self.r = redis.Redis(
            host=redis_host, port=redis_port, decode_responses=True)

    def get_by_id(self, session_type_id: int) -> Dict:
        """Получить тип сессии по ID"""
        return self.r.hgetall(f"session_type:{session_type_id}")

    def get_by_name(self, name: str) -> List[Dict]:
        """Поиск по точному названию типа"""
        session_ids = self.r.smembers(
            f"index:session_type:name:{name.lower()}")
        return [self.r.hgetall(f"session_type:{id}") for id in session_ids]


def main():
    sync_session_types_to_redis()
    searcher = SessionTypeSearch()

    # Примеры использования:
    print("Все лекции:")
    print(searcher.get_by_name("Лекция"))


if __name__ == "__main__":
    main()
