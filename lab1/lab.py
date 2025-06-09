
from neo4j import GraphDatabase
from typing import List, Dict
import psycopg2
from typing import List, Dict, Optional

from lecture_session import LectureMaterialSearcher

DB_NAME = "postgres_db"
DB_USER = "postgres_user"
DB_PASSWORD = "postgres_password"
DB_HOST = "localhost"
DB_PORT = "5430"


class AttendanceFinder:
    def __init__(
        self,
        uri: str = 'bolt://localhost:7687',
        user: str = 'neo4j',
        password: str = 'strongpassword'
    ):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def get_schedule(self, session_id: int, start_date: str, end_date: str) -> List[tuple]:
        """Получает расписание для указанной сессии в заданном временном промежутке"""
        query = """
            SELECT 
                sch.schedule_id,
                sch.group_id,
                sch.room,
                sch.scheduled_date,
                sch.start_time,
                ls.topic,
                ls.duration_minutes
            FROM Schedule sch
            JOIN Lecture_Sessions ls ON sch.session_id = ls.session_id
            WHERE 
                sch.session_id = %s
                AND sch.scheduled_date BETWEEN %s AND %s
            ORDER BY sch.scheduled_date, sch.start_time
        """
        params = (session_id, start_date, end_date)

        try:
            cur = self.conn.cursor()
            cur.execute(query, params)
            return cur.fetchall()
        except Exception as e:
            print(f"Error getting schedule: {e}")
            return []

    def find_worst_attendees(
        self,
        lecture_ids: List[int],
        top_n: int = 10,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Dict]:
        return self._find_attendance(
            lecture_ids,
            limit=top_n,
            worst=True,
            start_date=start_date,
            end_date=end_date
        )

    def get_attendance_summary(
        self,
        lecture_ids: List[int],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Dict]:
        return self._find_attendance(
            lecture_ids,
            limit=None,
            worst=False,
            start_date=start_date,
            end_date=end_date
        )

    def _find_attendance(
        self,
        lecture_ids: List[int],
        limit: Optional[int] = None,
        worst: bool = True,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict]:
        if not lecture_ids:
            return []

        session_ids = []
        for lect_id in lecture_ids:
            sessions = self.get_schedule(lect_id, start_date, end_date)
            for session_id in sessions:
                session_ids.append(session_id)

        # Запрос к PostgreSQL для посещаемости
        query = """
        SELECT a.student_id, sch.group_id,
               COUNT(a.attendance_id) AS total_count,
               SUM(CASE WHEN a.attended THEN 1 ELSE 0 END) AS attended_count,
               ROUND((SUM(CASE WHEN a.attended THEN 1 ELSE 0 END)::FLOAT / 
                      NULLIF(COUNT(a.attendance_id), 0)) * 100) AS attendance_percent
        FROM Schedule sch
        LEFT JOIN Attendance a ON sch.schedule_id = a.schedule_id
        WHERE sch.session_id = ANY(%s)
        """
        params = [session_ids]
        # if start_date and end_date:
        #     query += " AND sch.scheduled_date BETWEEN %s AND %s"
        #     params.extend([start_date, end_date])
        query += """
        GROUP BY a.student_id, sch.group_id
        HAVING COUNT(a.attendance_id) > 0
        """
        if worst:
            query += " ORDER BY attendance_percent ASC"
        else:
            query += " ORDER BY a.student_id ASC"
        if limit:
            query += f" LIMIT {limit}"

        try:
            conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT
            )
            cur = conn.cursor()
            cur.execute(query, params)
            rows = cur.fetchall()
            print(
                f"Found {len(rows)} attendance records from PostgreSQL")
            if not rows:
                return []

            # Получение данных из Neo4j
            student_ids = [item[0] for item in rows]
            group_ids = [item[1] for item in rows]

            with self.driver.session() as session:
                result = session.run("""
                    MATCH (s:Student)
                    WHERE s.postgres_id IN $student_ids
                    OPTIONAL MATCH (s)-[:MEMBER_OF]->(g:StudentGroup)
                    WHERE g.postgres_id IN $group_ids
                    RETURN s.postgres_id AS student_id, s.name AS name, 
                           s.enrollment_year AS enrollment_year, s.date_of_birth AS date_of_birth,
                           s.email AS email, s.book_number AS book_number,
                           g.postgres_id AS group_id, g.name AS group_name
                """, student_ids=student_ids, group_ids=group_ids)

                neo4j_data = {r['student_id']: r for r in result.data()}

            return [{
                'studentId': row[0],
                'studentName': neo4j_data.get(row[0], {}).get('name'),
                'attendedCount': row[3],
                'totalCount': row[2],
                'attendancePercent': row[4],
                'enrollment_year': neo4j_data.get(row[0], {}).get('enrollment_year'),
                'date_of_birth': neo4j_data.get(row[0], {}).get('date_of_birth'),
                'email': neo4j_data.get(row[0], {}).get('email'),
                'book_number': neo4j_data.get(row[0], {}).get('book_number'),
                'group_id': neo4j_data.get(row[0], {}).get('group_id'),
                'group_name': neo4j_data.get(row[0], {}).get('group_name')
            } for row in rows]

        except Exception as e:
            print(f"Error finding attendance: {e}")
            return []


if __name__ == '__main__':
    term = "физика"
    searcher = LectureMaterialSearcher()
    lecture_ids = searcher.search_by_course_and_session_type(term, '1')
    print(f"Найдены лекции: {lecture_ids}")

    finder = AttendanceFinder()

    start = "2023-05-01"
    end = "2023-12-31"

    try:
        worst = finder.find_worst_attendees(lecture_ids,
                                            top_n=10, start_date=start, end_date=end)
        print(worst)
        if worst:
            print("\n10 студентов с худшей посещаемостью:")
            for idx, rec in enumerate(worst, 1):
                # redis_info = r.hgetall(f"student:{rec['studentId']}")
                # info_str = f"[Redis] Name: {redis_info.get('name')}, Age: {redis_info.get('age')}, Mail: {redis_info.get('mail')}, Group: {redis_info.get('group')}"
                print(
                    f"{idx}. {rec['studentName']} — {rec['attendancePercent']}% ({rec['attendedCount']}/{rec['totalCount']})")
        else:
            print("Нет данных о посещаемости.")

        # summary = finder.get_attendance_summary(
        #     session_ids, start_date=start, end_date=end)
        # if summary:
        #     print("\nСводка посещаемости:")
        #     for rec in summary:
        #         redis_info = r.hgetall(f"student:{rec['studentId']}")
        #         info_str = f"[Redis] Name: {redis_info.get('name')}, Age: {redis_info.get('age')}, Mail: {redis_info.get('mail')}, Group: {redis_info.get('group')}"
        #         print(
        #             f"{rec['studentName']}: {rec['attendancePercent']}% ({rec['attendedCount']}/{rec['totalCount']}) {info_str}")
        # else:
        #     print("Нет данных для сводки.")

    finally:
        finder.close()
