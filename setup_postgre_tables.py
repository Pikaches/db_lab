import psycopg2
from env import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER


def setup_tables():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()

    try:
        # Таблица Universities
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Universities (
                university_id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                address TEXT,
                founded_date DATE
            )
        """)

    # Таблица Institutes
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Institutes (
                institute_id SERIAL PRIMARY KEY,
                university_id INTEGER REFERENCES Universities(university_id),
                name VARCHAR(255) NOT NULL
            )
        """)

        # Таблица Departments
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Departments (
                department_id SERIAL PRIMARY KEY,
                institute_id INTEGER REFERENCES Institutes(institute_id),
                name VARCHAR(255) NOT NULL
            )
        """)

        # Таблица Specialties
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Specialties (
                specialty_id SERIAL PRIMARY KEY,
                code VARCHAR(20) NOT NULL,
                name VARCHAR(255) NOT NULL,
                description TEXT
            )
        """)

        # Таблица Courses
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Courses (
                course_id SERIAL PRIMARY KEY,
                department_id INTEGER REFERENCES Departments(department_id),
                name VARCHAR(255) NOT NULL,
                description TEXT,
                duration_weeks INTEGER
            )
        """)

        # Таблица Student_Groups
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Student_Groups (
                group_id SERIAL PRIMARY KEY,
                department_id INTEGER REFERENCES Departments(department_id),
                specialty_id INTEGER REFERENCES Specialties(specialty_id),
                name VARCHAR(50) NOT NULL,
                course_year INTEGER
            )
        """)

        # Таблица Group_Courses
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Group_Courses (
                group_id INTEGER REFERENCES Student_Groups(group_id),
                course_id INTEGER REFERENCES Courses(course_id),
                PRIMARY KEY (group_id, course_id)
            )
        """)

        # Таблица Session_Types
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Session_Types (
                session_type_id SERIAL PRIMARY KEY,
                name VARCHAR(50) NOT NULL
            )
        """)

        # Таблица Lecture_Sessions
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Lecture_Sessions (
                session_id SERIAL PRIMARY KEY,
                course_id INTEGER REFERENCES Courses(course_id),
                session_type_id INTEGER REFERENCES Session_Types(session_type_id),
                topic VARCHAR(255) NOT NULL,
                duration_minutes INTEGER,
                description TEXT,
                tags JSON
            )
        """)

        # Таблица Schedule
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Schedule (
                schedule_id SERIAL PRIMARY KEY,
                group_id INTEGER REFERENCES Student_Groups(group_id),
                session_id INTEGER REFERENCES Lecture_Sessions(session_id),
                room VARCHAR(50),
                scheduled_date DATE,
                start_time TIME
            )
        """)

        # Таблица Students
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Students (
                student_id SERIAL PRIMARY KEY,
                group_id INTEGER REFERENCES Student_Groups(group_id),
                name VARCHAR(255) NOT NULL,
                enrollment_year INTEGER,
                date_of_birth DATE,
                email VARCHAR(255),
                book_number VARCHAR(20)
            )
        """)

        # Таблица Attendance
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Attendance (
                attendance_id SERIAL PRIMARY KEY,
                schedule_id INTEGER REFERENCES Schedule(schedule_id),
                student_id INTEGER REFERENCES Students(student_id),
                attended BOOLEAN NOT NULL,
                absence_reason TEXT
            )
        """)

        # Таблица Lecture_Materials
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Lecture_Materials (
                material_id SERIAL PRIMARY KEY,
                session_id INTEGER REFERENCES Lecture_Sessions(session_id),
                file_path VARCHAR(255) NOT NULL,
                type VARCHAR(50),
                uploaded_at TIMESTAMP
            )
        """)

        conn.commit()
        print("Таблицы успешно созданы!")

    except Exception as e:
        conn.rollback()
        print(f"Ошибка: {e}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    setup_tables()
