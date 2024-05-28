from faker import Faker
import random
import sqlite3
import os

fake = Faker()


class CreateConnection:
    '''
    Class for creating a database connection. Takes the database name as an argument.
    The connection is opened when the object is created.
    '''
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cur = self.conn.cursor()

    def close_connection(self):
        self.conn.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.conn.rollback()
        else:
            self.conn.commit()
        self.conn.close()


class CreateTables:
    '''
    Class for creating tables in the database. Takes a connection object as an argument.
    The constructor creates the tables: students, groups, lecturers, subjects, grades.
    '''
    def __init__(self, connection):
        self.conn = connection
        self.cur = connection.cur

    def create_tables(self):
        self.cur.execute('''CREATE TABLE IF NOT EXISTS students (
                            id INTEGER PRIMARY KEY,
                            name TEXT,
                            group_id INTEGER)''')
        self.cur.execute('''CREATE TABLE IF NOT EXISTS groups (
                            id INTEGER PRIMARY KEY,
                            name TEXT)''')
        self.cur.execute('''CREATE TABLE IF NOT EXISTS lecturers (
                            id INTEGER PRIMARY KEY,
                            name TEXT)''')
        self.cur.execute('''CREATE TABLE IF NOT EXISTS subjects (
                            id INTEGER PRIMARY KEY,
                            name TEXT,
                            lecturer_id INTEGER,
                            FOREIGN KEY (lecturer_id) REFERENCES lecturers(id))''')
        self.cur.execute('''CREATE TABLE IF NOT EXISTS grades (
                            id INTEGER PRIMARY KEY,
                            student_id INTEGER,
                            subject_id INTEGER,
                            grade INTEGER,
                            date TEXT,
                            FOREIGN KEY (student_id) REFERENCES students(id),
                            FOREIGN KEY (subject_id) REFERENCES subjects(id))''')


class GroupData:
    '''
    Class for inserting data into the groups table. Takes a connection object as an argument.
    Provides a method to insert group data.
    '''
    def __init__(self, connection):
        self.conn = connection
        self.cur = connection.cur

    def insert(self, name):
        self.cur.execute("INSERT INTO groups (name) VALUES (?)", (name,))
        self.conn.conn.commit()
        return self.cur.lastrowid


class StudentData:
    '''
    Class for inserting data into the students table. Takes a connection object as an argument.
    Provides a method to insert student data.
    '''
    def __init__(self, connection):
        self.conn = connection
        self.cur = connection.cur

    def insert(self, name, group_id):
        self.cur.execute("INSERT INTO students (name, group_id) VALUES (?, ?)", (name, group_id))
        self.conn.conn.commit()
        return self.cur.lastrowid


class LecturerData:
    '''
    Class for inserting data into the lecturers table. Takes a connection object as an argument.
    Provides a method to insert lecturer data.
    '''
    def __init__(self, connection):
        self.conn = connection
        self.cur = connection.cur

    def insert(self, name):
        self.cur.execute("INSERT INTO lecturers (name) VALUES (?)", (name,))
        self.conn.conn.commit()
        return self.cur.lastrowid


class SubjectData:
    '''
    Class for inserting data into the subjects table. Takes a connection object as an argument.
    Provides a method to insert subject data.
    '''
    def __init__(self, connection):
        self.conn = connection
        self.cur = connection.cur

    def insert(self, name, lecturer_id):
        self.cur.execute("INSERT INTO subjects (name, lecturer_id) VALUES (?, ?)", (name, lecturer_id))
        self.conn.conn.commit()
        return self.cur.lastrowid


class GradeData:
    '''
    Class for inserting data into the grades table. Takes a connection object as an argument.
    Provides a method to insert grade data.
    '''
    def __init__(self, connection):
        self.conn = connection
        self.cur = connection.cur

    def insert(self, student_id, subject_id, grade, date):
        self.cur.execute("INSERT INTO grades (student_id, subject_id, grade, date) VALUES (?, ?, ?, ?)", (student_id, subject_id, grade, date))
        self.conn.conn.commit()


class FakeDataGenerator:
    '''
    Class for generating fake data. Uses the Faker library to create fake data for groups, lecturers, subjects, students, and grades.
    Provides a method to generate fake data.
    '''
    def __init__(self):
        self.fake = Faker()

    def generate_fake_data(self):
        groups = ['Group A', 'Group B', 'Group C']
        lecturers = [self.fake.name() for _ in range(5)]
        subjects = ['Mathematics', 'Physics', 'Chemistry', 'Biology', 'History', 'English']
        students = [(self.fake.name(), random.choice(groups)) for _ in range(40)]
               
        grades = []
        for student in students:
            student_grades = []
            for subject in subjects:
                for _ in range(2):
                    grade = random.randint(1, 6)
                    date = self.fake.date_this_year(before_today=True, after_today=False)
                    student_grades.append((student[0], subject, grade, date))
            additional_grades_count = random.randint(10, 20) - len(student_grades)
            for _ in range(additional_grades_count):
                subject = random.choice(subjects)
                grade = random.randint(1, 6)
                date = self.fake.date_this_year(before_today=True, after_today=False)
                student_grades.append((student[0], subject, grade, date))
            grades.extend(student_grades)

        return groups, lecturers, subjects, students, grades


class InsertFakeData:
    '''
    Class for inserting fake data into the database. Takes a connection object as an argument.
    Uses other data insertion classes to insert the generated fake data into the respective tables.
    '''
    def __init__(self, connection):
        self.group_data = GroupData(connection)
        self.student_data = StudentData(connection)
        self.lecturer_data = LecturerData(connection)
        self.subject_data = SubjectData(connection)
        self.grade_data = GradeData(connection)
        self.generator = FakeDataGenerator()

    def insert_all_data(self):
        groups, lecturers, subjects, students, grades = self.generator.generate_fake_data()

        group_ids = {}
        for group in groups:
            group_id = self.group_data.insert(group)
            group_ids[group] = group_id

        lecturer_ids = {}
        for lecturer in lecturers:
            lecturer_id = self.lecturer_data.insert(lecturer)
            lecturer_ids[lecturer] = lecturer_id

        subject_ids = {}
        lecturer_ids_list = list(lecturer_ids.values())
        for i, subject in enumerate(subjects):
            lecturer_id = lecturer_ids_list[i % len(lecturer_ids_list)]
            subject_id = self.subject_data.insert(subject, lecturer_id)
            subject_ids[subject] = subject_id

        student_ids = {}
        for student, group in students:
            student_id = self.student_data.insert(student, group_ids[group])
            student_ids[student] = student_id

        for grade in grades:
            student_id = student_ids[grade[0]]
            subject_id = subject_ids[grade[1]]
            self.grade_data.insert(student_id, subject_id, grade[2], grade[3].strftime('%Y-%m-%d'))


class DatabaseInitializer:
    '''
    Class for initializing the database. Takes the database name as an argument.
    Provides a method to initialize the database by creating tables and inserting fake data.
    '''
    def __init__(self, db_name):
        self.db_name = db_name

    def initialize_database(self):
        with CreateConnection(self.db_name) as connection:
            create_tables = CreateTables(connection)
            create_tables.create_tables()

            insert_fake_data = InsertFakeData(connection)
            insert_fake_data.insert_all_data()


class QueryExecutor:
    '''
    Class for executing SQL queries. Takes the database name as an argument.
    Provides methods to execute queries from a file, check if specific data exists, and verify relationships between data.
    '''
    def __init__(self, db_name):
        self.db_name = db_name

    def execute_sql_query(self, query_file_path, parameters=None):
        with open(query_file_path, 'r') as query_file:
            query = query_file.read()

        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            if parameters:
                cursor.execute(query, parameters)
            else:
                cursor.execute(query)
            result = cursor.fetchall()
        return result

    def check_subject_exists(self, subject):
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM subjects WHERE name = ?", (subject,))
            result = cursor.fetchone()
        return result[0] > 0

    def check_lecturer_exists(self, lecturer):
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM lecturers WHERE name = ?", (lecturer,))
            result = cursor.fetchone()
        return result[0] > 0

    def check_student_exists(self, student):
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM students WHERE name = ?", (student,))
            result = cursor.fetchone()
        return result[0] > 0

    def check_group_exists(self, group):
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM groups WHERE name = ?", (group,))
            result = cursor.fetchone()
        return result[0] > 0
    
    def check_lecturer_teaches_subject(self, lecturer, subject):
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) 
                FROM subjects
                WHERE lecturer_id = (SELECT id FROM lecturers WHERE name = ?)
                AND name = ?
            """, (lecturer, subject))
            result = cursor.fetchone()
        return result[0] > 0


class QuestionSelector:
    '''
    Class for selecting a question to execute a specific query. Provides a method to choose a question from a list of predefined options.
    '''
    def choose_question():
        print("Choose a question number:")
        print("1. Top 5 students with the highest average grade across all subjects.")
        print("2. The student with the highest average grade in a chosen subject.")
        print("3. Average grades in groups for a chosen subject.")
        print("4. Average grades for all groups, considering all grades.")
        print("5. Subjects taught by a chosen lecturer.")
        print("6. List of students in a chosen group.")
        print("7. Grades of students in a chosen group for a specific subject.")
        print("8. Average grades given by a lecturer for a specific subject.")
        print("9. List of courses attended by a student.")
        print("10. List of courses taught by a chosen lecturer for a specific student.")
        print("exit - to exit")
        print()

        while True:
            user_input = input("Choose a question number (1-10): ")
            print()
            if user_input == "exit":
                return None
            elif user_input.isdigit():
                question_number = int(user_input)
                if 1 <= question_number <= 10:
                    return question_number
                else:
                    print("Invalid question number. Please enter a number from 1 to 10.")
                    print()
            else:
                print("Invalid question number. Please enter a number from 1 to 10.")
                print()


def main():
    db_filename = "university.db"

    if not os.path.exists(db_filename):
        db_initializer = DatabaseInitializer(db_filename)
        db_initializer.initialize_database()
    else:
        print("The database exists.")
    print()

    query_executor = QueryExecutor("university.db")
    while True:
        question_number = QuestionSelector.choose_question()
        
        if question_number is None:
            break

        elif question_number == 1:
            query_result = query_executor.execute_sql_query("query_1.sql")
            query_result.sort(key=lambda x: x[2], reverse=True)
            print("Below is a list of the 5 people with the highest average:")
            for i, row in enumerate(query_result[:5], 1):
                print(f"{i}. {row[1]} average {row[2]}")
            print()

        elif question_number == 2:
            subject = input("Enter the name of the course subject (Mathematics, Physics, Chemistry, Biology, History, English): ")
            if not query_executor.check_subject_exists(subject):
                print("There is no such subject. Back to question selection.")
                print()
                continue
            query_result = query_executor.execute_sql_query("query_2.sql", (subject,))
            best_student = max(query_result, key=lambda x: x[2])
            print(f"{best_student[1]} has the highest average in {subject} and it is {best_student[2]}")
            print()

        elif question_number == 3:
            subject = input("Enter the name of the course subject (Mathematics, Physics, Chemistry, Biology, History, English): ")
            if not query_executor.check_subject_exists(subject):
                print("There is no such subject. Back to question selection.")
                print()
                continue
            query_result = query_executor.execute_sql_query("query_3.sql", (subject,))
            print(f"Average grade for the subject {subject}:")
            for row in query_result:
                print(f"{row[1]} average {row[2]}")
            print()

        elif question_number == 4:
            query_result = query_executor.execute_sql_query("query_4.sql")
            print("Average grade in all subjects:")
            for row in query_result:
                print(f"{row[1]} average {row[2]}")
            print()

        elif question_number == 5:
            lecturer = input("Enter the lecturer's name (university.db file): ")
            if not query_executor.check_lecturer_exists(lecturer):
                print("There is no such lecturer. Back to question selection.")
                print()
                continue
            query_result = query_executor.execute_sql_query("query_5.sql", (lecturer,))
            print(f"Lecturer {lecturer} teaches the following subjects:")
            for i, row in enumerate(query_result, start=1):
                print(f"{i}. {row[1]}")
            print()

        elif question_number == 6:
            group = input("Enter a group name (Group A, Group B, Group C): ")
            if not query_executor.check_group_exists(group):
                print("There is no such group. Back to question selection.")
                print()
                continue
            query_result = query_executor.execute_sql_query("query_6.sql", (group,))
            print(f"List of people in the {group}:")
            for i, row in enumerate(query_result, start=1):
                print(f"{i}. {row[1]}")
            print()

        elif question_number == 7:
            group = input("Enter a group name (Group A, Group B, Group C): ")
            if not query_executor.check_group_exists(group):
                print("There is no such group. Back to question selection.")
                print()
                continue
            subject = input("Enter the name of the course subject (Mathematics, Physics, Chemistry, Biology, History, English): ")
            if not query_executor.check_subject_exists(subject):
                print("There is no such subject. Back to question selection.")
                print()
                continue
            query_result = query_executor.execute_sql_query("query_7.sql", (group, subject))
            print()
            print(f"For {group} in {subject}, students obtained the following grades:")
            students_grades = {}
            for row in query_result:
                student_name = row[1]
                grade = row[2]
                if student_name in students_grades:
                    students_grades[student_name].append(grade)
                else:
                    students_grades[student_name] = [grade]
            for student, grades in students_grades.items():
                grades_str = ", ".join(map(str, grades))
                print(f"{student}: {grades_str}")
            print()

        elif question_number == 8:
            lecturer_name = input("Enter the lecturer's name and surname (university.db file): ")
            if not query_executor.check_lecturer_exists(lecturer_name):
                print("There is no such lecturer. Back to question selection.")
                print()
                continue
            subject = input("Enter the name of the course subject (Mathematics, Physics, Chemistry, Biology, History, English): ")
            if not query_executor.check_subject_exists(subject):
                print("There is no such subject. Back to question selection.")
                print()
                continue
            if not query_executor.check_lecturer_teaches_subject(lecturer_name, subject):
                print(f"Lecturer {lecturer_name} does not teach {subject}.\n")
                continue

            query_result = query_executor.execute_sql_query("query_8.sql", (lecturer_name, subject))
            if not query_result:
                print(f"No data for the lecturer {lecturer_name} and the subject {subject}.\n")
            else:
                for row in query_result:
                    print(f"The average grade given by {lecturer_name} for the subject {row[1]} is {row[2]}.")
                print()

        elif question_number == 9:
            student = input("Enter the student name (university.db file): ")
            if not query_executor.check_student_exists(student):
                print("There is no such student. Back to question selection.\n")
                continue
            query_result = query_executor.execute_sql_query("query_9.sql", (student,))
            print(f"Student {student} attends the following subjects:")
            for i, row in enumerate(query_result, start=1):
                print(f"{i}. {row[1]}")
            print()

        elif question_number == 10:
            lecturer = input("Enter the name of the lecturer (university.db file): ")
            print()
            if not query_executor.check_lecturer_exists(lecturer):
                print("There is no such lecturer. Back to question selection.\n")
                continue
            student = input("Enter the student name (university.db file): ")
            print()
            if not query_executor.check_student_exists(student):
                print("There is no such student. Back to question selection.\n")
                continue
            query_result = query_executor.execute_sql_query("query_10.sql", (lecturer, student))
            print(f"List of subjects kept by {lecturer} for studenta {student}: ")
            for i, row in enumerate(query_result, start=1):
                print(f"{i}. {row[1]}")
            print()


if __name__ == "__main__":
    main()