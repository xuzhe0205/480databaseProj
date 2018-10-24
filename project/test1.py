import project
import os
from pprint import pprint


def check(sql_statement, conn, expected):
    print("SQL: " + sql_statement)
    result = conn.execute(sql_statement)
    result_list = list(result)

    print("expected:")
    pprint(expected)
    print("student: ")
    pprint(result_list)
    assert expected == result_list


conn = project.connect("test1.db")
conn.execute("CREATE TABLE students (name TEXT, grade REAL, course INTEGER);")
conn.execute("CREATE TABLE profs (name TEXT, course INTEGER);")

conn.execute("""INSERT INTO students VALUES ('Zizhen', 4.0, 450),
('Cam', 3.5, 480),
('Cam', 3.0, 450),
('Jie', 0.0, 231),
('Jie', 2.0, 331),
('Anne', 3.0, 231),
('Josh', 1.0, 231),
('Josh', 0.0, 480),
('Josh', 0.0, 331);""")

conn.execute("""INSERT INTO profs VALUES ('Josh', 480),
('Josh', 450),
('Rich', 231),
('Sebnem', 331);""")

check("""SELECT profs.name, students.grade, students.name
FROM students LEFT OUTER JOIN profs ON students.course = profs.course
WHERE students.grade > 0.0 ORDER BY students.name, profs.name;""",
      conn,
      [('Rich', 3.0, 'Anne'),
       ('Josh', 3.5, 'Cam'),
       ('Josh', 3.0, 'Cam'),
       ('Sebnem', 2.0, 'Jie'),
       ('Rich', 1.0, 'Josh'),
       ('Josh', 4.0, 'Zizhen')])