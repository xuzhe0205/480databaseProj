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
('Dennis', 2.0, 331),
('Dennis', 2.0, 231),
('Anne', 3.0, 231),
('Josh', 1.0, 231),
('Josh', 0.0, 480),
('Josh', 0.0, 331);""")

conn.execute("""INSERT INTO profs VALUES ('Josh', 480),
('Josh', 450),
('Rich', 231),
('Sebnem', 331);""")

check("""SELECT students.name
FROM students ORDER BY students.name DESC;""",
      conn,
      [('Zizhen',),
       ('Josh',),
       ('Josh',),
       ('Josh',),
       ('Jie',),
       ('Jie',),
       ('Dennis',),
       ('Dennis',),
       ('Cam',),
       ('Cam',),
       ('Anne',)]
      )