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
conn.execute(
    "CREATE TABLE students (name TEXT DEFAULT '', health INTEGER DEFAULT 100, grade REAL DEFAULT 0.0, id TEXT DEFAULT 'NONE PROVIDED');")

conn.execute("INSERT INTO students VALUES ('Zizhen', 45, 4.0, 'Hi');")
conn.execute("INSERT INTO students DEFAULT VALUES;")
conn.execute("INSERT INTO students (name, id) VALUES ('Cam', 'Hello');")
conn.execute("INSERT INTO students (id, name) VALUES ('Instructor', 'Josh');")
conn.execute("INSERT INTO students DEFAULT VALUES;")

conn.execute("INSERT INTO students (id, name, grade) VALUES ('TA', 'Dennis', 3.0);")
conn.execute("INSERT INTO students (id, name) VALUES ('regular', 'Emily'), ('regular', 'Alex');")

check("""SELECT name, id, grade, health  FROM students ORDER BY students.name;""",
      conn,
      [('', 'NONE PROVIDED', 0.0, 100),
       ('', 'NONE PROVIDED', 0.0, 100),
       ('Alex', 'regular', 0.0, 100),
       ('Cam', 'Hello', 0.0, 100),
       ('Dennis', 'TA', 3.0, 100),
       ('Emily', 'regular', 0.0, 100),
       ('Josh', 'Instructor', 0.0, 100),
       ('Zizhen', 'Hi', 4.0, 45)]
      )