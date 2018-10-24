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
conn.execute("CREATE TABLE students (name TEXT, grade REAL, class INTEGER DEFAULT 231);")
conn.executemany("INSERT INTO students VALUES (?, ?, 480);", [('Josh', 3.5), ('Tyler', 2.5), ('Grant', 3.0)])
conn.executemany("INSERT INTO students VALUES (?, 0.0, ?);", [('Jim', 231), ('Tim', 331), ('Gary', 450)])

conn.executemany("INSERT INTO students (grade, name) VALUES (?, ?);", [(4.1, 'Tess'), (1.1, 'Jane')])

check("""SELECT name, class, grade FROM students ORDER BY grade, name;""",
      conn,
      [('Gary', 450, 0.0),
       ('Jim', 231, 0.0),
       ('Tim', 331, 0.0),
       ('Jane', 231, 1.1),
       ('Tyler', 480, 2.5),
       ('Grant', 480, 3.0),
       ('Josh', 480, 3.5),
       ('Tess', 231, 4.1)]
      )