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


conn = project.connect("test.db")
conn.execute("CREATE TABLE students (name TEXT, grade REAL, class INTEGER);")
conn.executemany("INSERT INTO students VALUES (?, ?, ?);",
                 [('Josh', 3.5, 480),
                  ('Tyler', 2.5, 480),
                  ('Tosh', 4.5, 450),
                  ('Losh', 3.2, 450),
                  ('Grant', 3.3, 480),
                  ('Emily', 2.25, 450),
                  ('James', 2.25, 450)])
check("SELECT max(grade) FROM students ORDER BY grade;",
      conn,
      [(4.5,)]
      )

check("SELECT min(class), max(name) FROM students ORDER BY grade, name;",
      conn,
      [(450, 'Tyler')]
      )