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
conn.execute("CREATE TABLE students (name TEXT, grade REAL);")
conn.execute("CREATE VIEW stu_view AS SELECT * FROM students WHERE grade > 3.0 ORDER BY name;")

check("""SELECT name FROM stu_view ORDER BY grade;""",
      conn,
      []
      )

conn.execute("""INSERT INTO students VALUES 
('Josh', 3.5),
('Dennis', 2.5),
('Cam', 1.5),
('Zizhen', 4.0)
;""")

check("""SELECT name FROM stu_view ORDER BY grade;""",
      conn,
      [('Josh',), ('Zizhen',)]
      )

conn.execute("""INSERT INTO students VALUES 
('Emily', 3.7),
('Alex', 2.5),
('Jake', 3.2)
;""")

check("""SELECT name FROM stu_view ORDER BY grade;""",
      conn,
      [('Jake',), ('Josh',), ('Emily',), ('Zizhen',)]
      )