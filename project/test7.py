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

check("""SELECT grade, name FROM stu_view WHERE name < 'W' ORDER BY grade DESC;""",
      conn,
      [(3.7, 'Emily'), (3.5, 'Josh'), (3.2, 'Jake')]
      )

conn.execute("CREATE TABLE enroll (student_name TEXT, course INTEGER);")
conn.execute("""INSERT INTO enroll VALUES 
('Josh', 480),
('Dennis', 331),
('Emily', 231),
('Zizhen', 231)
;""")

check("""SELECT students.name, students.grade, enroll.course 
FROM students LEFT OUTER JOIN enroll ON students.name = enroll.student_name
ORDER BY students.name DESC;""",
      conn,
      [('Zizhen', 4.0, 231),
       ('Josh', 3.5, 480),
       ('Jake', 3.2, None),
       ('Emily', 3.7, 231),
       ('Dennis', 2.5, 331),
       ('Cam', 1.5, None),
       ('Alex', 2.5, None)]
      )

conn.execute("""CREATE VIEW stu_view2 AS 
SELECT students.name, students.grade, enroll.course 
FROM students LEFT OUTER JOIN enroll ON students.name = enroll.student_name
ORDER BY students.name DESC;
""");

check("""SELECT name, course 
FROM stu_view2 WHERE grade > 2.0
ORDER BY grade, name;""",
      conn,
      [('Alex', None),
       ('Dennis', 331),
       ('Jake', None),
       ('Josh', 480),
       ('Emily', 231),
       ('Zizhen', 231)]
      )

conn.execute("""INSERT INTO enroll VALUES ('Jake', 480);""")

check("""SELECT name, course 
FROM stu_view2 WHERE grade > 2.0
ORDER BY grade DESC, name;""",
      conn,
      [('Zizhen', 231),
       ('Emily', 231),
       ('Josh', 480),
       ('Jake', 480),
       ('Alex', None),
       ('Dennis', 331)]
      )