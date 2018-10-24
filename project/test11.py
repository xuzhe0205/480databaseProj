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
check("SELECT * FROM students ORDER BY class, name;",
      conn,
      [('Emily', 2.25, 450),
       ('James', 2.25, 450),
       ('Losh', 3.2, 450),
       ('Tosh', 4.5, 450),
       ('Grant', 3.3, 480),
       ('Josh', 3.5, 480),
       ('Tyler', 2.5, 480)]
      )


def collate_ignore_first_letter(string1, string2):
    string1 = string1[1:]
    string2 = string2[1:]
    if string1 == string2:
        return 0
    if string1 < string2:
        return -1
    else:
        return 1


conn.create_collation("skip", collate_ignore_first_letter)

check("SELECT * FROM students ORDER BY name COLLATE skip, grade;",
      conn,
      [('James', 2.25, 450),
       ('Emily', 2.25, 450),
       ('Losh', 3.2, 450),
       ('Josh', 3.5, 480),
       ('Tosh', 4.5, 450),
       ('Grant', 3.3, 480),
       ('Tyler', 2.5, 480)]
      )