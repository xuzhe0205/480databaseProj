from project import Collection
from project import Database

d = Database("grades.db")
c = d.get_collection("student_grades")

docs = [
    {"student": "Josh", "grades": {"hw1": 2, "hw2": 1.5}},
    {"student": "Emily", "grades": {"hw2": 2, "hw3": 1.5}},
    {"student": "Charles", "grades": {}},
    {"student": "Tyler"},
    {"student": "Grant", "grades": {"hw1": 2, "hw3": 1.5}},
    {"student": "Rich", "grades": {"hw2": 2, "hw3": 1}},
]

for doc in docs:
    c.insert(doc)

assert c.find_all() == docs, c.find_all()

assert d.get_names_of_collections() == ["student_grades"], d.get_names_of_collections()

c2 = d.get_collection("student_grades")
assert c2.find_all() == docs, c2.find_all()
