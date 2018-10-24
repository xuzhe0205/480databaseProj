"""
Name: Oliver Xu
Time To Completion: 6 hours
Comments:

Sources:
    https://docs.python.org/2/library/json.html
"""
import re
import copy
import json

class Collection:
    """
    A list of dictionaries (documents) accessible in a DB-like way.
    """

    def __init__(self):
        """
        Initialize an empty collection.
        """
        self.collection = []
    
    def insert(self, document):
        """
        Add a new document (a.k.a. python dict) to the collection.
        """
        self.collection.append(document)

    def find_all(self):
        """
        Return list of all docs in database.
        """
        return self.collection

    def delete_all(self):
        """
        Truncate the collection.
        """
        self.collection = []
        

    def find_one(self, where_dict):
        """
        Return the first matching doc.
        If none is found, return None.
        """
        l = len(where_dict)
        for d in self.collection:
            cnt = 0
            for key in where_dict:
                if key in d:
                    if (type(d[key]) is dict) and (type(where_dict[key]) is dict):
                        if where_dict[key].items() <= d[key].items():
                            cnt += 1
                    elif d[key] == where_dict[key]:
                        cnt += 1
            if cnt == l:
                return d
        return None

    def find(self, where_dict):
        """
        Return matching list of matching doc(s).
        """
        match = []
        l = len(where_dict)
        for d in self.collection:
            cnt = 0
            for key in where_dict:
                if key in d:
                    if (type(d[key]) is dict) and (type(where_dict[key]) is dict):
                        if where_dict[key].items() <= d[key].items():
                            cnt += 1
                    elif d[key] == where_dict[key]:
                        cnt += 1
            if cnt == l:
                match.append(d)
        if len(match) == 0:
            return []
        return match                    
            

    def count(self, where_dict):
        """
        Return the number of matching docs.
        """
        l = len(where_dict)
        num = 0
        for d in self.collection:
            cnt = 0
            for key in where_dict:
                if key in d:
                    if (type(d[key]) is dict) and (type(where_dict[key]) is dict):
                        if where_dict[key].items() <= d[key].items():
                            cnt += 1
                    elif d[key] == where_dict[key]:
                        cnt += 1
            if cnt == l:
                num += 1
        return num

    def delete(self, where_dict):
        """
        Delete matching doc(s) from the collection.
        """
        diff = []
        for d in self.collection:
            for key in where_dict:
                if (key not in d) or d[key] != where_dict[key]:
                    diff.append(d)
        self.collection = diff

    def update(self, where_dict, changes_dict):
        """
        Update matching doc(s) with the values provided.
        """
        l = len(where_dict)
        for d in self.collection:
            cnt = 0
            for key in where_dict:
                if key in d:
                    if where_dict[key] == d[key]:
                        cnt += 1
            if cnt == l:
                for k in changes_dict:
                    d[k] = changes_dict[k]

    def map_reduce(self, map_function, reduce_function):
        """
        Applies a map_function to each document, collating the results.
        Then applies a reduce function to the set, returning the result.
        """
        mapRes = []
        for doc in self.collection:
            mapRes.append(map_function(doc))
        return reduce_function(mapRes)

class Database:
    """
    Dictionary-like object containing one or more named collections.
    """

    def __init__(self, filename):
        """
        Initialize the underlying database. If filename contains data, load it.
        """
        try:
            self.collections = {}
            self.f = open(filename,"r")
            jsonStr = self.f.read()
            jsonDict = json.loads(jsonStr)
            for k in jsonDict:
                c = Collection()
                c.collection = jsonDict[k]
                self.collections[k] = c
        except:
            self.f = open(filename, "w")
            self.collections = {}
            
        
    def get_collection(self, name):
        """
        Create a collection (if new) in the DB and return it.
        """
        cName = name
        if name not in self.collections:
            name = Collection()
            self.collections[cName] = name
        return self.collections[cName]
    
    def drop_collection(self, name):
        """
        Drop the specified collection from the database.
        """
        if name in self.collections:
            del(self.collections[name])

    def get_names_of_collections(self):
        """
        Return a list of the sorted names of the collections in the database.
        """
        nameList = []
        # for variable name of the Collection object:
        for c in self.collections:
            nameList.append(c)
        return sorted(nameList)

    def close(self):
        """
        Save and close file.
        """
        jsonD = {}
        for k in self.collections:
            jsonD[k] = self.collections[k].collection
        json_str = json.dumps(jsonD)
        self.f.write(json_str)
        self.f.close()