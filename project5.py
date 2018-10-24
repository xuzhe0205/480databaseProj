"""
Name: Oliver Xu
Time To Completion: 40 hours
Comments:

Sources:
    re.findall: https://docs.python.org/3/library/re.html  (6.2.5.6.)
    strip: https://docs.python.org/2/library/string.html (7.1.6)
    eval: https://docs.python.org/3/library/functions.html
    tuple: https://www.digitalocean.com/community/tutorials/understanding-tuples-in-python-3
    split: https://pythonprogramminglanguage.com/split-string/
    index:  https://docs.python.org/3/tutorial/datastructures.html
    MSU CSE 480 course material
    Homework 1 Question #6
    copy, deepcopy: https://stackoverflow.com/questions/5105517/deep-copy-of-a-dict-in-python
    functools.cmp_to_key: https://docs.python.org/3.6/library/functools.html
    https://stackoverflow.com/questions/42875410/how-to-use-sorted-function
    https://stackoverflow.com/questions/32752739/python-how-does-the-functools-cmp-to-key-function-works
"""
import re, copy, functools

_ALL_DATABASES = {}
custom = {}

class TableExistException(Exception): pass
class TableNotExistException(Exception): pass
class TranOpenException(Exception): pass
class ExclusiveLockException(Exception): pass
class ImmdiateLockException(Exception): pass
class TranNotOpenException(Exception): pass
class CommitAfterRollbackException(Exception): pass
class shareLockException(Exception): pass
class RepeatRollbackException(Exception): pass
class AcquireExclusiveLockException(Exception): pass
class AcquireImmediateLockException(Exception): pass
class ReleaseImmediateLockException(Exception): pass

class Connection(object):
    def __init__(self, filename):
        """
        It works as a constructor that takes a filename and corresponds
        to a database
        """
        if filename in _ALL_DATABASES:
            self.db = _ALL_DATABASES[filename]
            self.db.conn.append(self)
        else:
            self.db = Database(filename)
            self.db.conn.append(self)
            _ALL_DATABASES[filename] = self.db
        self.begin = 0
        self.copy = None
        self.rollback = 0

    def execute(self, statement):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """
        # Check what the statement wish the code to do： CREATE，INSERT
        # SELECT, DELETE,or UPDATE
        # Eliminate the "\n" to handle the triple quotes string (''')
        statement = statement.replace("\n", "")
        if statement.find('CREATE TABLE') != -1:
            self.db.createTable(statement)
            return
        if statement.find('CREATE VIEW') != -1:
            self.db.createView(statement)
            return
        if statement.find('INSERT') != -1:
            # Call the "insertValue" function in Database class
            self.db.insertValue(statement, self)
            return
        # Call the "selectField" function in Database class
        if statement.find('SELECT') != -1:
            result = self.db.selectField(statement, self)
            return result
        # Call the "deleteElement" function in Database class
        if statement.find('DELETE') != -1:
            self.db.deleteElement(statement, self)
        # Call the "updateElement" function in Database class
        if statement.find('UPDATE') != -1:
            self.db.updateElement(statement, self)
        if statement.find('DROP') != -1:
            self.db.dropTable(statement)
        if statement.find('BEGIN') != -1:
            self.db.beginTran(self, statement)
        if statement.find('COMMIT') != -1:
            self.db.commitTran(self)
        if statement.find('ROLLBACK') != -1:
            self.db.rollback(self)
        else:
            return
    # Handle parameterized queries
    def executemany(self, statement, valueList):
        strs = re.findall("VALUES(.*);", statement)[0].strip()
        placeholders = strs.split(",")
        # Replace "?" with the corresponding value, and execute command on each value
        for i in range(0, len(valueList)):
            directive = statement
            for j in range(0, len(valueList[i])):
                directive = directive.replace("?", str(valueList[i][j]), 1)
            self.execute(directive)
    
    # Create Collation for custom sorting
    def create_collation(self, fun_name, fun_define):
        if fun_name not in custom:
            custom[fun_name] = fun_define

    def copyDatabase(self, copyTables):
        self.db.tables = copy.deepcopy(copyTables)

    def close(self):
        """
        Empty method that will be used in future projects
        """
        pass


def connect(filename, **kw):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename)


# Parse filename to find corresponding database,
# Construct a database object
class Database:
    def __init__(self, filename):
        # take in string: xxx.db, extract "xxx"
        db_name = re.findall("(.*).db", filename)
        # initialize name and list of tables of database as required
        self.__name = db_name[0]
        self.__tableName = []
        self.tables = []
        self.conn = []
        self.exlock = None
        self.sharelock = None
        self.immlock = None
        self.deferred = None
        self.views = []

    # find the targeted table's name from the database in "JOIN" case
    def getTable(self, name):
        for t in self.tables:
            if t.getName() == name:
                return t

    def createTable(self, statement):
        # Extract the required table name from the query
        if statement.find('IF NOT EXISTS') == -1:
            result = re.findall("TABLE(.*)\(", statement)
            if len(result) > 0:
                tableName = result[0].strip()
            else:
                tableName = re.findall("TABLE(.*);", statement)[0].strip()
        else:
            result = re.findall("EXISTS(.*)\(", statement)
            if len(result) > 0:
                tableName = result[0].strip()
            else:
                tableName = re.findall("EXISTS(.*);", statement)[0].strip()
        # if the targeted table exist, then return
        if tableName in self.__tableName and statement.find('IF NOT EXISTS') == -1:
            raise TableExistException('Table exists and can not be created.')
        else:
            if tableName in self.__tableName:
                return
            # add new-created table to list of tables
            self.__tableName.append(tableName)
            fieldString = re.findall("\((.*)\)", statement)
            self.tables.append(Table(tableName, fieldString[0].strip()))

    # Create view for user to perform customary read-type action on the database
    def createView(self, statement):
        viewName = re.findall("VIEW(.*)AS", statement)[0].strip()
        fieldString = re.findall("SELECT(.*)FROM", statement)[0].strip()
        command = re.findall("AS(.*)", statement)[0].strip()
        if len(re.findall("FROM(.*)WHERE", statement)) > 0:
            tableNames = re.findall("FROM(.*)WHERE", statement)[0].strip()
        elif len(re.findall("FROM(.*)ORDER", statement)) > 0:
            tableNames = re.findall("FROM(.*)ORDER", statement)[0].strip()
        elif len(re.findall("FROM(.*);", statement)) > 0:
            tableNames = re.findall("FROM(.*);", statement)[0].strip()
        # Create a table instance, and initialize it with empty string ('')
        t = Table(viewName, '')
        fields = []
        strs = fieldString.split(",")
        for s in strs:
            s = s.strip()
            # If SELECT all values 
            if s == '*':
                # If that is a JOIN, combine the values from the two tables into the View
                if tableNames.find("LEFT OUTER JOIN") != -1:
                    table1 = re.findall("FROM(.*)LEFT", tableNames)[0].strip()
                    table2 = re.findall("JOIN(.*)ON", tableNames)[0].strip()
                    for f1 in self.getTable(table1).getFieldName():
                        fields.append(f1)
                    for f2 in self.getTable(table2).getFieldName():
                        fields.append(f2)
                else:
                    for f in self.getTable(tableNames).getFieldName():
                        fields.append(f)
            else:
                # Handle characterized form (xx.xxx), extract "xxx"
                item = s.split(".")
                if len(item) is 2:
                    fields.append(item[1].strip())
                else:
                    fields.append(s)
        # Call the associated function from Table class to add the fields into the View
        t.setView(fields)
        # Use the extracted command (after "AS") to obtain the values from the View, and store them
        t.setValues(self.conn[0], command)
        self.views.append(t)

    def dropTable(self, statement):
        if statement.find('IF EXISTS') == -1:
            result = re.findall("TABLE(.*);", statement)
            tableName = result[0].strip()
        else:
            result = re.findall("EXISTS(.*);", statement)
            tableName = result[0].strip()
        if tableName not in self.__tableName and statement.find('IF EXISTS') == -1:
            raise TableNotExistException('Table does not exist and can not be dropped.')
        else:
            if tableName not in self.__tableName:
                return
            self.__tableName.remove(tableName)
            self.tables.remove(self.getTable(tableName))

    def beginTran(self, conn, statement):
        if conn.begin == 1:
            raise TranOpenException('Transaction is already open.')
        else:
            if statement.find('EXCLUSIVE') != -1:
                if self.exlock is None and (self.immlock is None or self.immlock is conn):
                    conn.copy = copy.deepcopy(self.tables)
                    self.exlock = conn
                    conn.begin = 1
                else:
                    raise ExclusiveLockException('Exclusive lock exception.')
            elif statement.find('IMMEDIATE') != -1:
                if self.immlock is None:
                    conn.copy = copy.deepcopy(self.tables)
                    self.immlock = conn
                    conn.begin = 1
                else:
                    raise ImmdiateLockException('Immediate lock exception.')
            # Deferred Transaction
            else:
                if conn.rollback == 1:
                    conn.rollback = 0
                conn.begin = 1
                self.deferred = conn
                conn.copy = copy.deepcopy(self.tables)

    def commitTran(self, conn):
        if conn.begin == 0:
            raise TranNotOpenException('No prior begin transaction.')
        elif conn.rollback == 1:
            raise CommitAfterRollbackException('can not commit after rollback.')
        else:
            if self.sharelock is not conn and self.sharelock is not None:
                raise shareLockException('Share lock exception.')
            else:
                if self.sharelock is conn:
                    self.sharelock = None
                if self.exlock is conn:
                    self.exlock = None
                if self.immlock is conn:
                    if self.sharelock is None and self.exlock is None:
                        self.immlock = None
                    else:
                        raise ReleaseImmediateLockException('Cannot release reserved lock')
                conn.begin = 0
                for c in self.conn:
                    if c.begin == 0:
                        c.copyDatabase(conn.copy)

    def rollback(self, conn):
        if conn.begin == 0:
            raise TranNotOpenException('no prior begin transaction before rollback.')
        else:
            if conn.rollback == 1:
                raise RepeatRollbackException('can not rollback again.')
            else:
                if self.sharelock is conn:
                    self.sharelock = None
                if self.exlock is conn:
                    self.exlock = None
                if self.immlock is conn:
                    if self.sharelock is None and self.exlock is None:
                        self.immlock = None
                    else:
                        raise ReleaseImmediateLockException('Cannot release reserved lock')
                conn.begin = 0
                conn.rollback = 1
                conn.copy = None

    # this is the "insertValue" function for database class to parse the input
    # query and go in to specific table and perform corresponding operations
    def insertValue(self, statement, conn):
        if self.immlock is None and self.deferred is conn:
            self.immlock = conn
        if self.exlock is not None and self.exlock != conn:
            raise AcquireExclusiveLockException('Other connection holds exclusive lock')
        elif self.immlock is not None and self.immlock != conn:
            raise AcquireImmediateLockException('Immediate Lock')
        else:
            nameList = self.__tableName
            tableList = self.tables
            if conn.begin == 1 and conn.rollback == 0:
                tableList = conn.copy
            inserted_col = []
            if '(' in re.findall("INTO(.*)VALUES", statement)[0]:
                temp = re.findall("INSERT(.*)VALUES", statement)
                result = re.findall("INTO(.*)\(", temp[0])
                inserted_col = re.findall("\((.*)\)", temp[0])
            else:
                if statement.find("DEFAULT") != -1:
                    result = re.findall("INTO(.*)DEFAULT", statement)
                else:
                    result = re.findall("INTO(.*)VALUES", statement)
            tableName = result[0].strip()
            valueString = re.findall("VALUES(.*);", statement)
            if tableName in nameList:
                for table in tableList:
                    if table.getName() == tableName:
                        if statement.find("DEFAULT") != -1:
                            table.insertDefaultValue()
                            break
                        else:
                            string = ""
                            i = 0
                            while i < len(valueString[0]):
                                if valueString[0][i] != '(' and valueString[0][i] != ')':
                                    string += valueString[0][i]
                                elif valueString[0][i] == ')':
                                    table.insertValue(string, inserted_col)
                                    string = ""
                                    i += 2
                                    continue
                                else:
                                    i += 1
                                    continue
                                i += 1
                            break

    # this is the "deleteElement" function for database to parse the input query
    # to handle the "DELETE" case
    def deleteElement(self, statement, conn):
        if self.exlock is not None and self.exlock != conn:
            raise AcquireExclusiveLockException('Other connection holds exclusive lock')
        elif self.immlock is not None and self.immlock != conn:
            raise AcquireImmediateLockException('Immediate Lock')
        else:
            # if there is "WHERE" in the "DELETE" query, pass the query to
            # "deleteElement" function in the table class
            nameList = self.__tableName
            tableList = self.tables
            if conn.begin == 1 and conn.rollback == 0:
                tableList = conn.copy
            if statement.find("WHERE") != -1:
                tableName = re.findall("FROM(.*)WHERE", statement)[0].strip()
            else:
                tableName = re.findall("FROM(.*);", statement)[0].strip()
            if tableName in nameList:
                for table in tableList:
                    if table.getName() == tableName:
                        table.deleteElement(statement)
                        break

    # this is the "selectField" function for database class to check if the
    # input query has "DISTINCT", "JOIN", "LEFT OUTER JOIN" or "WHERE". And
    # then pass the associated variables including the condition of "WHERE"
    # to "selectField" function of table class to perform detailed operations
    def selectField(self, statement, conn):
        if self.exlock is not None and self.exlock != conn:
            raise AcquireExclusiveLockException('Other connection holds exclusive lock')
        else:
            tableList = self.tables
            # if auto commit  is off
            if conn.begin == 1 and conn.rollback == 0:
                tableList = conn.copy
                if self.immlock is not conn:
                    self.sharelock = conn
            otherTable = None
            relation = None
            result = re.findall("SELECT(.*)FROM", statement)
            fieldList = []
            join = False
            if result[0].find("DISTINCT") != -1:
                distinct = True
                fieldList.append(re.findall("DISTINCT(.*)FROM", statement)[0].strip())
            else:
                distinct = False
                fieldList = result[0].strip().split(",")
            # Extract name of targeted table from the query
            res = re.findall("FROM(.*)ORDER", statement)
            # Extract the required order, which is stored as "tagList"
            strs = re.findall("BY(.*);", statement)
            tagList = strs[0].strip().split(",")
            condition = ''
            # if there is "LEFT OUTER JOIN", extract name of the current table and
            # the JOIN table
            if res[0].find("LEFT OUTER JOIN") != -1:
                join = True
                tableName = re.findall("FROM(.*)LEFT", statement)[0].strip()
                fieldList = [f.strip() for f in fieldList]
                otherTable = self.getTable(re.findall("JOIN(.*)ON", statement)[0].strip())
                # find the required relation between the two table, and the required
                # condition with "WHERE" clause
                # relation is the "A = B" in "X LEFT OUTER JOIN Y ON A = B"
                if res[0].find("WHERE") != -1:
                    relation = re.findall("ON(.*)WHERE", statement)[0].strip()
                    condition = re.findall("WHERE(.*)ORDER", statement)[0].strip()
                else:
                    relation = re.findall("ON(.*)ORDER", statement)[0].strip()
            else:
                # handle "a.b" case (qualified name)
                fieldList = [f[f.find(".") + 1:len(f)].strip() for f in fieldList]
                if res[0].find("WHERE") != -1:
                    tableName = re.findall("FROM(.*)WHERE", statement)[0].strip()
                    condition = re.findall("WHERE(.*)ORDER", statement)[0].strip()

                else:
                    tableName = res[0].strip()
                    condition = ''
                tagList = [t[t.find(".") + 1:len(t)].strip() for t in tagList]
            resList = []
            # Find the targeted table, and call the "selectField" function for table
            # class to perform specific operation on that table
            for t in tableList:
                if t.getName() == tableName:
                    resList = t.selectField(fieldList, tagList, condition, distinct, join, otherTable, relation)
                    break
            # If the required table does not exist in the databae, then look it up in the View
            for v in self.views:
                if v.getName() == tableName:
                    v.setValues(self.conn[0], v.getCommand())
                    resList = v.selectField(fieldList, tagList, condition, distinct, join, otherTable, relation)
                    break
            return resList


    # this is the "updateElement" function for database class
    def updateElement(self, statement, conn):
        if self.exlock is not None and self.exlock != conn:
            raise AcquireExclusiveLockException('Other connection holds exclusive lock')
        elif self.immlock is not None and self.immlock != conn:
            raise AcquireImmediateLockException('Immediate Lock')
        else:
            tableList = self.tables
            if conn.begin == 1 and conn.rollback == 0:
                tableList = conn.copy
            tableName = re.findall("UPDATE(.*)SET", statement)[0].strip()
            if statement.find("WHERE") != -1:
                valueset = re.findall("SET(.*)WHERE", statement)[0].strip()
                condition = re.findall("WHERE(.*);", statement)[0].strip()
            else:
                # Extract the fields that need to be updated
                valueset = re.findall("SET(.*);", statement)[0].strip()
                condition = ''
            # find the required table and call the "updateElement" function for
            # table class to update element in that table as specified
            for t in tableList:
                if t.getName() == tableName:
                    t.updateElement(valueset, condition)
                    break


# Construct Table object
# Perform detailed functions on the table
# Read and edit the table
class Table:
    def __init__(self, name, fieldString):
        self.__name = name
        self.__fieldName = []
        self.__fields = []
        self.__values = []
        # Create an object (a list) that stores the default value
        self.__default = []
        self.__command = ''
        self.addColumn(fieldString)

    def getName(self):
        return self.__name
        
    # Execute the statement after "AS", and obtain values from the View
    def getValues(self):
        return self.__values

    def getFieldName(self):
        return self.__fieldName

    def getCommand(self):
        return self.__command

    def setView(self, field):
        self.__fieldName = field

    def setValues(self, conn, command):
        self.__command = command
        self.__values = conn.execute(command)

    def addColumn(self, fieldString):
        if fieldString == '':
            return
        fields = fieldString.split(",")
        for f in fields:
            # split and store field types and names
            strs = f.strip().split(" ")
            if strs[1] and strs[0]:
                if strs[1].strip() in self.__fieldName:
                    return
                else:
                    # If the SQL statement specifies default value
                    if len(strs) > 2 and strs[2].find("DEFAULT") != -1:
                        f = ''
                        for i in range(3, len(strs)):
                            f += strs[i]
                            f += ' '
                        f = f.strip()
                        try:
                            f = int(f)
                            self.__default.append(f)
                        except ValueError:
                            try:
                                f = float(f)
                                self.__default.append(f)
                            except ValueError:
                                f = eval(f)
                                self.__default.append(f)
                    else:
                        self.__default.append(None)
                    self.__fieldName.append(strs[0].strip())
                    self.__fields.append(Field(strs[1].strip(), strs[0].strip()))
   
    # Insert the desired default values into that specific row in the table
    def insertDefaultValue(self):
        self.__values.append(tuple(self.__default))

    # specifically insert values into table as required and specified
    def insertValue(self, valueString, inserted_col):
        # initialize each row of data as default values: with specified default values or "None"
        row = copy.deepcopy(self.__default)
        insert_i = []
        if len(inserted_col) != 0:
            temp = inserted_col[0].split(',')
            for ele in temp:
                ele = ele.strip()
            for ele in temp:
                temp[temp.index(ele)] = ele.strip()
            for ele in temp:
                insert_i.append(self.__fieldName.index(ele))
        else:
            for ele in self.__fieldName:
                insert_i.append(self.__fieldName.index(ele))
        values = valueString.split(',')
        # convert string to csv form
        cnt = 0
        for v in values:
            v = v.strip()
            try:
                i = int(v)
                row[insert_i[cnt]] = i
            except ValueError:
                try:
                    f = float(v)
                    row[insert_i[cnt]] = f
                except ValueError:
                    if v != 'NULL':
                        result_str = ""
                        quote = False
                        i = 0
                        if v[0] == "'" and v[-1] == "'":
                            new_str = v[1:len(v) - 1]
                            while i < len(new_str):
                                if new_str[i] == "'":
                                    if i + 1 < len(new_str) and new_str[i + 1] == "'":
                                        quote = True
                                        result_str += new_str[i + 1]
                                        i += 2
                                        continue
                                    else:
                                        quote = False
                                        break
                                else:
                                    result_str += new_str[i]
                                    quote = True
                                i += 1
                        elif v[0] != "'" and v[-1] != "'":
                            while i < len(v):
                                if v[i] != "'":
                                    result_str += v[i]
                                    quote = True
                                else:
                                    quote = False
                                    break
                                i += 1
                        if quote is True:
                            result = result_str
                        row[insert_i[cnt]] = result
                    else:
                        cnt += 1
                        continue
            cnt += 1
        self.__values.append(tuple(row))

    # this is the "selectField" function for table class
    def selectField(self, fieldList, tagList, condition, distinct, join, otherTable, relation):
        # if there is "WHERE", find the field that is going to be filtered under
        # specified condition
        filterField = ''
        if condition != '':
            # eg: if there is "a > b", split it by each space
            filters = condition.split(" ")
            filterField = filters[0].strip()
            if filterField.find(".") != -1:
                # if there is no "LEFT OUTER JOIN" in query, take fields
                # of table "a" from qualified name: "table.a"
                if join is False:
                    filterField = filterField[filterField.find(".") + 1:len(filterField)]
                else:
                    # when there is "LEFT OUTER JOIN": if the table name after
                    # "." is the same as current table, "filterField" = table's
                    # fields. Otherwise, combine fields from the two tables
                    if self.getName() == filterField.split(".")[0].strip():
                        filterField = self.__fieldName.index(filterField.split(".")[1].strip())
                    else:
                        filterField = len(self.__fieldName) + otherTable.getFieldName().index(
                            filterField.split(".")[1].strip())
            if filters[len(filters) - 1].strip() == 'NULL':
                filterNum = None
            else:
                # if there is no "NULL" in the condition after "WHERE" clause,
                # store the "b" from eg.: "a > b"
                try:
                    filterNum = float(filters[len(filters) - 1].strip())
                except ValueError:
                    filterNum = filters[len(filters) - 1].strip()
        # store indices of 'SELECT' fields
        referList = []
        if join is False:
            # handle the aggregate funtion
            agg_fields = []
            for i in range(0, len(fieldList)):
                # for each entry of data, if there is "max"/"min" in SQL statement, collect the corresponding values
                if fieldList[i].find("max") != -1 or fieldList[i].find("min") != -1:
                    aggregate = re.findall("\((.*)\)", fieldList[i])[0].strip()
                    if fieldList[i].find("max") != -1:
                        fieldList[i] = fieldList[i].replace("max(", "")
                        fieldList[i] = fieldList[i].replace(")", "")
                        agg_fields.append(True)
                    elif fieldList[i].find("min") != -1:
                        fieldList[i] = fieldList[i].replace("min(", "")
                        fieldList[i] = fieldList[i].replace(")", "")
                        agg_fields.append(False)
            for i in range(len(fieldList)):
                # handle "SELECT *" case
                if (fieldList[i] == '*'):
                    for j in range(len(self.__fieldName)):
                        referList.append(j)
                else:
                    referList.append(self.__fieldName.index(fieldList[i]))
        # if there is "LEFT OUTER JOIN", find and store indices of the combined
        # fields, similar to above
        else:
            agg_fields = []
            for i in range(0, len(fieldList)):
                if fieldList[i].find("max") != -1 or fieldList[i].find("min") != -1:
                    aggregate = re.findall("\((.*)\)", fieldList[i])[0].strip()
                    if fieldList[i].find("max") != -1:
                        fieldList[i] = fieldList[i].replace("max(", "")
                        fieldList[i] = fieldList[i].replace(")", "")
                        agg_fields.append(True)
                    elif fieldList[i].find("min") != -1:
                        fieldList[i] = fieldList[i].replace("min(", "")
                        fieldList[i] = fieldList[i].replace(")", "")
                        agg_fields.append(False)
            fields = fieldList
            fieldList = []
            for f in fields:
                if self.getName() == f.split(".")[0]:
                    fieldList.append(self.__fieldName.index(f.split(".")[1].strip()))
                else:
                    fieldList.append(len(self.__fieldName) + otherTable.getFieldName().index(f.split(".")[1].strip()))
            # find relationship between the two tables
            relation = relation.split("=")
            relations = []
            for r in relation:
                rf = []
                rf.append(r.split(".")[0].strip())
                if self.getName() == r.split(".")[0]:
                    rf.append(self.__fieldName.index(r.split(".")[1].strip()))
                else:
                    rf.append(otherTable.getFieldName().index(r.split(".")[1].strip()))
                relations.append(rf)
        # find the index list of the required order for the all fields of the
        # table, order the associated values in reverse desired order
        result = []
        if join is False:
            order = []
            for i in range(0, len(tagList)):
                # Handle DESC and Collation
                if tagList[i].find("COLLATE") != -1:
                    if tagList[i].find("DESC") != -1:
                        custom_fun = re.findall("COLLATE(.*)DESC", tagList[i])
                        custom_fun = custom_fun[0].strip()
                        order.append([custom_fun, 1])
                        tagList[i] = tagList[i].replace("DESC", "").strip()
                    else:
                        custom_fun = re.findall("COLLATE(.*)", tagList[i])
                        custom_fun = custom_fun[0].strip()
                        order.append([custom_fun, 0])
                    tagList[i] = tagList[i].replace("COLLATE", "").strip()
                    tagList[i] = tagList[i].replace(custom_fun, "").strip()
                elif tagList[i].find("DESC") != -1:
                    order.append(1)
                    tagList[i] = tagList[i].replace("DESC", "").strip()
                else:
                    order.append(0)
            indexList = [self.__fieldName.index(t) for t in tagList]
            resultList = self.__values
            for i in indexList[::-1]:
                if order[indexList.index(i)] is 0:
                    resultList = sorted(resultList, key=lambda x: (x[i] == None, x[i] != None, x[i]), reverse=False)
                elif order[indexList.index(i)] is 1:
                    resultList = sorted(resultList, key=lambda x: (x[i] == None, x[i] != None, x[i]), reverse=True)
                else:
                    if order[indexList.index(i)][1] is 0:
                        resultList = sorted(resultList, key=lambda x: functools.cmp_to_key(custom[order[indexList.index(i)][0]])(x[i]), reverse=False)
                    else:
                        resultList = sorted(resultList, key=lambda x: functools.cmp_to_key(custom[order[indexList.index(i)][0]])(x[i]), reverse=True)
            # Extract the values of "SELECT" fields, according to the ordered
            # table from above
            resultList = [list(x) for x in resultList]
            for x in resultList:
                res = []
                for i in referList:
                    # if there is "WHERE"
                    if condition != '' and x[self.__fieldName.index(filterField)] is not None:
                        # if the value is not a digit, make it a string for
                        # comparison, else, convert it to float
                        try:
                            value = float(x[self.__fieldName.index(filterField)])
                        except ValueError:
                            value = "'" + x[self.__fieldName.index(filterField)] + "'"
                        # handle the condition and extract the corresponding values
                        if filters[1].strip().find(">") != -1:
                            if value > filterNum:
                                res.append(x[i])
                        elif filters[1].strip().find("<") != -1:
                            if value < filterNum:
                                res.append(x[i])
                        elif filters[1].strip().find("=") != -1 and len(filters[1].strip()) == 1:
                            if value == filterNum:
                                res.append(x[i])
                        elif filters[1].strip().find("!=") != -1:
                            if value != filterNum:
                                res.append(x[i])
                        elif filters[1].strip().find("IS") != -1:
                            if filters[2].strip().find("NOT") != -1:
                                if value != filterNum:
                                    res.append(x[i])
                    else:
                        # if there is "WHERE" and the value used to filter the
                        # fields of the table is not None
                        if filterField != '' and x[self.__fieldName.index(filterField)] is not None:
                            res.append(x[i])
                        # if there is "IS NOT"
                        elif condition != '' and filters[1].strip().find("IS") != -1 and filters[2].strip().find(
                                "NOT") == -1:
                            if x[self.__fieldName.index(filterField)] == filterNum:
                                res.append(x[i])
                        # if there is no "WHERE"
                        elif condition == '':
                            res.append(x[i])
                if len(res) != 0:
                    result.append(res)
            if len(agg_fields) != 0:
                result = [list(x) for x in result]
                every = []
                final = []
                for i in range(0, len(agg_fields)):
                    result = sorted(result, key=lambda x: x[i], reverse=agg_fields[i])
                    every.append(result[0][i])
                final.append(every)
                result = copy.deepcopy(final)
        # if there is "LEFT OUTER JOIN"
        else:
            # take the values from current table and "JOIN" table, and combine
            # them to a list
            thisValue = self.__values
            otherValue = otherTable.getValues()
            combinValue = []
            for tv in thisValue:
                # "tag" represents whether there is a found match "b" in
                # "a.x = b.y", if found, combine the the two values
                tag = 0
                for ov in otherValue:
                    if relations[0][0] == self.__name:
                        if tv[relations[0][1]] == ov[relations[1][1]] and tv[relations[0][1]] != None:
                            tag = 1
                            tempValue = tv + ov
                            combinValue.append(tempValue)
                    elif relations[0][0] == otherTable.getName():
                        if tv[relations[1][1]] == ov[relations[0][1]] and tv[relations[0][1]] != None:
                            tag = 1
                            tempValue = tv + ov
                            combinValue.append(tempValue)
                # if there is no match, combine the values of current table and
                # "None" in the length of the "JOIN" table
                if tag == 0:
                    addition = []
                    for n in range(len(ov)):
                        addition.append(None)
                    tempValue = list(tv) + addition
                    combinValue.append(tuple(tempValue))
            order = []
            for i in range(0, len(tagList)):
                if tagList[i].find("COLLATE") != -1:
                    if tagList[i].find("DESC") != -1:
                        custom_fun = re.findall("COLLATE(.*)DESC", tagList[i])
                        custom_fun = custom_fun[0].strip()
                        order.append([custom_fun, 1])
                        tagList[i] = tagList[i].replace("DESC", "").strip()
                    else:
                        custom_fun = re.findall("COLLATE(.*)", tagList[i])
                        custom_fun = custom_fun[0].strip()
                        order.append([custom_fun, 0])
                    tagList[i] = tagList[i].replace("COLLATE", "").strip()
                    tagList[i] = tagList[i].replace(custom_fun, "").strip()
                elif tagList[i].find("DESC") != -1:
                    order.append(1)
                    tagList[i] = tagList[i].replace("DESC", "").strip()
                else:
                    order.append(0)
            # Find the indices of order fields
            indexList = []
            for t in tagList:
                if t.split(".")[0].strip() == self.__name:
                    indexList.append(self.__fieldName.index(t.split(".")[1].strip()))
                # List of indices of the "JOIN" table's fields is the length of
                # current table + index of the "JOIN" table, just like appending
                # the values from "JOIN" table's fields
                else:
                    indexList.append(len(self.__fieldName) + otherTable.getFieldName().index(t.split(".")[1].strip()))
            # Order the "SELECT" fields
            for i in indexList[::-1]:
                if order[indexList.index(i)] is 0:
                    combinValue = sorted(combinValue, key=lambda x: (x[i] == None, x[i] != None, x[i]), reverse=False)
                elif order[indexList.index(i)] is 1:
                    combinValue = sorted(combinValue, key=lambda x: (x[i] == None, x[i] != None, x[i]), reverse=True)
                else:
                    combinValue = sorted(combinValue, key=lambda x: functools.cmp_to_key(custom[order[indexList.index(i)]])(x[i]), reverse=False)
            # Filter out required values from combined list
            for x in combinValue:
                res = []
                for i in fieldList:
                    if condition != '' and x[filterField] is not None:
                        try:
                            value = float(x[filterField])
                        except ValueError:
                            value = "'" + x[filterField] + "'"
                        if filters[1].strip().find(">") != -1:
                            if value > filterNum:
                                res.append(x[i])
                        elif filters[1].strip().find("<") != -1:
                            if value < filterNum:
                                res.append(x[i])
                        elif filters[1].strip().find("=") != -1 and len(filters[1].strip()) == 1:
                            if value == filterNum:
                                res.append(x[i])
                        elif filters[1].strip().find("!=") != -1:
                            if value != filterNum:
                                res.append(x[i])
                        elif filters[1].strip().find("IS") != -1:
                            if filters[2].strip().find("NOT") != -1:
                                if value != filterNum:
                                    res.append(x[i])
                    else:
                        if filterField != '' and x[filterField] is not None:
                            res.append(x[i])
                        elif condition != '' and filters[1].strip().find("IS") != -1 and filters[2].strip().find(
                                "NOT") == -1:
                            if x[filterField] == filterNum:
                                res.append(x[i])
                        elif condition == '':
                            res.append(x[i])
                if len(res) != 0:
                    result.append(res)

            if len(agg_fields) != 0:
                result = [list(x) for x in result]
                every = []
                final = []
                for i in range(0, len(agg_fields)):
                    result = sorted(result, key=lambda x: x[i], reverse=agg_fields[i])
                    every.append(result[0][i])
                final.append(every)
                result = copy.deepcopy(final)

        # convert to tuple
        result = [tuple(x) for x in result]
        if distinct is True:
            result = sorted(set(result), key=result.index)
        return result

    # this is the "deleteElement" functoin for table class
    def deleteElement(self, statement):
        if statement.find("WHERE") != -1:
            # Similarly, let "a" be filterField and "b" be filterNum under
            # condition like: a > b
            condition = re.findall("WHERE(.*);", statement)[0].strip()
            filters = condition.split(" ")
            filterField = filters[0].strip()
            if filterField.find(".") != -1:
                filterField = filterField[filterField.find(".") + 1:len(filterField)]
            if filters[len(filters) - 1].strip() == 'NULL':
                filterNum = None
            else:
                try:
                    filterNum = float(filters[len(filters) - 1].strip())
                except ValueError:
                    filterNum = filters[len(filters) - 1].strip()
            # Whenever the value is deleted, the index of the subsequent
            # values should change correspondingly
            i = 0
            self.__values = [list(x) for x in self.__values]
            while i < len(self.__values):
                if self.__values[i][self.__fieldName.index(filterField)] is not None:
                    try:
                        if filterNum != None:
                            value = float(self.__values[i][self.__fieldName.index(filterField)])
                    except ValueError:
                        if filterNum != None:
                            value = "'" + self.__values[i][self.__fieldName.index(filterField)] + "'"
                    if filters[1].strip().find(">") != -1:
                        # Every index after the deleted value should - 1
                        if value > filterNum:
                            self.__values.remove(self.__values[i])
                            i -= 1
                    elif filters[1].strip().find("<") != -1:
                        if value < filterNum:
                            self.__values.remove(self.__values[i])
                            i -= 1
                    elif filters[1].strip().find("=") != -1:
                        if value == filterNum:
                            self.__values.remove(self.__values[i])
                            i -= 1
                    elif filters[1].strip().find("!=") != -1:
                        if value != filterNum:
                            self.__values.remove(self.__values[i])
                            i -= 1
                    elif filters[1].strip().find("IS") != -1:
                        if filters[2].strip().find("NOT") != -1:
                            if value != filterNum:
                                self.__values.remove(self.__values[i])
                                i -= 1
                elif self.__values[i][self.__fieldName.index(filterField)] is None:
                    if filters[1].strip().find("IS") != -1 and filters[2].strip().find("NOT") == -1:
                        self.__values.remove(self.__values[i])
                        i -= 1
                i += 1
        else:
            self.__values = []

    # this is the "updateElement" function for table class
    def updateElement(self, valueset, condition):
        value = []
        setting = valueset.split(",")
        # Convert condition like: "a = b" to: [index of a, b]
        if condition != '':
            filters = condition.split(" ")
            filterField = filters[0].strip()
            if filterField.find(".") != -1:
                filterField = filterField[filterField.find(".") + 1:len(filterField)]
            if filters[len(filters) - 1].strip() == 'NULL':
                filterNum = None
            else:
                try:
                    filterNum = float(filters[len(filters) - 1].strip())
                except ValueError:
                    filterNum = filters[len(filters) - 1].strip()
        # Make the "SET" condition from  into a list
        for i in range(len(setting)):
            # The first element of the pair, is the field that is needed to be
            # updated, the second element is the value that the value of that
            # field is updated to
            pair = []
            s = setting[i].split("=")
            pair.append(self.__fieldName.index(s[0].strip()))
            try:
                integer = int(s[1].strip())
                pair.append(integer)
            except ValueError:
                try:
                    f = float(s[1].strip())
                    pair.append(f)
                except ValueError:
                    if s[1].strip() == 'NULL':
                        pair.append(None)
                    else:
                        pair.append(eval(s[1].strip()))
            value.append(pair)
        self.__values = [list(x) for x in self.__values]
        for x in self.__values:
            if condition == '':
                for pair in value:
                    x[pair[0]] = pair[1]
            else:
                if x[self.__fieldName.index(filterField)] is not None:
                    try:
                        f = float(x[self.__fieldName.index(filterField)])
                    except ValueError:
                        x[self.__fieldName.index(filterField)] = "'" + x[self.__fieldName.index(filterField)] + "'"
                    # update value
                    if filters[1].strip().find(">") != -1:
                        if x[self.__fieldName.index(filterField)] > filterNum:
                            x[pair[0]] = pair[1]
                    elif filters[1].strip().find("<") != -1:
                        if x[self.__fieldName.index(filterField)] < filterNum:
                            x[pair[0]] = pair[1]
                    elif filters[1].strip().find("=") != -1:
                        if x[self.__fieldName.index(filterField)] == filterNum:
                            x[pair[0]] = pair[1]
                    elif filters[1].strip().find("!=") != -1:
                        if x[self.__fieldName.index(filterField)] != filterNum:
                            x[pair[0]] = pair[1]
                    elif filters[1].strip().find("IS") != -1:
                        if filters[2].strip().find("NOT") != -1:
                            if x[self.__fieldName.index(filterField)] != filterNum:
                                x[pair[0]] = pair[1]
                        else:
                            if x[self.__fieldName.index(filterField)] == filterNum:
                                x[pair[0]] = pair[1]
                    try:
                        f = float(x[self.__fieldName.index(filterField)])
                    except ValueError:
                        x[self.__fieldName.index(filterField)] = eval(x[self.__fieldName.index(filterField)])
                elif x[self.__fieldName.index(filterField)] is None:
                    if filters[1].strip().find("IS") != -1 and filters[2].strip().find("NOT") == -1:
                        x[pair[0]] = pair[1]
        self.__values = [tuple(x) for x in self.__values]


# Get the name and type of the field of a table
class Field:
    def __init__(self, fieldType, fieldName):
        self.__type = fieldType
        self.__name = fieldName

    def getType(self):
        return self.__type

    def getName(self):
        return self.__name