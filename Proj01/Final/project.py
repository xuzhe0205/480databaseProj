"""
Project # 1 
Name: Zhe (Oliver) Xu
Time to completion: 16 hours
Comments:

Sources:
https://stackoverflow.com/questions/21572175/convert-csv-file-to-list-of-dictionaries#comment82378974_21572244
https://pymotw.com/2/xml/etree/ElementTree/create.html
https://stackoverflow.com/questions/7630273/convert-multiline-into-list
"""

# Importing useful module 
import csv
import json
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element,SubElement, tostring
import io

def read_csv_string(input_):
    """
    Takes a string which is the contents of a CSV file.
    Returns an object containing the data from the file.
    The specific representation of the object is up to you.
    The data object will be passed to the write_*_string functions.
    """
    in_list = [y for y in (x.strip() for x in input_.splitlines()) if y]
    temp = []
    #Split the modified string in the list by comma
    for s in in_list:                           
        temp.append(s.split(','))
    input_list = []
    for ele in temp:
        input_list.append(','.join(ele))
    #Parse the list of separate string, which representing each rows of input_ 
    #using csv DictReader
    #Return output as a list of dictionaries
    csv_list = [{k: v for k, v in row.items()} for row in csv.DictReader (input_list)]
    return csv_list


def write_csv_string(data):
    """
    Takes a data object (created by one of the read_*_string functions).
    Returns a string in the CSV format.
    """
    file = io.StringIO()
    writer = csv.DictWriter(file, sorted(data[0]))
    writer.writeheader()
    writer.writerows(data)
    result_str = file.getvalue()
    return result_str


def read_json_string(input_):
    """
    Similar to read_csv_string, except works for JSON files.
    """
    json_list = json.loads(input_)
    return json_list


def write_json_string(data):
    """
    Writes JSON strings. Similar to write_csv_string.
    """
    json_str = json.dumps(data)
    return json_str


def read_xml_string(input_):
    #Starting from the top to the bottom (outside to inside) of the tree, parse
    #input_ to a list of dictionaries
    xml_dict = {}
    xml_list = []
    nfield = 0
    cnt2 = 0
    rowcnt = 0
    finalrow = 0
    root = ET.fromstring(input_)
    for child in root:
        for ele in child:
            if finalrow == 1:
                nfield += 1
        finalrow += 1       
        
    for child in root:
        #if the value counter move to next row, row counter + 1
        if cnt2 == (rowcnt) * nfield :   
            rowcnt += 1
        for ele in child:
            xml_dict[ele.tag] = ele.text
            if cnt2 == (rowcnt) * nfield - 1:
                xml_list.append(xml_dict)
                xml_dict = {}
            cnt2 +=1
    return xml_list


def write_xml_string(data):
    #write data, a list of dictionaries to a xml string
    top = Element ("data")
    parent = SubElement(top, "record")
    for xml_dict in data:
        for key in xml_dict:
            children = SubElement(parent,key)
            children.text = xml_dict[key]
        if xml_dict == data[-1]:
            break
        parent = SubElement(top, "record")
    #convert byte string to normal string
    result_str = tostring(top,encoding="unicode")
    return result_str



# The code below isn't needed, but may be helpful in testing your code.
if __name__ == "__main__":
    input_ = """
    col1,col2,col3
    1,2,3
    4,5,6
    """
    expected = """
    [{"col1": "1", "col2": "2", "col3": "3"}, {"col1": "4", "col2": "5", "col3": "6"}]
    """

    def super_strip(input_):
        """
        Removes all leading/trailing whitespace and blank lines
        """
        lines = []
        for line in input_.splitlines():
            stripped = line.strip()
            if stripped:
                lines.append(stripped)
        return "\n".join(lines) + "\n"

    input_ = super_strip(input_)
    expected = super_strip(expected)

    print("Input:")
    print(input_)
    print()
    data = read_csv_string(input_)
    print("Your data object:")
    print(data)
    print()
    output = write_json_string(data)
    output = super_strip(output)
    print("Output:")
    print(output)
    print()
