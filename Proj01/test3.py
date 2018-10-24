import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element,SubElement, tostring
input_ = """
<data><record><col1>1</col1><col2>2</col2><col3>3</col3></record><record><col1>4</col1><col2>5</col2><col3>6</col3></record></data>
"""
xml_dict = {}
xml_list = []
nfield = 0
cnt2 = 0
fieldcnt = 0
rowcnt = 0
finalrow = 0
root = ET.fromstring(input_)
key_list = []
for child in root:
    for ele in child:
        if finalrow == 1:
            nfield += 1
            key_list.append(ele.tag)
    finalrow += 1       
    
for child in root:
    
    if cnt2 == (rowcnt) * nfield : #if the value counter move to next row, row counter + 1
        rowcnt += 1
    for ele in child:

        xml_dict[ele.tag] = ele.text
        if cnt2 == (rowcnt) * nfield - 1:
            xml_list.append(xml_dict)
            xml_dict = {}
        cnt2 +=1
    

print (xml_list)
print ("---------------------------------------")
top = Element ("data")
parent = SubElement(top, "record")
cnt = 0
for xml_d in xml_list:
    for key in xml_d:
        children = SubElement(parent,key)
        children.text = xml_d[key]
    if xml_d == xml_list[-1]:
        break
    parent = SubElement(top, "record")
result_str = tostring(top,encoding="unicode")
print (result_str)