import json
key_list = []
input_ = """
[{"col1": "1", "col2": "2", "col3": "3"}, {"col1": "4", "col2": "5", "col3": "6"}]
"""
json_list = json.loads(input_)
quote = '"""'
#for row in json_list:
#    for key in row:
#        row[key] = int(row[key])
#    json_str += json.dumps(row, sort_keys = True, indent = 4).strip("{").strip("}")
json_str = json.dumps(json_list)
result_str = quote+"\n"+json_str+"\n"+quote