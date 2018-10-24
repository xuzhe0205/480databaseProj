import csv
input_ = ("""
    col1,col2,col3
    1,2,3
    4,5,6
    """)
key_list = []
input_ = input_.split()
csv_reader = csv.DictReader(input_, delimiter = ',')
cnt1 = 0
csv_dict = {}
val_cnt = 0
r_cnt1 = 0
val_list = []
for row in csv_reader:
    for key, value in row.items():
        if cnt1 == 0:
            key_list.append(key)
    cnt1 += 1    
    num_field = len(key_list)
    for key, value in row.items():
        if val_cnt < num_field:
            csv_dict[key] = [value]
        elif key in csv_dict:
            csv_dict[key].append(value)
        val_cnt += 1
        for dct in csv_reader:
            print (f"{dct}")
    r_cnt1 += 1
#print (csv_dict[key_list[0]][1])
quote = "'''"
field = ""
field = ",".join(csv_dict)
#print (field)
for key in csv_dict:
    csv_dict
    css = ",".join(map(str, csv_dict[key]))