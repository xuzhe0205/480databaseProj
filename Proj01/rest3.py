import csv
import io
input_ = ("""
    col1,col2,col3
    1,2,3
    4, ,6
    """)
#buff = StringIO(input_)
#reader = csv.reader(buff)
#for line in reader:
#    print(line)

in_list = [y for y in (x.strip() for x in input_.splitlines()) if y]
temp = []
for s in in_list:
    temp.append(s.split(','))
input_list = []
for ele in temp:
    input_list.append(','.join(ele))
#inpu_list = input_.split()
#f = io.StringIO()
#f.write(input_)
#s = f.getvalue()
#csv_str = csv.reader(s.splitlines())
##input_list = list (csv_str)
csv_list = [{k: v for k, v in row.items()} for row in csv.DictReader (input_list)]
#with open ("temp.csv","w") as temp_file:
#    temp_file.write("")
#with open ('temp.csv','w',newline = '') as new_in:
#rowcnt = 0
#output = io.StringIO()
#temp_str = ""
#value_list = []
#writer = csv.writer(output)
#cnt = 0
#i = 0
##Write field
#    #for k in key_list:
#    #    temp_str += k
#    #    if cnt < len(key_list) - 1:
#    #        temp_str += ","
#    #    cnt += 1
#writer.writerow(key_list) 
##Write value
#for row in csv_list:
#    for key in row:
##        if i < (len(key_list)):
#        value_list.append(row[key])
##        if i < (len(key_list) ):
##            value_list.append(",")
#        i +=1     
#    writer.writerow(value_list)
#    value_list = []
#result_str = output.getvalue()
#print (result_str)
file = io.StringIO()
writer = csv.DictWriter(file, sorted(csv_list[0]))
writer.writeheader()
writer.writerows(csv_list)
result_str = file.getvalue()