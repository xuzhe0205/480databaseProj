cnt1 = 0
    key_list = []
    for row in data:
        for key in row:
            if cnt1 == 0:
                key_list.append(key)
        cnt1 += 1
    output = io.StringIO()
    temp_str = ""
    value_list = []
    writer = csv.writer(output)
    cnt = 0
    i = 0
    writer.writerow(key_list) 
#Write value
    for row in data:
        for key in row:
    #        if i < (len(key_list)):
            value_list.append(row[key])
    #        if i < (len(key_list) ):
    #            value_list.append(",")
            i +=1     
        writer.writerow(value_list)
        value_list = []