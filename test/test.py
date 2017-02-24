# coding=utf-8

import csv
import os
import os.path
root_dir = "C:\\Users\\Ben\\Desktop\\origin_data\\"


file_list = os.listdir(root_dir)
for file_name in file_list:
    file_name_all = root_dir + file_name
    print(file_name_all)
    reader = csv.reader(file(file_name_all, 'rb'))
    writer = csv.writer(file(root_dir + 'sss.csv', 'ab'))
    # 日期、开、高、低、收、成交、持仓
    writer.writerow(['futuresID', 'date', 'open', 'high', 'low', 'close', 'turnover', 'position'])

    for line in reader:
        line[1:] = line[:]
        line[0] = file_name[:4]
        line[1] = line[1][:4] + '-' + line[1][4:6] + '-' + line[1][-2:]
        # print(line)
        writer.writerow(line)


