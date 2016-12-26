# coding=utf-8

content_list = [{'key4': 200, 'key2': 3000, 'key1': 1}, {'key3': 200, 'key2': 3000, 'key1': 1}]

result = {}

for i in range(len(content_list)):
    for s in content_list[i]:
        if s in result.keys():
            result[s] += content_list[i][s]
    else:
        result[s] = content_list[i][s]

print(result)
