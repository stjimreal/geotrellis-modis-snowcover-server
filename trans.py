'''
Date: 2022-04-21 21:17:12
LastEditors: LIULIJING
LastEditTime: 2022-04-21 21:32:08
'''
import json

filename = 'src/main/resources/World_Countries.json'
output = 'src/main/resources/World_Countries.json'
with open(filename) as f:
    d = json.load(f)
    len1 = len(d['features'])
    set1 = set()
    for feature in d['features']:
        set1.add(feature['properties']['COUNTRY'])
    print(len1, len(set1))
    
# with open(output, 'w', encoding='utf-8') as fout:
#     json.dump(d, fout, ensure_ascii=False)