import json
from pathlib import Path

from pypinyin import lazy_pinyin, Style

area = json.loads(Path('./area_code_2022.json').read_text(encoding='utf-8'))
# pprint(area)

ps = ('湖南省', '海南省', '广东省')
ls = {}
for prov in area:
    if prov['name'] not in ps:
        continue
    for a in prov['children']:
        ls[a['name'][:-1]] = ''.join(lazy_pinyin(a['name'][:-1], style=Style.NORMAL))
print(list(ls.keys()))
Path('./sds.json').write_text(json.dumps(ls, ensure_ascii=False), encoding='utf-8')



