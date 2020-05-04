import json

from DistributedSpiders.items import DistributedSpidersItem


class ItemEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, DistributedSpidersItem):
            tmp = {'mark': o["mark"], 'data': o["data"]}
            return json.dumps(tmp, ensure_ascii=False)





