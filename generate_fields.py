import json, csv, glob

# 1. 读取所有 JSON，收集所有节点名称
nodes = set()
for fn in glob.glob("./chunks/*.json"):
    data = json.load(open(fn, encoding='utf-8'))
    for r in data["relationships"]:
        # 目标字段
        tgt = f"{r['target']['parentName']}.{r['target']['column']}"
        nodes.add(tgt)
        # 源字段
        for src in r["sources"]:
            s = f"{src['parentName']}.{src['column']}"
            nodes.add(s)

# 2. 写入 fields.csv
with open("fields.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["name"])         # 必需的列头
    for name in sorted(nodes):
        writer.writerow([name])

print(f"Generated fields.csv with {len(nodes)} rows.")
