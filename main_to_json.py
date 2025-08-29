import re
import logging
import os
import subprocess
import json
import glob
import shutil  # 用于删除目录
from merge_lineage import merge_lineage_jsons  # 合并工具
import csv  # 用于输出 CSV
# 预处理SQL内容
def preprocess_sql(sql_content):
    if not sql_content:
        return ""
    patterns = [
        (r'\bNOLOGGING\b', ''),
        (r'--.*$', '', re.MULTILINE),
        (r'#.*$', '', re.MULTILINE),
        (r'/\*.*?\*/', '', re.DOTALL),
        (r'（', '('),
        (r'）', ')'),
        (r'，', ','),
        (r'。', '.'),
    ]
    for pattern, replacement, *flags in patterns:
        sql_content = re.sub(pattern, replacement, sql_content, *flags)
    sql_content = "\n".join(
        line.strip() for line in sql_content.splitlines() if line.strip()
    )
    return sql_content.strip()

# 从INSERT语句提取表名
def extract_table_name_from_insert(insert_stmt):
    m = re.search(r'INSERT\s+INTO\s+([^\s(]+)', insert_stmt, re.IGNORECASE)
    return m.group(1) if m else None

# 提取所有INSERT语句
def extract_insert_statements(sql_content, src_file_name):
    pat = re.compile(r"INSERT\s+INTO\s+(?:[^']|'[^']*')*?;", re.IGNORECASE | re.DOTALL)
    insert_statements = []
    for match in pat.findall(sql_content):
        if not re.match(r'insert\s+into', match.strip(), re.IGNORECASE):
            logging.error(f"跳过非 INSERT 语句: {match.strip()[:100]}")
            continue
        table_name = extract_table_name_from_insert(match)
        if not table_name:
            logging.error(f"未能提取表名，跳过语句: {match.strip()[:100]}")
            continue
        start = sql_content.find(match)
        lineno = sql_content[:start].count('\n') + 1
        insert_statements.append({
            'table_name': table_name,
            'sql': match.strip(),
            'line_number': lineno,
            'src_file_name': src_file_name
        })
    logging.info(f"共提取到 {len(insert_statements)} 条 INSERT 语句。")
    return insert_statements

# 拆分成不超 max_length 的片段
def split_sql_chunks(insert_statements, max_length=10000):
    chunks, current = [], ""
    for stmt in insert_statements:
        snippet = stmt['sql'] + "\n"
        if len(current) + len(snippet) <= max_length:
            current += snippet
        else:
            chunks.append(current.strip())
            current = snippet
    if current:
        chunks.append(current.strip())
    logging.info(f"共拆分为 {len(chunks)} 个 SQL 片段，每个片段≤{max_length} 字符。")
    return chunks

# 加工单文件：读取、预处理、拆分
def process_and_split_sql(file_path, max_length=10000):
    with open(file_path, 'r', encoding='utf-8') as f:
        raw = f.read()
    logging.info("开始预处理SQL...")
    cleaned = preprocess_sql(raw)
    logging.info("提取INSERT语句...")
    inserts = extract_insert_statements(cleaned, file_path)
    logging.info("拆分SQL语句...")
    return split_sql_chunks(inserts, max_length)

# 遍历 chunk_dir 下 *.sql，执行 dlineage 并写 JSON
def generate_lineage_json(chunk_dir, db_type='oracle', dlineage_script='dlineage.py'):
    json_map = {}
    sql_files = sorted(glob.glob(os.path.join(chunk_dir, '*.sql')))
    if not sql_files:
        logging.warning(f"目录 {chunk_dir} 下未找到 .sql 文件。")
        return json_map

    for sql_file in sql_files:
        base = os.path.splitext(os.path.basename(sql_file))[0]
        out_json = os.path.join(chunk_dir, f"{base}.json")

        cmd = ['python3', dlineage_script, '/t', db_type, '/f', sql_file, '/json','/traceView']
        logging.info(f"处理 {sql_file} → {out_json}")
        proc = subprocess.run(cmd, capture_output=True, text=True)

        if proc.returncode != 0:
            logging.error(f"dlineage 执行失败 ({sql_file})，退出码 {proc.returncode}")
            logging.error(proc.stderr.strip())
            continue

        # 只提取第一个大括号包裹的 JSON 对象
        m = re.search(r'(\{.*\})', proc.stdout, re.DOTALL)
        if not m:
            logging.error(f"未找到 JSON 输出 ({sql_file})，原始 stdout:\n{proc.stdout[:200]}…")
            continue

        try:
            data = json.loads(m.group(1))
        except json.JSONDecodeError as e:
            logging.error(f"解析 JSON 失败 ({sql_file}): {e}")
            continue

        with open(out_json, 'w', encoding='utf-8') as jf:
            json.dump(data, jf, ensure_ascii=False, indent=2)
        logging.info(f"已写入 {out_json} (大小 {os.path.getsize(out_json)} 字节)")
        json_map[base] = data
    return json_map

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    src = 'test.sql'
    chunks = process_and_split_sql(src)

    base = os.path.splitext(os.path.basename(src))[0]
    out_dir = f"chunks"
    # 每次运行前删除旧的输出目录
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
    os.makedirs(out_dir, exist_ok=True)
    logging.info(f"输出目录: {os.path.abspath(out_dir)}")

    for idx, ch in enumerate(chunks, 1):
        p = os.path.join(out_dir, f"{base}_{idx}.sql")
        with open(p, 'w', encoding='utf-8') as f:
            f.write(ch)
        logging.info(f"已保存 {p} (长度 {len(ch)} 字符)")

    lineage = generate_lineage_json(out_dir, db_type='oracle', dlineage_script='dlineage.py')
    logging.info(f"共处理 {len(lineage)} 个片段，生成 JSON 对象。")
    # 4. 合并全局血缘模型并输出
    merged = merge_lineage_jsons(out_dir)
    with open('global_lineage.json', 'w', encoding='utf-8') as mf:
        json.dump(merged, mf, ensure_ascii=False, indent=2)
    logging.info("全局血缘模型已写入 global_lineage.json")