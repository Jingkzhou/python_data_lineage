import re
import logging
import os
import subprocess
import glob
import shutil
import csv
import pymysql

# ---------- 1. 预处理 SQL ----------
def preprocess_sql(sql_content: str) -> str:
    """去注释、全角转半角、规范标点"""
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
    for pat, repl, *flags in patterns:
        sql_content = re.sub(pat, repl, sql_content, *flags)
    # 去掉空行并 trim
    lines = [ln.strip() for ln in sql_content.splitlines() if ln.strip()]
    return "\n".join(lines)

# ---------- 2. 提取 INSERT 语句 ----------
def extract_table_name_from_insert(stmt: str) -> str:
    m = re.search(r'INSERT\s+INTO\s+([^\s(]+)', stmt, re.IGNORECASE)
    return m.group(1) if m else None

def extract_insert_statements(sql: str) -> list[dict]:
    """返回 [{'table_name':..., 'sql':..., 'line_number':...}, ...]"""
    pat = re.compile(r"INSERT\s+INTO\s+(?:[^']|'[^']*')*?;", re.IGNORECASE | re.DOTALL)
    results = []
    for match in pat.findall(sql):
        tbl = extract_table_name_from_insert(match)
        if not tbl:
            continue
        start = sql.find(match)
        lineno = sql[:start].count('\n') + 1
        results.append({
            'table_name': tbl,
            'sql': match.strip(),
            'line_number': lineno
        })
    logging.info(f"提取到 {len(results)} 条 INSERT 语句。")
    return results

# ---------- 3. 拆分 SQL 片段 ----------
def split_sql_chunks(inserts: list[dict], max_len: int = 10000) -> list[str]:
    chunks, current = [], ""
    for item in inserts:
        logging.info(f"长度为： {len(item['sql'] )} ")
   
        seg = item['sql'] + "\n"
        if len(current) + len(seg) <= max_len:
            current += seg
        else:
            chunks.append(current.strip())
            current = seg
    if current:
        chunks.append(current.strip())
    logging.info(f"拆分为 {len(chunks)} 段，每段≤{max_len} 字符。")
    return chunks

# ---------- 4. 调用 dlineage.py 生成单段 CSV ----------
def generate_chunk_csvs(chunk_dir: str,
                        db_type: str = 'mysql',
                        dlineage_script: str = 'dlineage.py') -> None:
    sql_files = sorted(glob.glob(os.path.join(chunk_dir, '*.sql')))
    if not sql_files:
        logging.warning(f"{chunk_dir} 下未找到任何 .sql 文件。")
        return

    for sql_file in sql_files:
        base = os.path.splitext(os.path.basename(sql_file))[0]
        out_csv = os.path.join(chunk_dir, f"{base}.csv")

        cmd = ['python3', dlineage_script,
               '/t', db_type,
               '/f', sql_file,
               '/csv','/traceView']  # 直接请求 CSV 输出
        logging.info(f"运行：{' '.join(cmd)}")
        proc = subprocess.run(cmd, capture_output=True, text=True)

        if proc.returncode != 0:
            logging.error(f"dlineage 失败 ({sql_file})，stderr: {proc.stderr.strip()}")
            continue

        # 将 stdout（CSV 文本）写入文件
        with open(out_csv, 'w', encoding='utf-8') as f:
            f.write(proc.stdout)
        logging.info(f"已生成 CSV：{out_csv}")

# ---------- 5. 合并所有段 CSV 为 global_lineage.csv ----------
def merge_csvs(chunk_dir: str, output_csv: str) -> None:
    csv_files = sorted(glob.glob(os.path.join(chunk_dir, '*.csv')))
    if not csv_files:
        logging.warning("无可合并的 CSV 文件。")
        return
    header_written = False
    with open(output_csv, 'w', newline='', encoding='utf-8') as out_f:
        writer = None
        for csv_file in csv_files:
            with open(csv_file, 'r', encoding='utf-8') as in_f:
                reader = csv.reader(in_f)
                try:
                    header = next(reader)
                except StopIteration:
                    continue
                if not header_written:
                    writer = csv.writer(out_f)
                    writer.writerow(header)
                    header_written = True
                for row in reader:
                    writer.writerow(row)
    logging.info(f"合并 CSV 完成：{output_csv}")

# ---------- 主流程 ----------
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")

    sql_dir = 'sql'
    chunk_dir = 'chunks'
    if os.path.exists(chunk_dir):
        shutil.rmtree(chunk_dir)
    os.makedirs(chunk_dir, exist_ok=True)

    sql_files = sorted(glob.glob(os.path.join(sql_dir, '*.sql')))
    for src_sql in sql_files:
        try:
            # 尝试GB18030编码（中文国标编码）
            raw = open(src_sql, encoding='gb18030').read()
        except UnicodeDecodeError:
            try:
                # 如果GB18030失败，尝试GBK
                raw = open(src_sql, encoding='gbk').read()
            except UnicodeDecodeError:
                # 最后尝试UTF-8
                raw = open(src_sql, encoding='utf-8').read()
        
        cleaned = preprocess_sql(raw)
        inserts = extract_insert_statements(cleaned)
        chunks = split_sql_chunks(inserts)

        base_name = os.path.splitext(os.path.basename(src_sql))[0]
        for idx, seg in enumerate(chunks, start=1):
            path = os.path.join(chunk_dir, f"{base_name}_{idx}.sql")
            with open(path, 'w', encoding='utf-8') as f:
                f.write(seg)
            logging.info(f"已保存：{path}")

    # 生成每段 CSV
    generate_chunk_csvs(chunk_dir,
                        db_type='oracle',
                        dlineage_script='dlineage.py')

    
    # MySQL 连接配置
    conn = pymysql.connect(
        host='127.0.0.1',
        user='root',
        password='a8548879',
        database='lineage',
        charset='utf8mb4'
    )
    cursor = conn.cursor()

    csv_files = glob.glob('chunks/*.csv')
    # 插入前清空表
    cursor.execute("TRUNCATE TABLE lineage_table;")
    conn.commit()
    print("lineage_table 已清空，开始批量导入...")
    csv_files = glob.glob('chunks/*.csv')
    for csv_file in csv_files:
        base = os.path.splitext(os.path.basename(csv_file))[0]
        sql_file = os.path.join('chunks', base + '.sql')
        with open(sql_file, 'r', encoding='utf-8') as f_sql:
            sql_content = f_sql.read()
        with open(csv_file, 'r', encoding='utf-8') as f_csv:
            reader = csv.reader(f_csv)
            header = next(reader)
            # 明确插入顺序为表的所有字段顺序
            fields = [
                'SOURCE_DB', 'SOURCE_SCHEMA', 'SOURCE_TABLE_ID', 'SOURCE_TABLE',
                'SOURCE_COLUMN_ID', 'SOURCE_COLUMN', 'TARGET_DB', 'TARGET_SCHEMA',
                'TARGET_TABLE_ID', 'TARGET_TABLE', 'TARGET_COLUMN_ID', 'TARGET_COLUMN',
                'RELATION_TYPE', 'EFFECTTYPE', 'SQL_TEXT', 'FILE_NAME'
            ]
            placeholders = ','.join(['%s'] * len(fields))
            insert_sql = f"INSERT INTO lineage_table ({','.join(fields)}) VALUES ({placeholders})"
            for row in reader:
                # row和字段严格对齐
                # 如果遇到空行（或者全字段为空），跳出当前文件的插入循环
                if not row or all(cell.strip() == '' for cell in row):
                    print(f"遇到空行，跳过整个文件：{csv_file}")
                    break   # 跳出本文件循环，继续下一个csv文件
                ext_row = row + [sql_content, os.path.basename(sql_file)]
                if len(ext_row) != len(fields):
                    print(f"字段数量不一致，跳过: {csv_file} 行内容: {ext_row}")
                    continue
                try:
                    cursor.execute(insert_sql, ext_row)
                except Exception as e:
                    print(f"插入出错: {e}\nSQL: {insert_sql}\n数据: {ext_row}")
                    continue
        print(f"{os.path.basename(csv_file)} 导入完成")

        conn.commit()
    cursor.close()
    conn.close()   