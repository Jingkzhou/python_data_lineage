import re
import logging
import os
import subprocess
import glob
import shutil
import csv
import pymysql

SQLFLOW_CHAR_LIMIT = int(os.getenv("SQLFLOW_CHAR_LIMIT", "10000"))
EXPECTED_LINEAGE_COLUMNS = 14

_FULLWIDTH_TRANS = str.maketrans({
    '（': '(',
    '）': ')',
    '，': ',',
    '。': '.',
})


def _strip_inline_comment(line: str) -> str:
    in_single = False
    in_double = False
    i = 0
    length = len(line)
    while i < length:
        ch = line[i]
        if ch == "'" and not in_double:
            if in_single and i + 1 < length and line[i + 1] == "'":
                i += 2
                continue
            in_single = not in_single
            i += 1
            continue
        if ch == '"' and not in_single:
            if in_double and i + 1 < length and line[i + 1] == '"':
                i += 2
                continue
            in_double = not in_double
            i += 1
            continue
        if not in_single and not in_double:
            if ch == '-' and i + 1 < length and line[i + 1] == '-':
                return line[:i]
            if ch == '#' and line[:i].strip() == '':
                return line[:i]
        i += 1
    return line

# ---------- 1. 预处理 SQL ----------
def preprocess_sql(sql_content: str) -> str:
    """去注释、全角转半角、规范标点"""
    if not sql_content:
        return ""
    sql_content = sql_content.translate(_FULLWIDTH_TRANS)
    sql_content = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)
    sql_content = re.sub(r'\bNOLOGGING\b', '', sql_content, flags=re.IGNORECASE)

    lines = []
    for raw in sql_content.splitlines():
        stripped = raw.strip()
        if not stripped:
            continue
        if stripped.startswith('#'):
            continue
        cleaned = _strip_inline_comment(raw).strip()
        if cleaned:
            lines.append(cleaned)
    return "\n".join(lines)

# ---------- 2. 提取 INSERT 语句 ----------
def extract_table_name_from_insert(stmt: str) -> str:
    m = re.search(r'INSERT\s+INTO\s+([^\s(]+)', stmt, re.IGNORECASE)
    return m.group(1) if m else None

def extract_table_name_from_create(stmt: str) -> str:
    m = re.search(
        r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:TEMPORARY\s+|TEMP\s+|EXTERNAL\s+)?([^\s(]+)',
        stmt,
        re.IGNORECASE
    )
    return m.group(1) if m else None

def extract_insert_statements(sql: str) -> list[dict]:
    """返回 [{'table_name':..., 'sql':..., 'line_number':...}, ...]"""
    pat = re.compile(r"INSERT\s+INTO\s+(?:[^']|'[^']*')*?;", re.IGNORECASE | re.DOTALL)
    results = []
    for m in pat.finditer(sql):
        match = m.group(0)
        tbl = extract_table_name_from_insert(match)
        if not tbl:
            continue
        lineno = sql[:m.start()].count('\n') + 1
        results.append({
            'table_name': tbl,
            'sql': match.strip(),
            'line_number': lineno,
            'statement_type': 'insert'
        })
    logging.info(f"提取到 {len(results)} 条 INSERT 语句。")
    return results

CREATE_TABLE_AS_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:TEMPORARY\s+|TEMP\s+|EXTERNAL\s+)?"
    r"(?:[^;']|'[^']*')*?\bAS\s+(?:WITH\b(?:[^;']|'[^']*')*?SELECT|SELECT)"
    r"(?:[^;']|'[^']*')*?;",
    re.IGNORECASE | re.DOTALL
)

def extract_create_table_as_statements(sql: str) -> list[dict]:
    """返回 CREATE TABLE ... AS 语句列表"""
    results = []
    for m in CREATE_TABLE_AS_RE.finditer(sql):
        match = m.group(0)
        tbl = extract_table_name_from_create(match)
        if not tbl:
            continue
        lineno = sql[:m.start()].count('\n') + 1
        results.append({
            'table_name': tbl,
            'sql': match.strip(),
            'line_number': lineno,
            'statement_type': 'create_table_as'
        })
    logging.info(f"提取到 {len(results)} 条 CREATE TABLE ... AS 语句。")
    return results


def _split_by_top_level_commas(text: str) -> list[str]:
    parts = []
    current = []
    depth = 0
    in_single = False
    in_double = False
    i = 0
    length = len(text)
    while i < length:
        ch = text[i]
        if ch == "'" and not in_double:
            current.append(ch)
            if in_single and i + 1 < length and text[i + 1] == "'":
                i += 1
                current.append(text[i])
            else:
                in_single = not in_single
            i += 1
            continue
        if ch == '"' and not in_single:
            current.append(ch)
            if in_double and i + 1 < length and text[i + 1] == '"':
                i += 1
                current.append(text[i])
            else:
                in_double = not in_double
            i += 1
            continue
        if not in_single and not in_double:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth = max(depth - 1, 0)
            elif ch == ',' and depth == 0:
                parts.append(''.join(current).strip())
                current = []
                i += 1
                while i < length and text[i].isspace():
                    i += 1
                continue
        current.append(ch)
        i += 1
    if current:
        parts.append(''.join(current).strip())
    return [part for part in parts if part]


def _find_char_outside_quotes(text: str, char: str, start: int = 0) -> int:
    in_single = False
    in_double = False
    i = start
    length = len(text)
    while i < length:
        ch = text[i]
        if ch == "'" and not in_double:
            if in_single and i + 1 < length and text[i + 1] == "'":
                i += 2
                continue
            in_single = not in_single
            i += 1
            continue
        if ch == '"' and not in_single:
            if in_double and i + 1 < length and text[i + 1] == '"':
                i += 2
                continue
            in_double = not in_double
            i += 1
            continue
        if not in_single and not in_double and ch == char:
            return i
        i += 1
    return -1


def _find_matching_paren(text: str, start: int) -> int:
    depth = 0
    in_single = False
    in_double = False
    for i in range(start, len(text)):
        ch = text[i]
        if ch == "'" and not in_double:
            if in_single and i + 1 < len(text) and text[i + 1] == "'":
                continue
            in_single = not in_single
            continue
        if ch == '"' and not in_single:
            if in_double and i + 1 < len(text) and text[i + 1] == '"':
                continue
            in_double = not in_double
            continue
        if in_single or in_double:
            continue
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
            if depth == 0:
                return i
    return -1


def _find_top_level_keyword(text: str, keyword: str, start: int = 0) -> int:
    keyword_upper = keyword.upper()
    text_upper = text.upper()
    in_single = False
    in_double = False
    depth = 0
    i = start
    while i < len(text):
        ch = text[i]
        if ch == "'" and not in_double:
            if in_single and i + 1 < len(text) and text[i + 1] == "'":
                i += 2
                continue
            in_single = not in_single
            i += 1
            continue
        if ch == '"' and not in_single:
            if in_double and i + 1 < len(text) and text[i + 1] == '"':
                i += 2
                continue
            in_double = not in_double
            i += 1
            continue
        if in_single or in_double:
            i += 1
            continue
        if ch == '(':
            depth += 1
            i += 1
            continue
        if ch == ')':
            depth = max(depth - 1, 0)
            i += 1
            continue
        if depth == 0 and text_upper.startswith(keyword_upper, i):
            before = text_upper[i - 1] if i > 0 else ' '
            after = text_upper[i + len(keyword)] if i + len(keyword) < len(text) else ' '
            if not before.isalnum() and before != '_' and not after.isalnum() and after != '_':
                return i
        i += 1
    return -1


def _split_select_clause(select_sql: str) -> tuple[str, str] | tuple[None, None]:
    sql = select_sql.strip()
    if not sql.upper().startswith('SELECT'):
        return None, None
    from_idx = _find_top_level_keyword(sql, 'FROM', start=6)
    if from_idx == -1:
        return None, None
    projections = sql[6:from_idx].strip()
    tail = sql[from_idx:].strip()
    return projections, tail


VALUES_RE = re.compile(r'^(?P<header>INSERT\s+INTO\s+.+?\bVALUES\b)(?P<body>.+)$',
                       re.IGNORECASE | re.DOTALL)


def _split_values_groups(values_section: str) -> list[str]:
    text = values_section.strip()
    if text.endswith(';'):
        text = text[:-1].rstrip()
    groups = []
    current = []
    depth = 0
    in_single = False
    in_double = False
    i = 0
    length = len(text)
    while i < length:
        ch = text[i]
        if ch == "'" and not in_double:
            current.append(ch)
            if in_single and i + 1 < length and text[i + 1] == "'":
                i += 1
                current.append(text[i])
            else:
                in_single = not in_single
            i += 1
            continue
        if ch == '"' and not in_single:
            current.append(ch)
            if in_double and i + 1 < length and text[i + 1] == '"':
                i += 1
                current.append(text[i])
            else:
                in_double = not in_double
            i += 1
            continue
        if not in_single and not in_double:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth = max(depth - 1, 0)
            elif ch == ',' and depth == 0:
                groups.append(''.join(current).strip())
                current = []
                i += 1
                while i < length and text[i].isspace():
                    i += 1
                continue
        current.append(ch)
        i += 1
    if current:
        groups.append(''.join(current).strip())
    return [grp for grp in groups if grp]


def _split_insert_prefix(insert_sql: str) -> tuple[str, str]:
    stmt = insert_sql.strip()
    positions = []
    for keyword in ('VALUES', 'WITH', 'SELECT'):
        pos = _find_top_level_keyword(stmt, keyword)
        if pos != -1:
            positions.append((pos, keyword))
    if not positions:
        return stmt, ''
    pos, _ = min(positions, key=lambda item: item[0])
    prefix = stmt[:pos].rstrip()
    rest = stmt[pos:].lstrip()
    return prefix, rest


def _split_select_union_all(select_section: str) -> list[str]:
    text = select_section.strip()
    if not text:
        return [text]
    upper = text.upper()
    if upper.startswith('VALUES'):
        return [text]
    with_clause = ''
    select_part = text
    if upper.startswith('WITH'):
        select_idx = _find_top_level_keyword(text, 'SELECT')
        if select_idx == -1:
            return [text]
        with_clause = text[:select_idx].strip()
        select_part = text[select_idx:].strip()
    segments = []
    current = []
    depth = 0
    in_single = False
    in_double = False
    i = 0
    upper_select = select_part.upper()
    while i < len(select_part):
        if not in_single and not in_double and depth == 0 and upper_select.startswith('UNION ALL', i):
            segments.append(''.join(current).strip())
            current = []
            i += len('UNION ALL')
            while i < len(select_part) and select_part[i].isspace():
                i += 1
            continue
        ch = select_part[i]
        if ch == "'" and not in_double:
            current.append(ch)
            if in_single and i + 1 < len(select_part) and select_part[i + 1] == "'":
                i += 1
                current.append(select_part[i])
            else:
                in_single = not in_single
            i += 1
            continue
        if ch == '"' and not in_single:
            current.append(ch)
            if in_double and i + 1 < len(select_part) and select_part[i + 1] == '"':
                i += 1
                current.append(select_part[i])
            else:
                in_double = not in_double
            i += 1
            continue
        if not in_single and not in_double:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth = max(depth - 1, 0)
        current.append(ch)
        i += 1
    if current:
        segments.append(''.join(current).strip())
    if with_clause:
        return [f"{with_clause} {segment}".strip() for segment in segments if segment]
    return [segment for segment in segments if segment]


def _parse_insert_with_columns(insert_sql: str) -> dict | None:
    stmt = insert_sql.strip()
    into_idx = stmt.upper().find('INTO')
    paren_start = _find_char_outside_quotes(stmt, '(', into_idx if into_idx != -1 else 0)
    if paren_start == -1:
        return None
    paren_end = _find_matching_paren(stmt, paren_start)
    if paren_end == -1:
        return None
    prefix = stmt[:paren_start].strip()
    columns_text = stmt[paren_start + 1:paren_end].strip()
    columns = _split_by_top_level_commas(columns_text)
    if not columns:
        return None
    rest = stmt[paren_end + 1:].strip()
    upper_rest = rest.upper()
    if upper_rest.startswith('VALUES'):
        body = rest[6:].strip()
        groups = _split_values_groups(body)
        if not groups:
            return None
        rows = []
        for group in groups:
            grp = group.strip()
            if grp.endswith(';'):
                grp = grp[:-1].strip()
            if grp.startswith('(') and grp.endswith(')'):
                inner = grp[1:-1].strip()
                values = _split_by_top_level_commas(inner)
                rows.append(values)
            else:
                return None
        if not all(len(row) == len(columns) for row in rows):
            return None
        return {
            'type': 'values',
            'prefix': prefix,
            'columns': columns,
            'rows': rows
        }
    select_idx = _find_top_level_keyword(rest, 'SELECT')
    if select_idx == -1:
        return None
    leading = rest[:select_idx].strip()
    select_body = rest[select_idx:].strip()
    projections_text, tail = _split_select_clause(select_body)
    if projections_text is None or tail is None:
        return None
    projections = _split_by_top_level_commas(projections_text)
    if len(projections) != len(columns):
        return None
    return {
        'type': 'select',
        'prefix': prefix,
        'columns': columns,
        'leading': leading,
        'projections': projections,
        'tail': tail
    }


def _split_insert_values(insert_sql: str, max_len: int) -> list[str]:
    stmt = insert_sql.strip().rstrip(';')
    m = VALUES_RE.match(stmt)
    if not m:
        return [stmt + ';']
    header = m.group('header').strip()
    body = m.group('body').strip()
    groups = _split_values_groups(body)
    if len(groups) <= 1:
        return [stmt + ';']
    results = []
    for group in groups:
        candidate = f"{header} {group.strip().rstrip(',')};"
        results.append(candidate)
    return results


def _split_insert_columns(insert_sql: str, max_len: int) -> list[str]:
    parsed = _parse_insert_with_columns(insert_sql)
    if not parsed:
        return [insert_sql.strip().rstrip(';') + ';']
    columns = parsed['columns']
    if len(columns) <= 1:
        return [insert_sql.strip().rstrip(';') + ';']

    def build_values_stmt(idxs: list[int]) -> str:
        cols = ', '.join(columns[i] for i in idxs)
        row_texts = []
        for row in parsed['rows']:
            values = ', '.join(row[i] for i in idxs)
            row_texts.append(f"({values})")
        return f"{parsed['prefix']} ({cols}) VALUES {', '.join(row_texts)};"

    def build_select_stmt(idxs: list[int]) -> str:
        cols = ', '.join(columns[i] for i in idxs)
        projections = ', '.join(parsed['projections'][i] for i in idxs)
        select_parts = []
        if parsed.get('leading'):
            select_parts.append(parsed['leading'])
        select_parts.append(f"SELECT {projections}")
        select_parts.append(parsed['tail'])
        select_clause = ' '.join(part for part in select_parts if part).strip()
        return f"{parsed['prefix']} ({cols}) {select_clause};"

    build_stmt = build_values_stmt if parsed['type'] == 'values' else build_select_stmt
    count = len(columns)
    results = []
    start = 0
    while start < count:
        idxs: list[int] = []
        for idx in range(start, count):
            candidate = idxs + [idx]
            sql_candidate = build_stmt(candidate)
            if len(sql_candidate) > max_len:
                if not idxs:
                    logging.error(f"单列 SQL 长度仍超过 {max_len} 字符，无法拆分。")
                    return [insert_sql.strip().rstrip(';') + ';']
                break
            idxs = candidate
        if not idxs:
            break
        results.append(build_stmt(idxs))
        start = idxs[-1] + 1
    if not results:
        return [insert_sql.strip().rstrip(';') + ';']
    return results


def _split_insert_union_all(insert_sql: str, max_len: int) -> list[str]:
    stmt = insert_sql.strip().rstrip(';')
    if 'UNION ALL' not in stmt.upper():
        return [stmt + ';']
    prefix, rest = _split_insert_prefix(stmt)
    if not rest or rest.upper().startswith('VALUES'):
        return [stmt + ';']
    segments = _split_select_union_all(rest)
    if len(segments) <= 1:
        return [stmt + ';']
    results = []
    for segment in segments:
        candidate = f"{prefix} {segment.strip()};"
        results.append(candidate)
    return results


def split_insert_statement(insert_sql: str, max_len: int) -> list[str]:
    normalized = insert_sql.strip()
    if not normalized.endswith(';'):
        normalized = normalized.rstrip(';').strip() + ';'
    queue = [normalized]
    output = []
    while queue:
        current = queue.pop(0).strip()
        if len(current) <= max_len:
            output.append(current)
            continue
        base = current.rstrip(';').strip()
        splitted = None
        for splitter in (_split_insert_values, _split_insert_union_all, _split_insert_columns):
            logging.info(f"SQL 长度 {len(current)} 超过 {max_len}，尝试 {splitter.__name__} 拆分。")
            pieces = splitter(base, max_len)
            cleaned = []
            for piece in pieces:
                piece_norm = piece.strip()
                if not piece_norm.endswith(';'):
                    piece_norm = piece_norm.rstrip(';').strip() + ';'
                cleaned.append(piece_norm)
            if len(cleaned) == 1 and cleaned[0].strip().rstrip(';') == base:
                continue
            if cleaned:
                splitted = cleaned
                break
        if splitted:
            queue = splitted + queue
            logging.info(f"{splitter.__name__} 将语句拆分为 {len(splitted)} 段。")
            continue
        logging.error(f"无法控制 SQL 在 {max_len} 字符内，已跳过片段：{current[:200]}...")
    return output

# ---------- 3. 拆分 SQL 片段 ----------
def split_sql_chunks(statements: list[dict], max_len: int = SQLFLOW_CHAR_LIMIT) -> list[str]:
    chunks: list[str] = []
    for item in statements:
        stmt_type = item.get('statement_type', 'insert')
        raw_sql = item['sql']
        if stmt_type == 'insert':
            pieces = split_insert_statement(raw_sql, max_len=max_len)
            if not pieces:
                logging.error(f"INSERT 语句拆分失败（行 {item.get('line_number')}）。")
                continue
        else:
            normalized = raw_sql.strip()
            if not normalized.endswith(';'):
                normalized = normalized.rstrip(';').strip() + ';'
            if len(normalized) > max_len:
                logging.warning(
                    f"语句长度 {len(normalized)} 超过 {max_len} 字符（行 {item.get('line_number')}，类型 {stmt_type}）。保留原语句。"
                )
            pieces = [normalized]
        for stmt in pieces:
            stmt_text = stmt if stmt.endswith('\n') else f"{stmt}\n"
            chunks.append(stmt_text)
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

        stdout = proc.stdout
        marker = 'Error log:'
        idx = stdout.find(marker)
        if idx != -1:
            # dlineage 会在 CSV 结果后追加错误日志，这里截断掉日志部分
            stdout = stdout[:idx].rstrip('\r\n')
            if stdout:
                stdout += '\n'

        with open(out_csv, 'w', encoding='utf-8') as f:
            f.write(stdout)
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


def export_result_csvs(chunk_dir: str, result_dir: str = 'result') -> None:
    csv_files = sorted(glob.glob(os.path.join(chunk_dir, '*.csv')))
    if not csv_files:
        logging.warning("无可导出的 CSV。")
        return

    os.makedirs(result_dir, exist_ok=True)
    groups: dict[str, list[tuple[int, str]]] = {}
    for path in csv_files:
        base = os.path.splitext(os.path.basename(path))[0]
        prefix, _, suffix = base.rpartition('_')
        if suffix.isdigit():
            key = prefix
            order = int(suffix)
        else:
            key = base
            order = 0
        groups.setdefault(key, []).append((order, path))

    for key, entries in groups.items():
        entries.sort(key=lambda item: item[0])
        out_path = os.path.join(result_dir, f"{key}.csv")
        if os.path.exists(out_path):
            os.remove(out_path)

        writer = None
        out_f = None
        try:
            for _, src_path in entries:
                base_name = os.path.splitext(os.path.basename(src_path))[0]
                sql_path = os.path.join(chunk_dir, f"{base_name}.sql")
                sql_text = ''
                if os.path.exists(sql_path):
                    with open(sql_path, 'r', encoding='utf-8') as sql_f:
                        sql_text = sql_f.read().strip()

                with open(src_path, 'r', encoding='utf-8') as src_f:
                    reader = csv.reader(src_f)
                    try:
                        header = next(reader)
                    except StopIteration:
                        continue
                    if writer is None:
                        out_f = open(out_path, 'w', newline='', encoding='utf-8')
                        writer = csv.writer(out_f)
                        writer.writerow(header + ['SQL_TEXT'])
                    for row in reader:
                        if not row or all(cell.strip() == '' for cell in row):
                            continue
                        writer.writerow(row + [sql_text])
        finally:
            if out_f:
                out_f.close()
        if writer:
            logging.info(f"已生成合并 CSV：{out_path}")


def _normalize_lineage_row(row: list[str]) -> list[str] | None:
    """dlineage 导出的 CSV 使用反引号包裹表达式，csv.reader 会把表达式拆成多列，这里合并回单列。"""
    if len(row) == EXPECTED_LINEAGE_COLUMNS:
        return row
    if len(row) > EXPECTED_LINEAGE_COLUMNS >= 3:
        prefix = row[:11]
        suffix = row[-2:]
        middle = ','.join(row[11:-2]).strip()
        fixed = prefix + [middle] + suffix
        if len(fixed) == EXPECTED_LINEAGE_COLUMNS:
            return fixed
    logging.warning(f"无法纠正 CSV 行列数：{len(row)} 列，期望 {EXPECTED_LINEAGE_COLUMNS}。数据：{row}")
    return None

# ---------- 主流程 ----------
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")
    logging.info(f"单条 SQL 长度限制：{SQLFLOW_CHAR_LIMIT} 字符。")

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
        insert_statements = extract_insert_statements(cleaned)
        create_statements = extract_create_table_as_statements(cleaned)
        statements = sorted(
            insert_statements + create_statements,
            key=lambda item: item.get('line_number', 0)
        )
        if not statements:
            logging.info(f"{src_sql} 未提取到 INSERT 或 CREATE TABLE ... AS 语句，跳过。")
            continue
        chunks = split_sql_chunks(statements)
        if not chunks:
            logging.warning(f"{src_sql} 的语句拆分结果为空，跳过。")
            continue

        base_name = os.path.splitext(os.path.basename(src_sql))[0]
        for idx, seg in enumerate(chunks, start=1):
            path = os.path.join(chunk_dir, f"{base_name}_{idx}.sql")
            with open(path, 'w', encoding='utf-8') as f:
                f.write(seg)
            logging.info(f"已保存：{path}")

    # 生成每段 CSV
    generate_chunk_csvs(chunk_dir,
                        db_type='hive',
                        dlineage_script='dlineage.py')

    # 将每个存储过程的 CSV 汇总到 result 目录
    export_result_csvs(chunk_dir, result_dir='result')
    
    
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
                normalized_row = _normalize_lineage_row(row)
                if not normalized_row:
                    print(f"无法解析列，跳过: {csv_file} 行内容: {row}")
                    continue
                row = normalized_row
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
