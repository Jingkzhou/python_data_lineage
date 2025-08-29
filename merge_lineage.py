import json
import glob
import os


def merge_lineage_jsons(json_dir):
    """
    Merge multiple lineage JSON files into a global model.
    """
    merged = {
        "dbobjs": {"createdBy": None, "servers": []},
        "relationships": [],
        "processes": [],
        "errors": []
    }

    # helper maps for deduplication
    srv_map = {}
    rel_ids = set()
    proc_ids = set()
    err_keys = set()

    for path in glob.glob(os.path.join(json_dir, '*.json')):
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # createdBy (take first)
        if merged['dbobjs']['createdBy'] is None:
            merged['dbobjs']['createdBy'] = data.get('dbobjs', {}).get('createdBy')

        # merge servers/databases/schemas/tables/functions
        for srv in data.get('dbobjs', {}).get('servers', []):
            name = srv['name']
            if name not in srv_map:
                # deep copy server structure
                srv_map[name] = {
                    'name': name,
                    'dbVendor': srv.get('dbVendor'),
                    'supportsCatalogs': srv.get('supportsCatalogs'),
                    'supportsSchemas': srv.get('supportsSchemas'),
                    'databases': []
                }
            target_srv = srv_map[name]

            # merge databases
            for db in srv.get('databases', []):
                db_name = db['name']
                db_map = {d['name']: d for d in target_srv['databases']}
                if db_name not in db_map:
                    target_srv['databases'].append(db)
                else:
                    # merge schemas
                    existing_db = db_map[db_name]
                    schema_map = {s['name']: s for s in existing_db['schemas']}
                    for sch in db.get('schemas', []):
                        if sch['name'] not in schema_map:
                            existing_db['schemas'].append(sch)
                        else:
                            exist_sch = schema_map[sch['name']]
                            # merge tables
                            tbl_map = {t['id']: t for t in exist_sch.get('tables', [])}
                            for t in sch.get('tables', []):
                                if t['id'] not in tbl_map:
                                    exist_sch.setdefault('tables', []).append(t)
                            # merge others, processes inside dbobjs if needed
        # after iterating, rebuild merged['dbobjs']['servers'] list
    merged['dbobjs']['servers'] = list(srv_map.values())

    # merge relationships
    for path in glob.glob(os.path.join(json_dir, '*.json')):
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        for rel in data.get('relationships', []):
            if rel['id'] not in rel_ids:
                merged['relationships'].append(rel)
                rel_ids.add(rel['id'])

    # merge processes
    for path in glob.glob(os.path.join(json_dir, '*.json')):
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        for proc in data.get('processes', []):
            if proc['id'] not in proc_ids:
                merged['processes'].append(proc)
                proc_ids.add(proc['id'])

    # merge errors (by message + coords)
    for path in glob.glob(os.path.join(json_dir, '*.json')):
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        for err in data.get('errors', []):
            key = (err['errorMessage'], tuple(tuple(c.values()) for c in err.get('coordinates', [])))
            if key not in err_keys:
                merged['errors'].append(err)
                err_keys.add(key)

    return merged


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Merge lineage JSON chunks into one global model')
    parser.add_argument('json_dir', help='Directory containing chunk JSON files')
    parser.add_argument('-o', '--output', default='merged_lineage.json', help='Output file path')
    args = parser.parse_args()

    result = merge_lineage_jsons(args.json_dir)
    with open(args.output, 'w', encoding='utf-8') as out:
        json.dump(result, out, ensure_ascii=False, indent=2)
    print(f'Merged lineage written to {args.output}')
