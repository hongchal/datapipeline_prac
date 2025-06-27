import os

def read_sql_file(file_name, sql_dir, **kwargs):
    sql_dir_path = os.path.join(os.path.dirname(__file__), '..', sql_dir)
    file_path = os.path.join(sql_dir_path, file_name)
    with open(file_path, 'r', encoding='utf-8') as file:
        sql_content = file.read()
    sql= sql_content.format(**kwargs)
    return sql
