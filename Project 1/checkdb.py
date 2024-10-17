import psycopg2
from psycopg2 import OperationalError
import sys
import os

dbParams = {
    "host": "127.0.0.1",
    "dbname": "cosc3380",
    "user": "dbs01",
    "port": "5432",
    "password": "XXXX"
}

def checkFileExists(filePath):
    if os.path.exists(filePath):
        print(f"File exists: {filePath}")
    else:
        print(f"File does not exist: {filePath}")
        exit(1)

def getFilenameWithoutExtension(filepath):
    baseName = os.path.basename(filepath)
    return os.path.splitext(baseName)[0]

def connectDb():
    try:
        print("Connecting to database...")
        conn = psycopg2.connect(**dbParams)
        cursor = conn.cursor()
        print("Connected successfully!")
        return conn, cursor
    except psycopg2.OperationalError as e:
        print(f"Error connecting to the database: {e}")
        return None, None

def runSql(cursor, sqlQuery=""):
    print(f"Running Query: {sqlQuery}")
    cursor.execute(sqlQuery)
    records = cursor.fetchall()
    for record in records:
        print(record)

def set_search_path(cursor, sql_query=""):
    print(f"Query : {sql_query}")
    cursor.execute(sql_query)
    # Verify the search_path
    cursor.execute("SHOW search_path;")
    records = cursor.fetchall()
    # for record in records :
    #    print ( record )
        
def readInput(fileName):
    import re
    tables = {}
    tableRe = re.compile(r"(\w+)\((.+)\)")
    columnRe = re.compile(r"(\w+)\s*(\((pk|fk:[^()]+)\))?")
    
    with open(fileName, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            match = tableRe.match(line)
            if not match:
                continue

            tableName = match.group(1)
            columns = match.group(2)
            pk = None
            fkList = []

            for colMatch in columnRe.finditer(columns):
                colName = colMatch.group(1)
                annot = colMatch.group(3)

                if annot == 'pk':
                    pk = colName
                elif annot and annot.startswith('fk:'):
                    fkRef = annot.split('fk:')[1]
                    fkList.append((colName, fkRef))

            tables[tableName] = {'pk': pk, 'fks': fkList}
    return tables

def checkFk(conn, tables, test_case_file_name):
    results = []
    for table, info in tables.items():
        new_table_name = f"{test_case_file_name}_{table}"
        if info['fks']:
            for fkCol, fkRef in info['fks']:
                refTable, refCol = fkRef.split('.')
                new_ref_table = f"{test_case_file_name}_{refTable}"
                query = f"""
                SELECT COUNT(*) FROM {new_table_name}
                LEFT JOIN {new_ref_table} ON {new_table_name}.{fkCol} = {new_ref_table}.{refCol}
                WHERE {new_table_name}.{fkCol} IS NOT NULL AND {new_ref_table}.{refCol} IS NULL;
                """
                runSql(conn.cursor(), query)
                cur = conn.cursor()
                cur.execute(query)
                count = cur.fetchone()[0]
                results.append((table, "Y" if count == 0 else "N"))
        else:
            results.append((table, "Y"))
    return results

def checkNormalization(conn, tables, test_case_file_name):
    results = []
    for table, info in tables.items():
        pk = info['pk']
        fks = [fkCol for fkCol, _ in info['fks']]
        new_table_name = f"{test_case_file_name}_{table}"

        cur = conn.cursor()
        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{new_table_name.lower()}';")
        allCols = [row[0] for row in cur.fetchall()]

        nonPkCols = [col for col in allCols if col != pk]

        if not nonPkCols:
            results.append((table, "Y"))
            continue

        isNormal = True

        for col in nonPkCols:
            for fkCol, fkRef in info['fks']:
                refTable, refCol = fkRef.split('.')
                new_ref_table = f"{test_case_file_name}_{refTable}"
                if col == fkCol:
                    continue

                cur.execute(f"""
                SELECT COUNT(DISTINCT ({fkCol}, {col})) FROM {new_table_name}
                WHERE {fkCol} IS NOT NULL AND {col} IS NOT NULL;
                """)
                fkDepCount = cur.fetchone()[0]

                cur.execute(f"""
                SELECT COUNT(DISTINCT ({pk}, {col})) FROM {new_table_name}
                WHERE {pk} IS NOT NULL AND {col} IS NOT NULL;
                """)
                pkDepCount = cur.fetchone()[0]

                if fkDepCount < pkDepCount:
                    isNormal = False
                    break

            if not isNormal:
                break

        results.append((table, "Y" if isNormal else "N"))

    return results

def output(results, fileName):
    with open(fileName, 'w') as f:
        f.write("referential integrity  normalized\n")
        f.write("----------------------------------\n")

        refOk = True
        normOk = True

        for table, refint, norm in results:
            f.write(f"{table:<10} {refint:<5} {norm:<5}\n")
            
            if refint == "N":
                refOk = False
            if norm == "N":
                normOk = False

        f.write("----------------------------------\n")
        f.write(f"DB referential integrity: {'Y' if refOk else 'N'}\n")
        f.write(f"DB normalized: {'Y' if normOk else 'N'}\n")

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 script_name.py database=filename.txt")
        sys.exit(1)

    databaseFile = None
    for arg in sys.argv[1:]:
        if arg.startswith("database="):
            databaseFile = arg.split("=")[1]

    if not databaseFile:
        print("No database file provided.")
        sys.exit(1)

    checkFileExists(databaseFile)

    test_case_file_name = getFilenameWithoutExtension(databaseFile)
    tables = readInput(databaseFile)
    conn, cursor = connectDb()

    if conn:
        # 1. set search path
        query = f"SET search_path TO HW1, examples, public, dbs01"
        set_search_path(cursor, query)
        
        refintResults = checkFk(conn, tables, test_case_file_name)
        normResults = checkNormalization(conn, tables, test_case_file_name)
        finalResults = [(table, refint, norm) for (table, refint), (_, norm) in zip(refintResults, normResults)]
        output(finalResults, f"{test_case_file_name}_output.txt")
        cursor.close()
        conn.close()
        print("Database connection closed.")

if __name__ == "__main__":
    main()