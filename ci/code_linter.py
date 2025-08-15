import os
import re
import sys
from typing import List, Tuple

# --- Configuración de Directorios ---
DIRECTORIES_TO_SCAN = ["silver/", "gold/"]

# --- Reglas de Calidad para SQL ---
SQL_RULES = {
    "Uso de 'SELECT *' prohibido": 
        re.compile(r"\bselect\s+\*", re.IGNORECASE),
    "Referencia a esquemas con prefijo 'prod.' o 'dev.' prohibida": 
        re.compile(r"\b(prod|dev)\.", re.IGNORECASE),
}

# --- Reglas de Calidad para Python (Databricks/PySpark) ---
PYTHON_RULES = {
    "Uso de '.collect()' detectado. Puede causar OOM en el driver. Usar '.take()' o '.show()' para inspección.":
        re.compile(r"\.collect\s*\("),
    # --- LÍNEA CORREGIDA ---
    # La regex anterior con look-behind fallaba. Esta versión más simple detecta cualquier uso de .toPandas()
    # y el mensaje le pide al desarrollador que asegure su uso correcto con .limit().
    "Uso de '.toPandas()' detectado. Asegúrese de que se use con '.limit()' para evitar errores OOM.":
        re.compile(r"\.toPandas\s*\("),
    "Se detectó una UDF de Python. Las funciones nativas de Spark son preferibles por rendimiento.":
        re.compile(r"=\s*udf\("),
    "Credenciales hardcodeadas detectadas. Usar dbutils.secrets.get().":
        re.compile(r"(access_key|secret_key|password|token)['\"]?\s*[:=]\s*['\"][\w\-/+]{16,}['\"]"),
}

# --- Lógica del Linter ---

def remove_sql_comments(sql_code: str) -> str:
    """Elimina comentarios de un bloque de código SQL."""
    code = re.sub(r"/\*.*?\*/", "", sql_code, flags=re.DOTALL)
    code = re.sub(r"--.*", "", code)
    return code

def validate_sql_block(sql_code: str, start_line: int) -> List[Tuple[int, str, str]]:
    """Aplica todas las reglas SQL a un bloque de texto."""
    violations = []
    code_no_comments = remove_sql_comments(sql_code)
    lines_no_comments = code_no_comments.split('\n')
    original_lines = sql_code.split('\n')

    for i, line in enumerate(lines_no_comments):
        for rule_name, regex in SQL_RULES.items():
            if regex.search(line):
                violations.append((
                    start_line + i,
                    f"[SQL] {rule_name}",
                    original_lines[i].strip()
                ))
    return violations

def validate_python_block(py_code: str, start_line: int) -> List[Tuple[int, str, str]]:
    """Aplica todas las reglas de Python a un bloque de texto."""
    violations = []
    lines = py_code.split('\n')
    for i, line in enumerate(lines):
        if line.strip().startswith("#"):
            continue
        for rule_name, regex in PYTHON_RULES.items():
            if regex.search(line):
                violations.append((
                    start_line + i,
                    f"[Python] {rule_name}",
                    line.strip()
                ))
    return violations

def analyze_file(file_path: str) -> List[Tuple[int, str, str]]:
    """Analiza un archivo, diferenciando entre .sql y .py."""
    print(f"\nAnalizando archivo: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"Error: Archivo no encontrado en la ruta '{file_path}'")
        return []

    content = "".join(lines)
    all_violations = []

    if file_path.endswith(".sql"):
        all_violations.extend(validate_sql_block(content, 1))

    elif file_path.endswith(".py"):
        in_sql_block = False
        current_block = ""
        block_start_line = 1
        python_block_content = ""
        
        # Primero, extraemos y validamos todo el código Python de una vez
        # para evitar problemas con bloques segmentados.
        # Eliminamos las celdas %sql para aislar el Python.
        python_only_code = re.sub(r"%sql\n(.*?)(?=\n%|$)", "", content, flags=re.DOTALL)
        all_violations.extend(validate_python_block(python_only_code, 1))

        # Ahora, buscamos y validamos los bloques SQL
        # Regex para encontrar bloques que empiezan con %sql
        sql_blocks = re.finditer(r"^(%sql.*?)(?=(^%[a-zA-Z])|($))", content, re.MULTILINE | re.DOTALL)
        for match in sql_blocks:
            block_content = match.group(1)
            # Calcular línea de inicio del bloque
            start_line = content[:match.start()].count('\n') + 1
            all_violations.extend(validate_sql_block(block_content, start_line))

    # Ordenar violaciones por número de línea para un reporte más claro
    all_violations.sort(key=lambda x: x[0])
    return all_violations

def main():
    """Función principal que recorre directorios y ejecuta las validaciones."""
    print("--- Iniciando Linter de Calidad de Código Unificado ---")
    total_violations = 0
    
    files_to_scan = []
    for directory in DIRECTORIES_TO_SCAN:
        if not os.path.isdir(directory):
            print(f"Advertencia: El directorio '{directory}' no existe y será ignorado.")
            continue
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith((".sql", ".py")):
                    files_to_scan.append(os.path.join(root, file))

    if not files_to_scan:
        print("No se encontraron archivos .py o .sql en los directorios especificados.")

    for file_path in files_to_scan:
        violations = analyze_file(file_path)
        if violations:
            total_violations += len(violations)
            print(f"  [!] Se encontraron {len(violations)} violaciones:")
            # Imprimir violaciones agrupadas por archivo
            for line_num, rule, line_content in violations:
                print(f"    - Línea {line_num}: {rule}")
                print(f"      Fragmento: \"{line_content}\"")
        else:
            print("  [✓] Sin violaciones encontradas.")

    if total_violations > 0:
        print(f"\n--- Resumen: Se encontraron {total_violations} violaciones en total. La ejecución falló. ---")
        sys.exit(1)
    else:
        print("\n--- Resumen: Todos los archivos cumplen con las reglas de calidad. ¡Buen trabajo! ---")
        sys.exit(0)

if __name__ == "__main__":
    main()