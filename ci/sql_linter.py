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
    "Uso de '.toPandas()' sin '.limit()' es riesgoso. Considerar limitar los datos antes de la conversión.":
        re.compile(r"(?<!limit\s*\(\s*\d+\s*\)\s*)\.toPandas\s*\("),
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
    # Primero, limpiar comentarios del bloque completo
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
        # Ignorar líneas que son solo comentarios de Python
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
    
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    content = "".join(lines)
    all_violations = []

    if file_path.endswith(".sql"):
        all_violations.extend(validate_sql_block(content, 1))

    elif file_path.endswith(".py"):
        # Lógica para notebooks de Databricks (%sql, %python)
        in_sql_block = False
        current_block = ""
        block_start_line = 0

        for i, line in enumerate(lines):
            line_num = i + 1
            # Detecta el inicio de una celda SQL y procesa el bloque anterior
            if line.strip() == "%sql":
                if not in_sql_block and current_block: # Procesar bloque Python anterior
                    all_violations.extend(validate_python_block(current_block, block_start_line))
                in_sql_block = True
                current_block = ""
                block_start_line = line_num + 1
            # Detecta el fin de una celda SQL y procesa el bloque
            elif line.strip().startswith("%") and in_sql_block:
                all_violations.extend(validate_sql_block(current_block, block_start_line))
                in_sql_block = False
                current_block = line # El bloque actual es ahora de Python
                block_start_line = line_num
            else:
                current_block += line
        
        # Procesar el último bloque que quedó en el buffer
        if current_block:
            if in_sql_block:
                all_violations.extend(validate_sql_block(current_block, block_start_line))
            else:
                all_violations.extend(validate_python_block(current_block, block_start_line))

    return all_violations

def main():
    """Función principal que recorre directorios y ejecuta las validaciones."""
    print("--- Iniciando Linter de Calidad de Código Unificado ---")
    total_violations = 0
    
    files_to_scan = []
    for directory in DIRECTORIES_TO_SCAN:
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith((".sql", ".py")):
                    files_to_scan.append(os.path.join(root, file))

    for file_path in files_to_scan:
        violations = analyze_file(file_path)
        if violations:
            total_violations += len(violations)
            print(f"  [!] Se encontraron {len(violations)} violaciones:")
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