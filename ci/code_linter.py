import os
import re
import sys
from typing import List, Tuple

# --- Configuración de Directorios ---
DIRECTORIES_TO_SCAN = ["silver/", "gold/"]

# --- Reglas de Calidad para SQL ---
# La regex para SELECT * ya funciona para multilínea gracias a `\s+`, 
# el problema estaba en la lógica de lectura, no en la regex.
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

# --- FUNCIÓN CORREGIDA ---
def validate_sql_block(sql_code: str, start_line: int) -> List[Tuple[int, str, str]]:
    """
    Aplica todas las reglas SQL a un bloque de texto, manejando patrones multilínea.
    """
    violations = []
    # Guardamos las líneas originales para el reporte
    original_lines = sql_code.split('\n')
    # Limpiamos los comentarios del bloque completo para el análisis
    code_no_comments = remove_sql_comments(sql_code)

    reported_lines = set()

    for rule_name, regex in SQL_RULES.items():
        # Usamos finditer para encontrar TODAS las coincidencias en el bloque completo
        for match in regex.finditer(code_no_comments):
            # Calculamos el número de línea donde empieza la coincidencia
            line_num = code_no_comments[:match.start()].count('\n') + start_line
            
            # Para evitar reportar la misma línea dos veces por reglas diferentes
            # que coincidan en la misma línea.
            if (line_num, rule_name) in reported_lines:
                continue

            # Obtenemos el contenido original de la línea para un reporte claro
            # El índice es line_num - start_line porque original_lines es 0-indexed
            # y relativo al bloque que estamos analizando.
            original_line_index = line_num - start_line
            if original_line_index < len(original_lines):
                line_content = original_lines[original_line_index].strip()
            else:
                line_content = "" # Fallback por si algo sale mal

            violations.append((
                line_num,
                f"[SQL] {rule_name}",
                line_content
            ))
            reported_lines.add((line_num, rule_name))
            
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
        # Para archivos .py, la lógica actual de separar bloques es adecuada
        # porque las reglas de Python son por línea y los bloques SQL son celdas completas.
        
        # Validación de Python (excluyendo celdas SQL)
        python_only_code = re.sub(r"^%sql.*?(?=(^%[a-zA-Z])|$)", "", content, flags=re.MULTILINE | re.DOTALL)
        all_violations.extend(validate_python_block(python_only_code, 1))

        # Validación de celdas SQL
        sql_blocks = re.finditer(r"^(%sql.*?)(?=(^%[a-zA-Z])|$)", content, re.MULTILINE | re.DOTALL)
        for match in sql_blocks:
            block_content = match.group(1)
            start_line = content[:match.start()].count('\n') + 1
            all_violations.extend(validate_sql_block(block_content, start_line))

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