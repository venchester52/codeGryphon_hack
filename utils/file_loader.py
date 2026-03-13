from __future__ import annotations

import io
import json
import re
import sqlite3
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd


class FileLoadError(RuntimeError):
    """Понятная ошибка загрузки источника данных для UI."""


SUPPORTED_SOURCE_TYPES = {
    ".csv": "csv",
    ".xlsx": "excel",
    ".xls": "excel",
    ".json": "json",
    ".sql": "sql",
    ".sqlite": "sqlite",
    ".sqlite3": "sqlite",
    ".db": "sqlite",
}


def detect_source_type(filename: str) -> tuple[str, str]:
    extension = Path(filename).suffix.lower()
    source_type = SUPPORTED_SOURCE_TYPES.get(extension, "unknown")
    return source_type, extension


def inspect_source(filename: str, file_bytes: bytes) -> dict[str, Any]:
    source_type, extension = detect_source_type(filename)
    if source_type == "unknown":
        raise FileLoadError(
            f"Неподдерживаемый формат файла: {extension or 'без расширения'}. "
            "Поддерживаются CSV, Excel, JSON, SQL и SQLite."
        )

    if source_type == "csv":
        return {
            "source_type": "csv",
            "source_extension": extension,
            "parse_method": "pandas.read_csv",
        }

    if source_type == "excel":
        return _inspect_excel_source(file_bytes, extension)

    if source_type == "json":
        return _inspect_json_source(file_bytes, extension)

    if source_type == "sql":
        return _inspect_sql_source(file_bytes, extension)

    if source_type == "sqlite":
        return _inspect_sqlite_source(file_bytes, extension)

    raise FileLoadError("Не удалось определить формат источника данных.")


def load_source_dataframe(
    filename: str,
    file_bytes: bytes,
    selection: dict[str, str] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    source_info = inspect_source(filename, file_bytes)
    selection = selection or {}

    source_type = source_info["source_type"]
    if source_type == "csv":
        dataframe = _load_csv(file_bytes)
        metadata = {
            "source_type": "CSV",
            "parse_method": "pandas.read_csv",
        }
        return _finalize_result(dataframe, metadata)

    if source_type == "excel":
        sheet_name = selection.get("sheet_name") or source_info.get("default_sheet")
        if not sheet_name:
            raise FileLoadError("Не удалось выбрать лист Excel для анализа.")
        dataframe = _load_excel_sheet(file_bytes, sheet_name)
        metadata = {
            "source_type": "Excel",
            "parse_method": "pandas.read_excel",
            "selected_sheet": sheet_name,
        }
        return _finalize_result(dataframe, metadata)

    if source_type == "json":
        json_path = selection.get("json_path") or source_info.get("default_json_path")
        if not json_path:
            raise FileLoadError("Не удалось определить путь к записям JSON.")
        dataframe = _load_json_path(file_bytes, json_path)
        metadata = {
            "source_type": "JSON",
            "parse_method": "pandas.json_normalize",
            "selected_json_path": json_path,
        }
        return _finalize_result(dataframe, metadata)

    if source_type == "sql":
        parse_mode = source_info.get("parse_mode")
        table_name = selection.get("table_name") or source_info.get("default_table")
        if parse_mode == "sqlite_script":
            if not table_name:
                raise FileLoadError("Не удалось выбрать таблицу из SQL-дампа.")
            dataframe = _load_sql_table_via_sqlite_script(file_bytes, table_name)
            metadata = {
                "source_type": "SQL",
                "parse_method": "sqlite3 executescript + SELECT",
                "sql_parse_mode": "SQL-дамп с таблицами",
                "selected_table": table_name,
            }
            return _finalize_result(dataframe, metadata)

        if parse_mode == "insert_parser":
            parsed_tables = _parse_insert_tables(_decode_text(file_bytes))
            if not parsed_tables:
                raise FileLoadError("Не удалось извлечь строки из SQL INSERT-операторов.")
            if not table_name:
                table_name = _best_table_name(parsed_tables)
            if table_name not in parsed_tables:
                raise FileLoadError("Выбранная таблица не найдена в SQL-файле.")
            dataframe = parsed_tables[table_name]
            metadata = {
                "source_type": "SQL",
                "parse_method": "встроенный парсер INSERT",
                "sql_parse_mode": "INSERT-операторы",
                "selected_table": table_name,
            }
            return _finalize_result(dataframe, metadata)

        raise FileLoadError(
            "SQL-файл не содержит данных для анализа. "
            "Если это только SELECT-запрос, загрузите также источник данных (дамп/SQLite)."
        )

    if source_type == "sqlite":
        table_name = selection.get("table_name") or source_info.get("default_table")
        if not table_name:
            raise FileLoadError("Не удалось выбрать таблицу в SQLite-файле.")
        dataframe = _load_sqlite_table(file_bytes, table_name)
        metadata = {
            "source_type": "SQLite",
            "parse_method": "sqlite3 + SELECT",
            "selected_table": table_name,
        }
        return _finalize_result(dataframe, metadata)

    raise FileLoadError("Формат источника не поддерживается для загрузки.")


def _finalize_result(dataframe: pd.DataFrame, metadata: dict[str, Any]) -> tuple[pd.DataFrame, dict[str, Any]]:
    cleaned_df = dataframe.copy()
    cleaned_df = cleaned_df.dropna(how="all")

    if cleaned_df.empty:
        raise FileLoadError("После парсинга получен пустой набор данных.")

    if cleaned_df.shape[1] == 0:
        raise FileLoadError("После парсинга не найдены колонки для анализа.")

    metadata["rows"] = int(cleaned_df.shape[0])
    metadata["columns"] = int(cleaned_df.shape[1])
    return cleaned_df, metadata


def _load_csv(file_bytes: bytes) -> pd.DataFrame:
    try:
        return pd.read_csv(io.BytesIO(file_bytes))
    except Exception as error:
        raise FileLoadError(f"Не удалось прочитать CSV-файл: {error}") from error


def _inspect_excel_source(file_bytes: bytes, extension: str) -> dict[str, Any]:
    try:
        excel_file = pd.ExcelFile(io.BytesIO(file_bytes))
    except Exception as error:
        raise FileLoadError(f"Не удалось прочитать Excel-файл: {error}") from error

    if not excel_file.sheet_names:
        raise FileLoadError("В Excel-файле не найдено листов.")

    sheet_stats: list[dict[str, Any]] = []
    for sheet_name in excel_file.sheet_names:
        try:
            frame = excel_file.parse(sheet_name=sheet_name)
            non_empty_rows = int(frame.dropna(how="all").shape[0])
            columns_count = int(frame.shape[1])
            score = non_empty_rows * max(columns_count, 1)
            sheet_stats.append(
                {
                    "sheet_name": sheet_name,
                    "rows": non_empty_rows,
                    "columns": columns_count,
                    "score": score,
                }
            )
        except Exception:
            sheet_stats.append(
                {
                    "sheet_name": sheet_name,
                    "rows": 0,
                    "columns": 0,
                    "score": 0,
                }
            )

    non_empty_sheets = [item for item in sheet_stats if item["rows"] > 0 and item["columns"] > 0]
    if not non_empty_sheets:
        raise FileLoadError("В Excel-файле нет непустых листов с таблицей.")

    sorted_sheets = sorted(non_empty_sheets, key=lambda item: item["score"], reverse=True)
    best_sheet = sorted_sheets[0]["sheet_name"]
    requires_selection = len(sorted_sheets) > 1 and sorted_sheets[0]["score"] == sorted_sheets[1]["score"]

    return {
        "source_type": "excel",
        "source_extension": extension,
        "parse_method": "pandas.read_excel",
        "sheet_options": [item["sheet_name"] for item in non_empty_sheets],
        "default_sheet": best_sheet,
        "requires_sheet_selection": requires_selection,
        "sheet_stats": non_empty_sheets,
    }


def _load_excel_sheet(file_bytes: bytes, sheet_name: str) -> pd.DataFrame:
    try:
        dataframe = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name)
    except Exception as error:
        raise FileLoadError(f"Не удалось прочитать лист Excel «{sheet_name}»: {error}") from error

    if dataframe.dropna(how="all").empty:
        raise FileLoadError(f"Выбранный лист Excel «{sheet_name}» не содержит данных.")

    return dataframe


def _inspect_json_source(file_bytes: bytes, extension: str) -> dict[str, Any]:
    payload = _parse_json_payload(file_bytes)
    candidates = _collect_json_candidates(payload)

    if not candidates:
        raise FileLoadError(
            "JSON не удалось привести к табличному виду. "
            "Нужен массив записей (объектов) или вложенный путь к такому массиву."
        )

    sorted_candidates = sorted(candidates, key=lambda item: item["score"], reverse=True)
    best_path = sorted_candidates[0]["path"]

    return {
        "source_type": "json",
        "source_extension": extension,
        "parse_method": "pandas.json_normalize",
        "json_path_options": [item["path"] for item in sorted_candidates],
        "default_json_path": best_path,
        "requires_json_path_selection": len(sorted_candidates) > 1,
        "json_candidates": sorted_candidates,
    }


def _load_json_path(file_bytes: bytes, json_path: str) -> pd.DataFrame:
    payload = _parse_json_payload(file_bytes)
    records = _extract_json_by_path(payload, json_path)

    if not isinstance(records, list):
        raise FileLoadError("Выбранный JSON-путь не содержит список записей.")

    if not records:
        raise FileLoadError("Выбранный JSON-путь содержит пустой список.")

    if all(isinstance(item, dict) for item in records):
        dataframe = pd.json_normalize(records)
    else:
        dataframe = pd.DataFrame({"value": records})

    return dataframe


def _parse_json_payload(file_bytes: bytes) -> Any:
    try:
        return json.loads(_decode_text(file_bytes))
    except Exception as error:
        raise FileLoadError(f"Некорректный JSON-файл: {error}") from error


def _collect_json_candidates(payload: Any) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []

    if isinstance(payload, list):
        candidates.append(_json_candidate("$", payload))

    def walk(node: Any, path: str) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                child_path = f"{path}.{key}" if path else key
                if isinstance(value, list):
                    candidates.append(_json_candidate(child_path, value))
                walk(value, child_path)
        elif isinstance(node, list):
            for index, value in enumerate(node[:20]):
                walk(value, f"{path}[{index}]")

    walk(payload, "$")

    valid_candidates = [
        item
        for item in candidates
        if item["length"] > 0 and (item["dict_ratio"] > 0 or item["primitive_ratio"] > 0)
    ]

    unique_by_path: dict[str, dict[str, Any]] = {}
    for item in valid_candidates:
        if item["path"] not in unique_by_path or item["score"] > unique_by_path[item["path"]]["score"]:
            unique_by_path[item["path"]] = item

    return list(unique_by_path.values())


def _json_candidate(path: str, values: list[Any]) -> dict[str, Any]:
    length = len(values)
    if length == 0:
        return {"path": path, "length": 0, "dict_ratio": 0.0, "primitive_ratio": 0.0, "score": 0.0}

    dict_count = sum(1 for item in values if isinstance(item, dict))
    primitive_count = sum(1 for item in values if not isinstance(item, (dict, list)))

    dict_ratio = dict_count / length
    primitive_ratio = primitive_count / length
    score = length * (dict_ratio * 1.0 + primitive_ratio * 0.3)

    return {
        "path": path,
        "length": length,
        "dict_ratio": float(dict_ratio),
        "primitive_ratio": float(primitive_ratio),
        "score": float(score),
    }


def _extract_json_by_path(payload: Any, path: str) -> Any:
    if path == "$":
        return payload

    current = payload
    tokens = path.split(".")
    for token in tokens[1:]:
        if "[" in token and token.endswith("]"):
            name, index_raw = token[:-1].split("[", maxsplit=1)
            if name:
                if not isinstance(current, dict) or name not in current:
                    raise FileLoadError(f"JSON-путь не найден: {path}")
                current = current[name]
            if not isinstance(current, list):
                raise FileLoadError(f"JSON-путь не найден: {path}")
            index = int(index_raw)
            if index >= len(current):
                raise FileLoadError(f"JSON-путь не найден: {path}")
            current = current[index]
            continue

        if not isinstance(current, dict) or token not in current:
            raise FileLoadError(f"JSON-путь не найден: {path}")
        current = current[token]

    return current


def _inspect_sql_source(file_bytes: bytes, extension: str) -> dict[str, Any]:
    sql_text = _decode_text(file_bytes)

    sqlite_tables = _inspect_sql_script_tables(sql_text)
    if sqlite_tables:
        default_table = _best_table_name(sqlite_tables)
        return {
            "source_type": "sql",
            "source_extension": extension,
            "parse_mode": "sqlite_script",
            "table_options": sorted(sqlite_tables.keys()),
            "default_table": default_table,
            "requires_table_selection": len(sqlite_tables) > 1,
            "table_stats": {name: int(df.shape[0]) for name, df in sqlite_tables.items()},
        }

    insert_tables = _parse_insert_tables(sql_text)
    if insert_tables:
        default_table = _best_table_name(insert_tables)
        return {
            "source_type": "sql",
            "source_extension": extension,
            "parse_mode": "insert_parser",
            "table_options": sorted(insert_tables.keys()),
            "default_table": default_table,
            "requires_table_selection": len(insert_tables) > 1,
            "table_stats": {name: int(df.shape[0]) for name, df in insert_tables.items()},
        }

    if _looks_like_select_only(sql_text):
        return {
            "source_type": "sql",
            "source_extension": extension,
            "parse_mode": "query_only",
            "table_options": [],
            "default_table": None,
            "requires_table_selection": False,
        }

    return {
        "source_type": "sql",
        "source_extension": extension,
        "parse_mode": "unsupported",
        "table_options": [],
        "default_table": None,
        "requires_table_selection": False,
    }


def _load_sql_table_via_sqlite_script(file_bytes: bytes, table_name: str) -> pd.DataFrame:
    sql_text = _decode_text(file_bytes)
    try:
        with sqlite3.connect(":memory:") as connection:
            connection.executescript(sql_text)
            query = f'SELECT * FROM "{table_name}"'
            dataframe = pd.read_sql_query(query, connection)
    except Exception as error:
        raise FileLoadError(f"Не удалось извлечь данные из SQL-дампа: {error}") from error

    if dataframe.empty:
        raise FileLoadError(f"Таблица «{table_name}» не содержит строк для анализа.")

    return dataframe


def _inspect_sqlite_source(file_bytes: bytes, extension: str) -> dict[str, Any]:
    tables = _inspect_sqlite_tables(file_bytes)
    if not tables:
        raise FileLoadError("В SQLite-файле не найдено таблиц с данными.")

    default_table = _best_table_name(tables)

    return {
        "source_type": "sqlite",
        "source_extension": extension,
        "parse_mode": "sqlite_file",
        "table_options": sorted(tables.keys()),
        "default_table": default_table,
        "requires_table_selection": len(tables) > 1,
        "table_stats": {name: int(df.shape[0]) for name, df in tables.items()},
    }


def _load_sqlite_table(file_bytes: bytes, table_name: str) -> pd.DataFrame:
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=True) as temp_file:
        temp_file.write(file_bytes)
        temp_file.flush()

        try:
            with sqlite3.connect(temp_file.name) as connection:
                query = f'SELECT * FROM "{table_name}"'
                dataframe = pd.read_sql_query(query, connection)
        except Exception as error:
            raise FileLoadError(f"Не удалось прочитать SQLite-файл: {error}") from error

    if dataframe.empty:
        raise FileLoadError(f"Таблица «{table_name}» в SQLite-файле пустая.")

    return dataframe


def _inspect_sql_script_tables(sql_text: str) -> dict[str, pd.DataFrame]:
    try:
        with sqlite3.connect(":memory:") as connection:
            connection.executescript(sql_text)
            table_names = _fetch_sqlite_table_names(connection)
            if not table_names:
                return {}

            tables: dict[str, pd.DataFrame] = {}
            for table_name in table_names:
                dataframe = pd.read_sql_query(f'SELECT * FROM "{table_name}"', connection)
                if not dataframe.empty and dataframe.shape[1] > 0:
                    tables[table_name] = dataframe

            return tables
    except Exception:
        return {}


def _inspect_sqlite_tables(file_bytes: bytes) -> dict[str, pd.DataFrame]:
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=True) as temp_file:
        temp_file.write(file_bytes)
        temp_file.flush()

        try:
            with sqlite3.connect(temp_file.name) as connection:
                table_names = _fetch_sqlite_table_names(connection)
                tables: dict[str, pd.DataFrame] = {}
                for table_name in table_names:
                    dataframe = pd.read_sql_query(f'SELECT * FROM "{table_name}"', connection)
                    if not dataframe.empty and dataframe.shape[1] > 0:
                        tables[table_name] = dataframe
                return tables
        except Exception as error:
            raise FileLoadError(f"Некорректный SQLite-файл: {error}") from error


def _fetch_sqlite_table_names(connection: sqlite3.Connection) -> list[str]:
    query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    rows = connection.execute(query).fetchall()
    return [str(item[0]) for item in rows if item and item[0]]


def _parse_insert_tables(sql_text: str) -> dict[str, pd.DataFrame]:
    pattern = re.compile(
        r"INSERT\s+INTO\s+[`\"]?(?P<table>[A-Za-z0-9_]+)[`\"]?\s*"
        r"(?:\((?P<columns>[^)]*)\))?\s*VALUES\s*(?P<values>.*?);",
        flags=re.IGNORECASE | re.DOTALL,
    )

    table_rows: dict[str, list[list[Any]]] = {}
    table_columns: dict[str, list[str]] = {}

    for match in pattern.finditer(sql_text):
        table_name = match.group("table")
        raw_columns = match.group("columns")
        raw_values = match.group("values")

        columns = _split_sql_columns(raw_columns) if raw_columns else []
        rows = _parse_values_block(raw_values)
        if not rows:
            continue

        table_rows.setdefault(table_name, [])
        table_rows[table_name].extend(rows)

        if columns:
            table_columns[table_name] = columns

    result: dict[str, pd.DataFrame] = {}
    for table_name, rows in table_rows.items():
        if not rows:
            continue

        columns = table_columns.get(table_name, [])
        max_columns = max(len(row) for row in rows)

        if columns and len(columns) == max_columns:
            dataframe = pd.DataFrame(rows, columns=columns)
        else:
            dataframe = pd.DataFrame(rows)

        if not dataframe.empty and dataframe.shape[1] > 0:
            result[table_name] = dataframe

    return result


def _split_sql_columns(raw_columns: str) -> list[str]:
    values = _split_top_level(raw_columns, separator=",")
    cleaned: list[str] = []
    for value in values:
        item = value.strip().strip("`").strip('"').strip("[").strip("]")
        if item:
            cleaned.append(item)
    return cleaned


def _parse_values_block(raw_values: str) -> list[list[Any]]:
    tuples = _extract_value_tuples(raw_values)
    rows: list[list[Any]] = []

    for tuple_raw in tuples:
        raw_items = _split_top_level(tuple_raw, separator=",")
        row = [_parse_sql_literal(item.strip()) for item in raw_items]
        rows.append(row)

    return rows


def _extract_value_tuples(raw_values: str) -> list[str]:
    tuples: list[str] = []
    depth = 0
    in_single = False
    in_double = False
    buffer: list[str] = []

    index = 0
    while index < len(raw_values):
        char = raw_values[index]

        if char == "'" and not in_double:
            next_char = raw_values[index + 1] if index + 1 < len(raw_values) else ""
            if in_single and next_char == "'":
                buffer.append(char)
                buffer.append(next_char)
                index += 2
                continue
            in_single = not in_single

        elif char == '"' and not in_single:
            in_double = not in_double

        if not in_single and not in_double:
            if char == "(":
                if depth == 0:
                    buffer = []
                else:
                    buffer.append(char)
                depth += 1
                index += 1
                continue

            if char == ")":
                depth -= 1
                if depth == 0:
                    tuples.append("".join(buffer).strip())
                    buffer = []
                else:
                    buffer.append(char)
                index += 1
                continue

        if depth > 0:
            buffer.append(char)

        index += 1

    return [item for item in tuples if item]


def _split_top_level(raw_text: str, separator: str = ",") -> list[str]:
    parts: list[str] = []
    buffer: list[str] = []
    depth = 0
    in_single = False
    in_double = False

    index = 0
    while index < len(raw_text):
        char = raw_text[index]

        if char == "'" and not in_double:
            next_char = raw_text[index + 1] if index + 1 < len(raw_text) else ""
            if in_single and next_char == "'":
                buffer.append(char)
                buffer.append(next_char)
                index += 2
                continue
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double

        if not in_single and not in_double:
            if char == "(":
                depth += 1
            elif char == ")" and depth > 0:
                depth -= 1
            elif char == separator and depth == 0:
                parts.append("".join(buffer).strip())
                buffer = []
                index += 1
                continue

        buffer.append(char)
        index += 1

    if buffer:
        parts.append("".join(buffer).strip())

    return parts


def _parse_sql_literal(value: str) -> Any:
    upper_value = value.upper()
    if upper_value == "NULL":
        return None
    if upper_value in {"TRUE", "FALSE"}:
        return upper_value == "TRUE"

    if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
        unquoted = value[1:-1]
        return unquoted.replace("''", "'").replace('""', '"')

    integer_pattern = re.compile(r"^-?\d+$")
    float_pattern = re.compile(r"^-?\d+\.\d+$")

    if integer_pattern.match(value):
        try:
            return int(value)
        except Exception:
            return value

    if float_pattern.match(value):
        try:
            return float(value)
        except Exception:
            return value

    return value


def _looks_like_select_only(sql_text: str) -> bool:
    compact = re.sub(r"\s+", " ", sql_text.strip().lower())
    has_select = "select " in compact
    has_insert = "insert into" in compact
    has_create = "create table" in compact
    return has_select and not has_insert and not has_create


def _best_table_name(tables: dict[str, pd.DataFrame]) -> str | None:
    if not tables:
        return None
    sorted_items = sorted(tables.items(), key=lambda item: item[1].shape[0], reverse=True)
    return sorted_items[0][0]


def _decode_text(file_bytes: bytes) -> str:
    for encoding in ["utf-8-sig", "utf-8", "cp1251", "latin-1"]:
        try:
            return file_bytes.decode(encoding)
        except Exception:
            continue
    raise FileLoadError("Не удалось декодировать текст файла.")
