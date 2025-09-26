import os
import pandas as pd
from sqlalchemy import create_engine
import boto3
import json

# ================================
# 🔧 CONFIGURACIÓN
# ================================

# Conexión MySQL
DB_HOST = "172.31.27.82"
DB_PORT = 3307
DB_USER = "root"
DB_PASS = "utec"
DB_NAME = "qa_db"

# Bucket S3
BUCKET_NAME = "alessandro-ingesta"

# Diccionario de tablas y carpetas
TABLES = {
    "quest_and_answer": "questions_folder/",
    "user_apify_call_historial": "historial_folder/",
    "user_apify_filters": "filters_folder/",
    "scraped_account": "scraped_folder/"
}

# Schemas corregidos
SCHEMAS = {
    "quest_and_answer": [
        {"Name": "id", "Type": "int"},
        {"Name": "user_id", "Type": "int"},
        {"Name": "admin_id", "Type": "int"},
        {"Name": "status", "Type": "string"},
        {"Name": "questionDescription", "Type": "string"},
        {"Name": "questionDate", "Type": "date"},
        {"Name": "questionHour", "Type": "string"},
        {"Name": "answerDescription", "Type": "string"},
        {"Name": "answerDate", "Type": "date"},
        {"Name": "answerHour", "Type": "string"},
        {"Name": "createdAt", "Type": "timestamp"},
    ],
    "scraped_account": [
        {"Name": "id", "Type": "int"},
        {"Name": "accountName", "Type": "string"},
        {"Name": "userId", "Type": "int"},
        {"Name": "scrapedAt", "Type": "timestamp"},
    ],
    "user_apify_call_historial": [
        {"Name": "id", "Type": "int"},
        {"Name": "user_id", "Type": "int"},
        {"Name": "startDate", "Type": "timestamp"},
        {"Name": "endDate", "Type": "timestamp"},
        {"Name": "executionTime", "Type": "int"},
    ],
    "user_apify_filters": [
        {"Name": "id", "Type": "int"},
        {"Name": "filterName", "Type": "string"},
        {"Name": "filterConfig", "Type": "string"},
        {"Name": "status", "Type": "string"},
        {"Name": "createdAt", "Type": "timestamp"},
        {"Name": "historial_id", "Type": "int"},
    ]
}

# ================================
# 🚀 LÓGICA
# ================================

# Conexión MySQL
engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Cliente S3
s3 = boto3.client("s3")


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia strings y asegura compatibilidad con Athena"""
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(r"[\r\n\t]", " ", regex=True)   # quitar saltos de línea
            .str.replace(r"[^\x20-\x7E]", "", regex=True)  # quitar no imprimibles
        )
    return df


def cast_types(df: pd.DataFrame, schema: list) -> pd.DataFrame:
    """Convierte columnas a los tipos definidos en schema"""
    for col in schema:
        name, typ = col["Name"], col["Type"]
        if name not in df.columns:
            continue
        if typ == "int":
            df[name] = pd.to_numeric(df[name], errors="coerce").astype("Int64")
        elif typ == "date":
            df[name] = pd.to_datetime(df[name], errors="coerce").dt.strftime("%Y-%m-%d")
        elif typ == "timestamp":
            df[name] = pd.to_datetime(df[name], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        else:  # string
            df[name] = df[name].astype(str)
    return df


def export_to_ndjson(df: pd.DataFrame, filename: str):
    """Exporta a NDJSON (una fila = un JSON en una sola línea)"""
    with open(filename, "w", encoding="utf-8") as f:
        for _, row in df.iterrows():
            f.write(json.dumps(row.to_dict(), ensure_ascii=False) + "\n")


def main():
    # 1. Limpiar bucket
    print("🔄 Limpiando bucket...")
    try:
        objects = s3.list_objects_v2(Bucket=BUCKET_NAME)
        if "Contents" in objects:
            for obj in objects["Contents"]:
                if obj["Key"].endswith(".json"):
                    s3.delete_object(Bucket=BUCKET_NAME, Key=obj["Key"])
            print("✅ Archivos previos eliminados.")
    except Exception as e:
        print(f"⚠️ Error al limpiar bucket: {e}")

    # 2. Exportar tablas
    for table, folder in TABLES.items():
        try:
            print(f"📥 Exportando tabla: {table}")
            df = pd.read_sql(f"SELECT * FROM {table}", engine)

            # limpiar y castear tipos
            df = clean_dataframe(df)
            df = cast_types(df, SCHEMAS[table])

            # exportar NDJSON
            filename = f"{table}.json"
            export_to_ndjson(df, filename)

            # subir a S3
            s3_key = f"{folder}{filename}"
            print(f"⬆️ Subiendo {filename} a s3://{BUCKET_NAME}/{s3_key}")
            s3.upload_file(filename, BUCKET_NAME, s3_key)
            print(f"✅ {filename} subido en {folder}")

            os.remove(filename)
        except Exception as e:
            print(f"⚠️ Error con la tabla {table}: {e}")


if __name__ == "__main__":
    main()

