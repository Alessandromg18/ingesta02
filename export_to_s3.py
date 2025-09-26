import os
import pandas as pd
from sqlalchemy import create_engine
import boto3

# ================================
# 🔧 CONFIGURACIÓN A EDITAR
# ================================

# Conexión MySQL
DB_HOST = "172.31.27.82"
DB_PORT = 3307
DB_USER = "root"
DB_PASS = "utec"
DB_NAME = "qa_db"

# Nombre del bucket (📌 CAMBIA AQUÍ)
BUCKET_NAME = "alessandro-ingesta"

# Diccionario de tablas y carpetas en S3 (📌 CAMBIA AQUÍ)
TABLES = {
    "quest_and_answer": "questions_folder/",
    "user_apify_call_historial": "historial_folder/",
    "user_apify_filters": "filters_folder/",
    "scraped_account": "scraped_folder/"
}

# ================================
# 🚀 LÓGICA DEL PROGRAMA
# ================================

# Conexión MySQL
engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Cliente S3 (usa credenciales montadas en docker run)
s3 = boto3.client("s3")


def main():
    # 1. Eliminar todos los CSV previos en el bucket
    print("🔄 Eliminando archivos previos en el bucket...")
    try:
        objects = s3.list_objects_v2(Bucket=BUCKET_NAME)
        if "Contents" in objects:
            for obj in objects["Contents"]:
                if obj["Key"].endswith(".csv"):
                    s3.delete_object(Bucket=BUCKET_NAME, Key=obj["Key"])
            print("✅ Archivos CSV previos eliminados.")
        else:
            print("ℹ️ No había archivos CSV en el bucket.")
    except Exception as e:
        print(f"⚠️ Error al limpiar bucket: {e}")

    # 2. Exportar cada tabla y subir a su carpeta
    for table, folder in TABLES.items():
        try:
            print(f"📥 Exportando tabla: {table}")
            df = pd.read_sql(f"SELECT * FROM {table}", engine)

            filename = f"{table}.csv"
            df.to_csv(filename, index=False)

            # Generar clave (carpeta + archivo)
            s3_key = f"{folder}{filename}"

            # 📌 Subir archivo
            print(f"⬆️ Subiendo {filename} a s3://{BUCKET_NAME}/{s3_key} ...")
            s3.upload_file(filename, BUCKET_NAME, s3_key)
            print(f"✅ {filename} subido correctamente en {folder}")

            # Borrar archivo local
            os.remove(filename)
        except Exception as e:
            print(f"⚠️ Error con la tabla {table}: {e}")


if __name__ == "__main__":
    main()
