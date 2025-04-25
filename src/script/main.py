import os
import shutil
import pandas as pd
import pyodbc

conn_str = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=DAVIDTERAN;'       
    'DATABASE=BioNet;'        
    'Trusted_Connection=yes;' 
)

input_folder = './src/input-labs'
processed_folder = './src/processed'
error_folder = './src/error'

def validar_csv(path):
    try:
        df = pd.read_csv(path)
        columnas = {'laboratorio_id', 'paciente_id', 'tipo_examen', 'resultado', 'fecha_examen'}
        if not columnas.issubset(set(df.columns)):
            return False, "Columnas faltantes"

        if df.isnull().any().any():
            return False, "Datos incompletos"

        try:
            pd.to_datetime(df['fecha_examen'])
        except:
            return False, "Formato fecha incorrecto"

        return True, df
    except Exception as e:
        return False, str(e)

def insertar_datos(df, conn):
    cursor = conn.cursor()
    for _, row in df.iterrows():
        try:
            cursor.execute("""
                IF NOT EXISTS (
                    SELECT 1 FROM resultados_examenes
                    WHERE laboratorio_id = ? AND paciente_id = ? AND tipo_examen = ? AND fecha_examen = ?
                )
                BEGIN
                    INSERT INTO resultados_examenes(laboratorio_id, paciente_id, tipo_examen, resultado, fecha_examen)
                    VALUES (?, ?, ?, ?, ?)
                END
            """,
            row['laboratorio_id'], row['paciente_id'], row['tipo_examen'], row['fecha_examen'],
            row['laboratorio_id'], row['paciente_id'], row['tipo_examen'], row['resultado'], row['fecha_examen'])
        except Exception as e:
            print("Error insertando fila:", e)
    conn.commit()

def procesar_archivos():
    conn = pyodbc.connect(conn_str)
    for archivo in os.listdir(input_folder):
        if archivo.endswith('.csv'):
            path = os.path.join(input_folder, archivo)
            valido, resultado = validar_csv(path)
            if valido:
                insertar_datos(resultado, conn)
                shutil.move(path, os.path.join(processed_folder, archivo))
                print(f"{archivo} procesado correctamente.")
            else:
                shutil.move(path, os.path.join(error_folder, archivo))
                print(f"{archivo} movido a error: {resultado}")
    conn.close()

if __name__ == "__main__":
    procesar_archivos()
