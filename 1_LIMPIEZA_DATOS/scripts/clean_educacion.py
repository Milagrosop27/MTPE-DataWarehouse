import pandas as pd
import numpy as np
from pathlib import Path
import sys


def cargar_datos():
    """Carga el dataset EDUCACION"""
    DATA_RAW_PATH = Path(__file__).parent.parent.parent / 'data' / 'raw'

    df = pd.read_csv(DATA_RAW_PATH / 'DataSet_EDUCACION.csv', encoding='utf-8-sig')
    df.columns = df.columns.str.strip().str.upper()

    print(f"Registros originales: {len(df):,}")
    print(f"Columnas: {list(df.columns)}")
    return df


def validar_clave_foranea(df):
    """Valida ID_POSTULANTE - solo elimina nulos criticos"""
    print("\nValidando clave foranea ID_POSTULANTE...")

    inicial = len(df)
    nulos = df['ID_POSTULANTE'].isna().sum()
    print(f"  Nulos en ID_POSTULANTE: {nulos} ({nulos/inicial*100:.2f}%)")

    if nulos > 0:
        print(f"  Eliminando {nulos} registros sin ID_POSTULANTE (nulos criticos)")
        df = df.dropna(subset=['ID_POSTULANTE'])

    return df


def limpiar_nivel_educativo(df):
    """Limpia campo NIVEL_EDUCATIVO"""
    print("\nLimpiando campo NIVEL_EDUCATIVO...")

    if 'NIVEL_EDUCATIVO' in df.columns:
        df['NIVEL_EDUCATIVO'] = df['NIVEL_EDUCATIVO'].astype(str).str.upper().str.strip()
        df.loc[df['NIVEL_EDUCATIVO'] == 'NAN', 'NIVEL_EDUCATIVO'] = np.nan

        nulos = df['NIVEL_EDUCATIVO'].isna().sum()
        valores_unicos = df['NIVEL_EDUCATIVO'].nunique()

        print(f"  Nulos: {nulos} ({nulos/len(df)*100:.2f}%)")
        print(f"  Valores unicos: {valores_unicos}")

        if valores_unicos <= 20 and valores_unicos > 0:
            print(f"  Distribucion:")
            for nivel, count in df['NIVEL_EDUCATIVO'].value_counts().head(10).items():
                print(f"    {nivel}: {count:,}")
    else:
        print("  Columna NIVEL_EDUCATIVO no encontrada")

    return df


def limpiar_carrera(df):
    """Limpia y normaliza campo CARRERA"""
    print("\nLimpiando y normalizando campo CARRERA...")

    if 'CARRERA' in df.columns:
        import unicodedata
        import re

        inicial_unicos = df['CARRERA'].nunique()

        df['CARRERA'] = df['CARRERA'].astype(str).str.upper().str.strip()
        df.loc[df['CARRERA'] == 'NAN', 'CARRERA'] = np.nan

        def normalizar_carrera(texto):
            """Normaliza nombres de carreras"""
            if pd.isna(texto) or str(texto).upper() == 'NAN':
                return np.nan

            texto = str(texto).upper().strip()
            texto = ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
            palabras_remover = [' DE ', ' EN ', ' Y ', ' E ', ' LA ', ' DEL ', ' LAS ', ' LOS ', ' PARA ']
            for palabra in palabras_remover:
                texto = texto.replace(palabra, ' ')
            texto = re.sub(r'\s+', ' ', texto).strip()
            texto = re.sub(r'[^A-Z0-9\s]', '', texto)

            return texto if texto else np.nan

        df['CARRERA'] = df['CARRERA'].apply(normalizar_carrera)

        nulos = df['CARRERA'].isna().sum()
        valores_unicos = df['CARRERA'].nunique()

        print(f"  Nulos: {nulos} ({nulos/len(df)*100:.2f}%)")
        print(f"  Valores unicos originales: {inicial_unicos:,}")
        print(f"  Valores unicos normalizados: {valores_unicos:,}")
        print(f"  Duplicados eliminados: {inicial_unicos - valores_unicos:,}")
    else:
        print("  Columna CARRERA no encontrada")

    return df


def limpiar_estado(df):
    """Limpia campo ESTADO"""
    print("\nLimpiando campo ESTADO...")

    if 'ESTADO' in df.columns:
        df['ESTADO'] = df['ESTADO'].astype(str).str.upper().str.strip()
        df.loc[df['ESTADO'] == 'NAN', 'ESTADO'] = np.nan

        nulos = df['ESTADO'].isna().sum()
        valores_unicos = df['ESTADO'].nunique()

        print(f"  Nulos: {nulos} ({nulos/len(df)*100:.2f}%)")
        print(f"  Valores unicos: {valores_unicos}")

        if valores_unicos <= 10 and valores_unicos > 0:
            print(f"  Distribucion:")
            for estado, count in df['ESTADO'].value_counts().items():
                print(f"    {estado}: {count:,}")
    else:
        print("  Columna ESTADO no encontrada")

    return df


def limpiar_institucion(df):
    """Limpia y normaliza campo INSTITUCION"""
    print("\nLimpiando y normalizando campo INSTITUCION...")

    if 'INSTITUCION' in df.columns:
        import unicodedata
        import re

        inicial_unicos = df['INSTITUCION'].nunique()

        df['INSTITUCION'] = df['INSTITUCION'].astype(str).str.upper().str.strip()
        df.loc[df['INSTITUCION'] == 'NAN', 'INSTITUCION'] = np.nan

        def normalizar_institucion(texto):
            """Normaliza nombres de instituciones"""
            if pd.isna(texto) or str(texto).upper() == 'NAN':
                return np.nan

            texto = str(texto).upper().strip()
            texto = ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
            texto = re.sub(r'\s+', ' ', texto).strip()
            texto = re.sub(r'[^A-Z0-9\s]', '', texto)

            return texto if texto else np.nan

        df['INSTITUCION'] = df['INSTITUCION'].apply(normalizar_institucion)

        nulos = df['INSTITUCION'].isna().sum()
        valores_unicos = df['INSTITUCION'].nunique()

        print(f"  Nulos: {nulos} ({nulos/len(df)*100:.2f}%)")
        print(f"  Valores unicos originales: {inicial_unicos:,}")
        print(f"  Valores unicos normalizados: {valores_unicos:,}")
        print(f"  Duplicados eliminados: {inicial_unicos - valores_unicos:,}")
    else:
        print("  Columna INSTITUCION no encontrada")

    return df


def limpiar_fechas(df):
    """Limpia campos de fecha"""
    print("\nLimpiando campos de fecha...")

    fecha_cols = [col for col in df.columns if 'FECHA' in col.upper()]

    if fecha_cols:
        for col in fecha_cols:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                nulos = df[col].isna().sum()
                print(f"  {col}: {nulos} nulos ({nulos/len(df)*100:.2f}%)")
            except:
                print(f"  {col}: Error al convertir")
    else:
        print("  No se encontraron columnas de fecha")

    return df


def eliminar_duplicados_completos(df):
    """Elimina registros completamente duplicados"""
    print("\nEliminando duplicados completos...")

    inicial = len(df)
    df = df.drop_duplicates()
    eliminados = inicial - len(df)

    print(f"  Duplicados completos eliminados: {eliminados}")

    return df


def generar_reporte(df_original, df_limpio):
    """Genera reporte de limpieza"""
    print("\n" + "=" * 80)
    print("REPORTE DE LIMPIEZA - EDUCACION")
    print("=" * 80)

    print(f"\nRegistros originales: {len(df_original):,}")
    print(f"Registros finales: {len(df_limpio):,}")
    print(f"Registros eliminados: {len(df_original) - len(df_limpio):,}")
    print(f"Porcentaje retenido: {len(df_limpio)/len(df_original)*100:.2f}%")

    print("\nCalidad de datos finales:")
    total = len(df_limpio)
    for col in df_limpio.columns:
        nulos = df_limpio[col].isna().sum()
        pct = nulos / total * 100
        print(f"  {col}: {nulos:,} nulos ({pct:.2f}%)")

    print("\nEstadisticas finales:")
    print(f"  Postulantes unicos: {df_limpio['ID_POSTULANTE'].nunique():,}")


def guardar_datos(df):
    """Guarda el dataset limpio"""
    OUTPUT_PATH = Path(__file__).parent.parent.parent / 'data' / 'cleaned'
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

    output_file = OUTPUT_PATH / 'educacion_clean.csv'
    df.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f"\nArchivo guardado: {output_file}")
    print(f"Tamaño del archivo: {output_file.stat().st_size / 1024:.2f} KB")


def main():
    """Funcion principal de limpieza"""
    print("=" * 80)
    print("LIMPIEZA DE DATOS - EDUCACION")
    print("=" * 80 + "\n")

    try:
        df = cargar_datos()
        df_original = df.copy()

        df = validar_clave_foranea(df)
        df = limpiar_nivel_educativo(df)
        df = limpiar_carrera(df)
        df = limpiar_institucion(df)
        df = limpiar_estado(df)
        df = limpiar_fechas(df)
        df = eliminar_duplicados_completos(df)

        generar_reporte(df_original, df)
        guardar_datos(df)

        print("\n" + "=" * 80)
        print("LIMPIEZA COMPLETADA EXITOSAMENTE")
        print("=" * 80 + "\n")

        return 0

    except Exception as e:
        print(f"\nERROR durante la limpieza: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

