import pandas as pd
import numpy as np
from pathlib import Path
import sys


def cargar_datos():
    """Carga el dataset DISCAPACIDAD"""
    DATA_RAW_PATH = Path(__file__).parent.parent.parent / 'data' / 'raw'

    df = pd.read_csv(DATA_RAW_PATH / 'Dataset_DISCAPACIDAD.csv', encoding='utf-8-sig')
    df.columns = df.columns.str.strip().str.upper()

    print(f"Registros originales: {len(df):,}")
    print(f"Columnas: {list(df.columns)}")
    return df


def cargar_postulante_limpio():
    """DESACTIVADO: No validamos integridad referencial en limpieza"""
    print("\nValidacion de integridad referencial OMITIDA en limpieza")
    print("Las relaciones se manejaran en la fase ETL")
    return None


def validar_estructura(df):
    """Valida que existan las columnas esperadas"""
    print("\nValidando estructura del dataset...")

    columnas_requeridas = ['DBIDPOSTULANTE', 'CAUSA', 'DSCORE']

    for col in columnas_requeridas:
        if col not in df.columns:
            raise ValueError(f"Columna requerida no encontrada: {col}")

    print(f"  Todas las columnas requeridas presentes")
    return df


def validar_integridad_referencial(df, ids_postulante_validos):
    """DESACTIVADO: Integridad referencial se valida en ETL, no en limpieza"""
    print("\nIntegridad referencial: Se validara en fase ETL")
    print(f"  Manteniendo todos los {len(df):,} registros")
    return df


def validar_clave_foranea(df):
    """Valida DBIDPOSTULANTE - solo elimina nulos criticos"""
    print("\nValidando clave foranea DBIDPOSTULANTE...")

    inicial = len(df)
    nulos = df['DBIDPOSTULANTE'].isna().sum()
    print(f"  Nulos en DBIDPOSTULANTE: {nulos} ({nulos/inicial*100:.2f}%)")

    if nulos > 0:
        print(f"  Eliminando {nulos} registros sin DBIDPOSTULANTE (nulos criticos)")
        df = df.dropna(subset=['DBIDPOSTULANTE'])

    return df


def limpiar_causa(df):
    """Limpia y estandariza campo CAUSA"""
    print("\nLimpiando campo CAUSA...")

    df['CAUSA'] = df['CAUSA'].astype(str).str.upper().str.strip()
    df.loc[df['CAUSA'] == 'NAN', 'CAUSA'] = np.nan

    nulos = df['CAUSA'].isna().sum()
    valores_unicos = df['CAUSA'].nunique()

    print(f"  Nulos: {nulos} ({nulos/len(df)*100:.2f}%)")
    print(f"  Valores unicos: {valores_unicos}")

    if valores_unicos <= 20 and valores_unicos > 0:
        print(f"  Distribucion de causas:")
        for causa, count in df['CAUSA'].value_counts().head(10).items():
            print(f"    {causa}: {count:,}")

    return df

    return df


def limpiar_dscore(df):
    """Valida y limpia campo DSCORE"""
    print("\nLimpiando campo DSCORE...")

    inicial = len(df)

    # Convertir a numerico
    df['DSCORE'] = pd.to_numeric(df['DSCORE'], errors='coerce')

    nulos = df['DSCORE'].isna().sum()
    print(f"  Nulos: {nulos} ({nulos/len(df)*100:.2f}%)")

    if nulos > 0:
        print(f"  Conservando registros con DSCORE nulo")

    # Validar rango (asumiendo scores entre 0-100)
    if df['DSCORE'].notna().sum() > 0:
        min_score = df['DSCORE'].min()
        max_score = df['DSCORE'].max()
        print(f"  Rango de scores: {min_score:.2f} - {max_score:.2f}")

    return df


def eliminar_duplicados_completos(df):
    """Elimina registros completamente duplicados"""
    print("\nEliminando duplicados completos...")

    inicial = len(df)
    df = df.drop_duplicates()
    eliminados = inicial - len(df)

    print(f"  Duplicados completos eliminados: {eliminados}")

    return df


def analizar_cardinalidad(df):
    """Analiza cuantas discapacidades tiene cada postulante"""
    print("\nAnalizando cardinalidad POSTULANTE:DISCAPACIDAD...")

    conteo = df['DBIDPOSTULANTE'].value_counts()

    print(f"  Postulantes unicos con discapacidad: {len(conteo):,}")
    print(f"  Registros de discapacidad por postulante:")
    print(f"    - Minimo: {conteo.min()}")
    print(f"    - Maximo: {conteo.max()}")
    print(f"    - Promedio: {conteo.mean():.2f}")
    print(f"    - Mediana: {conteo.median():.0f}")

    if conteo.max() > 5:
        print(f"\n  Top 5 postulantes con mas discapacidades:")
        for idx, (id_post, count) in enumerate(conteo.head(5).items(), 1):
            print(f"    {idx}. ID {id_post}: {count} discapacidades")

    return df


def generar_reporte(df_original, df_limpio):
    """Genera reporte de limpieza"""
    print("\n" + "=" * 80)
    print("REPORTE DE LIMPIEZA - DISCAPACIDAD")
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
    print(f"  Postulantes unicos: {df_limpio['DBIDPOSTULANTE'].nunique():,}")
    print(f"  Causas unicas: {df_limpio['CAUSA'].nunique():,}")

    if df_limpio['DSCORE'].notna().sum() > 0:
        print(f"  DSCORE promedio: {df_limpio['DSCORE'].mean():.2f}")


def guardar_datos(df):
    """Guarda el dataset limpio"""
    OUTPUT_PATH = Path(__file__).parent.parent.parent / 'data' / 'cleaned'
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

    output_file = OUTPUT_PATH / 'discapacidad_clean.csv'
    df.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f"\nArchivo guardado: {output_file}")
    print(f"Tamaño del archivo: {output_file.stat().st_size / 1024:.2f} KB")


def main():
    """Funcion principal de limpieza"""
    print("=" * 80)
    print("LIMPIEZA DE DATOS - DISCAPACIDAD")
    print("=" * 80 + "\n")

    try:
        # 1. Cargar datos
        df = cargar_datos()
        df_original = df.copy()

        # 2. Cargar IDs validos de postulante
        ids_postulante_validos = cargar_postulante_limpio()

        # 3. Validar estructura
        df = validar_estructura(df)

        # 4. Validar clave foranea
        df = validar_clave_foranea(df)

        # 5. Validar integridad referencial
        df = validar_integridad_referencial(df, ids_postulante_validos)

        # 6. Limpiar campos individuales
        df = limpiar_causa(df)
        df = limpiar_dscore(df)

        # 7. Eliminar duplicados completos
        df = eliminar_duplicados_completos(df)

        # 8. Analizar cardinalidad
        df = analizar_cardinalidad(df)

        # 9. Generar reporte
        generar_reporte(df_original, df)

        # 10. Guardar datos limpios
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

