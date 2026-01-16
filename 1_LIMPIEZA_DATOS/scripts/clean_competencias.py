import pandas as pd
import numpy as np
from pathlib import Path
import sys


def cargar_datos():
    """Carga el dataset COMPETENCIAS"""
    DATA_RAW_PATH = Path(__file__).parent.parent.parent / 'data' / 'raw'

    try:
        df = pd.read_csv(DATA_RAW_PATH / 'DataSet_COMPETENCIAS.csv', encoding='utf-8-sig')
    except:
        df = pd.read_csv(DATA_RAW_PATH / 'DataSet_COMPETENCIAS.csv', encoding='latin1')

    df.columns = df.columns.str.strip().str.upper()

    print(f"Registros originales: {len(df):,}")
    print(f"Columnas: {list(df.columns)}")
    return df


def validar_clave_foranea(df):
    """Valida AVISOID - solo elimina nulos criticos"""
    print("\nValidando clave foranea AVISOID...")

    if 'AVISOID' in df.columns:
        inicial = len(df)
        nulos = df['AVISOID'].isna().sum()
        print(f"  Nulos en AVISOID: {nulos} ({nulos/inicial*100:.2f}%)")

        if nulos > 0:
            print(f"  Eliminando {nulos} registros sin AVISOID (nulos criticos)")
            df = df.dropna(subset=['AVISOID'])
    else:
        print("  Columna AVISOID no encontrada")

    return df


def limpiar_competencia(df):
    """Limpia campo COMPETENCIA"""
    print("\nLimpiando campo COMPETENCIA...")

    comp_cols = [col for col in df.columns if 'COMPETENCIA' in col.upper()]

    if comp_cols:
        for col in comp_cols:
            df[col] = df[col].astype(str).str.upper().str.strip()
            df.loc[df[col] == 'NAN', col] = np.nan

            nulos = df[col].isna().sum()
            valores_unicos = df[col].nunique()

            print(f"  {col}:")
            print(f"    Nulos: {nulos} ({nulos/len(df)*100:.2f}%)")
            print(f"    Valores unicos: {valores_unicos}")

            if valores_unicos <= 20 and valores_unicos > 0:
                print(f"    Top 10 competencias:")
                for idx, (comp, count) in enumerate(df[col].value_counts().head(10).items(), 1):
                    print(f"      {idx}. {comp}: {count:,}")
    else:
        print("  No se encontraron columnas de competencia")

    return df


def limpiar_tipo_competencia(df):
    """Limpia campo TIPO de competencia"""
    print("\nLimpiando campo TIPO de competencia...")

    tipo_cols = [col for col in df.columns if 'TIPO' in col.upper()]

    if tipo_cols:
        for col in tipo_cols:
            df[col] = df[col].astype(str).str.upper().str.strip()
            df.loc[df[col] == 'NAN', col] = np.nan

            nulos = df[col].isna().sum()
            valores_unicos = df[col].nunique()

            print(f"  {col}:")
            print(f"    Nulos: {nulos} ({nulos/len(df)*100:.2f}%)")
            print(f"    Valores unicos: {valores_unicos}")

            if valores_unicos <= 10 and valores_unicos > 0:
                print(f"    Distribucion:")
                for tipo, count in df[col].value_counts().items():
                    print(f"      {tipo}: {count:,}")
    else:
        print("  No se encontraron columnas de tipo de competencia")

    return df


def limpiar_nivel(df):
    """Limpia campo NIVEL de competencia"""
    print("\nLimpiando campo NIVEL de competencia...")

    nivel_cols = [col for col in df.columns if 'NIVEL' in col.upper()]

    if nivel_cols:
        for col in nivel_cols:
            df[col] = df[col].astype(str).str.upper().str.strip()
            df.loc[df[col] == 'NAN', col] = np.nan

            nulos = df[col].isna().sum()
            valores_unicos = df[col].nunique()

            print(f"  {col}:")
            print(f"    Nulos: {nulos} ({nulos/len(df)*100:.2f}%)")
            print(f"    Valores unicos: {valores_unicos}")

            if valores_unicos <= 10 and valores_unicos > 0:
                print(f"    Distribucion:")
                for nivel, count in df[col].value_counts().items():
                    print(f"      {nivel}: {count:,}")
    else:
        print("  No se encontraron columnas de nivel de competencia")

    return df


def limpiar_experiencia_requerida(df):
    """Limpia campo de experiencia requerida"""
    print("\nLimpiando campo de experiencia requerida...")

    exp_cols = [col for col in df.columns if 'EXPERIENCIA' in col.upper()]

    if exp_cols:
        for col in exp_cols:
            # Intentar convertir a numerico si parece ser años
            if any(x in col.upper() for x in ['AÑOS', 'MESES', 'AÑO']):
                df[col] = pd.to_numeric(df[col], errors='coerce')
            else:
                df[col] = df[col].astype(str).str.upper().str.strip()
                df.loc[df[col] == 'NAN', col] = np.nan

            nulos = df[col].isna().sum()
            print(f"  {col}: {nulos} nulos ({nulos/len(df)*100:.2f}%)")
    else:
        print("  No se encontraron columnas de experiencia requerida")

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
    print("REPORTE DE LIMPIEZA - COMPETENCIAS")
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

    if 'AVISOID' in df_limpio.columns:
        print("\nEstadisticas finales:")
        print(f"  Vacantes unicas: {df_limpio['AVISOID'].nunique():,}")


def guardar_datos(df):
    """Guarda el dataset limpio"""
    OUTPUT_PATH = Path(__file__).parent.parent.parent / 'data' / 'cleaned'
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

    output_file = OUTPUT_PATH / 'competencias_clean.csv'
    df.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f"\nArchivo guardado: {output_file}")
    print(f"Tamaño del archivo: {output_file.stat().st_size / 1024:.2f} KB")


def main():
    """Funcion principal de limpieza"""
    print("=" * 80)
    print("LIMPIEZA DE DATOS - COMPETENCIAS")
    print("=" * 80 + "\n")

    try:
        df = cargar_datos()
        df_original = df.copy()

        df = validar_clave_foranea(df)
        df = limpiar_competencia(df)
        df = limpiar_tipo_competencia(df)
        df = limpiar_nivel(df)
        df = limpiar_experiencia_requerida(df)
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

