import pandas as pd
import numpy as np
from pathlib import Path
import sys


def cargar_datos():
    """Carga el dataset EXPERIENCIAS"""
    DATA_RAW_PATH = Path(__file__).parent.parent.parent / 'data' / 'raw'

    df = pd.read_csv(DATA_RAW_PATH / 'Dataset_EXPERIENCIASLABORALES.csv',
                     encoding='latin1', on_bad_lines='skip')

    # Limpiar nombres de columnas - eliminar caracteres especiales
    df.columns = df.columns.str.replace('Ÿ', '').str.replace('ÿ', '').str.strip().str.upper()

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


def limpiar_cargo(df):
    """Limpia campo CARGO/PUESTO"""
    print("\nLimpiando campo CARGO/PUESTO...")

    cargo_cols = [col for col in df.columns if 'CARGO' in col.upper() or 'PUESTO' in col.upper()]

    if cargo_cols:
        for col in cargo_cols:
            df[col] = df[col].astype(str).str.upper().str.strip()
            df.loc[df[col] == 'NAN', col] = np.nan

            nulos = df[col].isna().sum()
            valores_unicos = df[col].nunique()

            print(f"  {col}:")
            print(f"    Nulos: {nulos} ({nulos/len(df)*100:.2f}%)")
            print(f"    Valores unicos: {valores_unicos}")
    else:
        print("  No se encontraron columnas de cargo/puesto")

    return df


def limpiar_empresa(df):
    """Limpia campo EMPRESA"""
    print("\nLimpiando campo EMPRESA...")

    empresa_cols = [col for col in df.columns if 'EMPRESA' in col.upper()]

    if empresa_cols:
        for col in empresa_cols:
            df[col] = df[col].astype(str).str.upper().str.strip()
            df.loc[df[col] == 'NAN', col] = np.nan

            nulos = df[col].isna().sum()
            print(f"  {col}: {nulos} nulos ({nulos/len(df)*100:.2f}%)")
    else:
        print("  No se encontraron columnas de empresa")

    return df


def limpiar_sector(df):
    """Limpia campo SECTOR/INDUSTRIA"""
    print("\nLimpiando campo SECTOR/INDUSTRIA...")

    sector_cols = [col for col in df.columns if 'SECTOR' in col.upper() or 'INDUSTRIA' in col.upper()]

    if sector_cols:
        for col in sector_cols:
            df[col] = df[col].astype(str).str.upper().str.strip()
            df.loc[df[col] == 'NAN', col] = np.nan

            nulos = df[col].isna().sum()
            valores_unicos = df[col].nunique()

            print(f"  {col}:")
            print(f"    Nulos: {nulos} ({nulos/len(df)*100:.2f}%)")
            print(f"    Valores unicos: {valores_unicos}")
    else:
        print("  No se encontraron columnas de sector/industria")

    return df


def limpiar_fechas(df):
    """Limpia campos de fecha"""
    print("\nLimpiando campos de fecha...")

    fecha_cols = [col for col in df.columns if 'FECHA' in col.upper() or 'INICIO' in col.upper() or 'FIN' in col.upper()]

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


def limpiar_duracion(df):
    """Limpia campos de duracion"""
    print("\nLimpiando campos de duracion...")

    duracion_cols = [col for col in df.columns if 'DURACION' in col.upper() or 'MESES' in col.upper() or 'AÑOS' in col.upper()]

    if duracion_cols:
        for col in duracion_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            nulos = df[col].isna().sum()
            print(f"  {col}: {nulos} nulos ({nulos/len(df)*100:.2f}%)")
    else:
        print("  No se encontraron columnas de duracion")

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
    print("REPORTE DE LIMPIEZA - EXPERIENCIAS LABORALES")
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

    output_file = OUTPUT_PATH / 'experiencias_clean.csv'
    df.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f"\nArchivo guardado: {output_file}")
    print(f"Tamaño del archivo: {output_file.stat().st_size / 1024:.2f} KB")


def main():
    """Funcion principal de limpieza"""
    print("=" * 80)
    print("LIMPIEZA DE DATOS - EXPERIENCIAS LABORALES")
    print("=" * 80 + "\n")

    try:
        df = cargar_datos()
        df_original = df.copy()

        df = validar_clave_foranea(df)
        df = limpiar_cargo(df)
        df = limpiar_empresa(df)
        df = limpiar_sector(df)
        df = limpiar_fechas(df)
        df = limpiar_duracion(df)
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

