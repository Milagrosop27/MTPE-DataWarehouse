import pandas as pd
import numpy as np
from pathlib import Path
import sys

def cargar_datos():
    """Carga el dataset VACANTES"""
    DATA_RAW_PATH = Path(__file__).parent.parent.parent / 'data' / 'raw'

    df = pd.read_csv(DATA_RAW_PATH / 'Dataset_VACANTES.csv', encoding='utf-8-sig')
    df.columns = df.columns.str.strip().str.upper()

    print(f"Registros originales: {len(df):,}")
    print(f"Columnas: {list(df.columns)}")
    return df

def validar_clave_primaria_vacantes(df: pd.DataFrame) -> pd.DataFrame:
    """Valida AVISOID como clave primaria usando utilidades"""
    return validar_clave_primaria(df, 'AVISOID',
                                   eliminar_duplicados=True,
                                   eliminar_nulos=True)

def limpiar_puesto(df):
    """Limpia campo PUESTO"""
    print("\nLimpiando campo PUESTO...")

    puesto_cols = [col for col in df.columns if 'PUESTO' in col.upper() or 'CARGO' in col.upper()]

    if puesto_cols:
        for col in puesto_cols:
            df[col] = df[col].astype(str).str.upper().str.strip()
            df.loc[df[col] == 'NAN', col] = np.nan

            nulos = df[col].isna().sum()
            valores_unicos = df[col].nunique()

            print(f"  {col}:")
            print(f"    Nulos: {nulos} ({nulos/len(df)*100:.2f}%)")
            print(f"    Valores unicos: {valores_unicos}")
    else:
        print("  No se encontraron columnas de puesto/cargo")

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


def limpiar_ubicacion(df):
    """Limpia campos de ubicacion"""
    print("\nLimpiando campos de ubicacion...")

    ubicacion_cols = [col for col in df.columns if any(x in col.upper() for x in ['DEPARTAMENTO', 'PROVINCIA', 'DISTRITO', 'UBIGEO'])]

    if ubicacion_cols:
        for col in ubicacion_cols:
            if 'UBIGEO' in col.upper():
                df[col] = df[col].astype(str).str.strip().str.zfill(6)
                df.loc[df[col].str.len() != 6, col] = np.nan
            else:
                df[col] = df[col].astype(str).str.upper().str.strip()
                df.loc[df[col] == 'NAN', col] = np.nan

            nulos = df[col].isna().sum()
            print(f"  {col}: {nulos} nulos ({nulos/len(df)*100:.2f}%)")
    else:
        print("  No se encontraron columnas de ubicacion")

    return df


def limpiar_salario(df):
    """Limpia campos de salario"""
    print("\nLimpiando campos de salario...")

    salario_cols = [col for col in df.columns if 'SALARIO' in col.upper() or 'SUELDO' in col.upper() or 'REMUNERACION' in col.upper()]

    if salario_cols:
        for col in salario_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            nulos = df[col].isna().sum()
            print(f"  {col}: {nulos} nulos ({nulos/len(df)*100:.2f}%)")

            if df[col].notna().sum() > 0:
                print(f"    Rango: {df[col].min():.2f} - {df[col].max():.2f}")
    else:
        print("  No se encontraron columnas de salario")

    return df


def limpiar_numero_vacantes(df):
    """Limpia campo numero de vacantes"""
    print("\nLimpiando campo numero de vacantes...")

    vacantes_cols = [col for col in df.columns if 'VACANTE' in col.upper() and 'NUMERO' in col.upper()]

    if vacantes_cols:
        for col in vacantes_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            nulos = df[col].isna().sum()
            print(f"  {col}: {nulos} nulos ({nulos/len(df)*100:.2f}%)")
    else:
        print("  No se encontraron columnas de numero de vacantes")

    return df


def limpiar_estado(df):
    """Limpia campo ESTADO"""
    print("\nLimpiando campo ESTADO...")

    estado_cols = [col for col in df.columns if 'ESTADO' in col.upper()]

    if estado_cols:
        for col in estado_cols:
            df[col] = df[col].astype(str).str.upper().str.strip()
            df.loc[df[col] == 'NAN', col] = np.nan

            nulos = df[col].isna().sum()
            valores_unicos = df[col].nunique()

            print(f"  {col}:")
            print(f"    Nulos: {nulos} ({nulos/len(df)*100:.2f}%)")
            print(f"    Valores unicos: {valores_unicos}")

            if valores_unicos <= 10 and valores_unicos > 0:
                print(f"    Distribucion:")
                for estado, count in df[col].value_counts().items():
                    print(f"      {estado}: {count:,}")
    else:
        print("  No se encontraron columnas de estado")

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
    print("REPORTE DE LIMPIEZA - VACANTES")
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


def guardar_datos(df):
    """Guarda el dataset limpio"""
    OUTPUT_PATH = Path(__file__).parent.parent.parent / 'data' / 'cleaned'
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

    output_file = OUTPUT_PATH / 'vacantes_clean.csv'
    df.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f"\nArchivo guardado: {output_file}")
    print(f"Tamaño del archivo: {output_file.stat().st_size / 1024:.2f} KB")


def main():
    """Funcion principal de limpieza"""
    print("=" * 80)
    print("LIMPIEZA DE DATOS - VACANTES")
    print("=" * 80 + "\n")

    try:
        df = cargar_datos()
        df_original = df.copy()

        df = validar_clave_primaria(df)
        df = limpiar_puesto(df)
        df = limpiar_empresa(df)
        df = limpiar_ubicacion(df)
        df = limpiar_salario(df)
        df = limpiar_numero_vacantes(df)
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

