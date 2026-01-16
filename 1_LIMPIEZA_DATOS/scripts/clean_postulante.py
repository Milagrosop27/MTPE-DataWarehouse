import pandas as pd
import numpy as np
from pathlib import Path
import sys

def cargar_datos():
    """Carga el dataset"""
    DATA_RAW_PATH = Path(__file__).parent.parent.parent / 'data' / 'raw'

    df = pd.read_csv(DATA_RAW_PATH / 'Dataset_POSTULANTE.csv', encoding='utf-8-sig')

    # Limpiar nombres de columnas
    df.columns = df.columns.str.strip().str.upper()
    print(f"Registros originales: {len(df):,}")
    print(f"Columnas: {list(df.columns)}")
    return df

def limpiar_columnas(df):
    """Verifica columnas ya están limpias"""
    print(f"\nColumnas disponibles: {list(df.columns)}")
    return df

def validar_claves_primarias(df):
    """Validar unicidad de claves primarias"""
    print("\nValidando claves primarias...")

    # ID_POSTULANTE
    duplicados_id = df['ID_POSTULANTE'].duplicated().sum()
    nulos_id = df['ID_POSTULANTE'].isna().sum()

    print(f"  ID_POSTULANTE - Duplicados: {duplicados_id}, Nulos: {nulos_id}")

    if duplicados_id > 0:
        print(f"  Eliminando {duplicados_id} duplicados en ID_POSTULANTE")
        df = df.drop_duplicates(subset=['ID_POSTULANTE'], keep='first')

    if nulos_id > 0:
        print(f"  Eliminando {nulos_id} registros sin ID_POSTULANTE")
        df = df.dropna(subset=['ID_POSTULANTE'])

    # DOC_ID
    duplicados_doc = df['DOC_ID'].duplicated().sum()
    nulos_doc = df['DOC_ID'].isna().sum()

    print(f"  DOC_ID - Duplicados: {duplicados_doc}, Nulos: {nulos_doc}")

    if duplicados_doc > 0:
        print(f"  Eliminando {duplicados_doc} duplicados en DOC_ID")
        df = df.drop_duplicates(subset=['DOC_ID'], keep='first')

    return df


def limpiar_edad(df):
    """Valida y limpia campo EDAD"""
    print("\nLimpiando campo EDAD...")

    inicial = len(df)

    # Convertir a numerico
    df['EDAD'] = pd.to_numeric(df['EDAD'], errors='coerce')

    # Filtrar edades válidas (16-100 años)
    df = df[(df['EDAD'] >= 16) & (df['EDAD'] <= 100)]

    eliminados = inicial - len(df)
    print(f"  Registros eliminados por edad invalida: {eliminados}")
    print(f"  Rango valido: {df['EDAD'].min():.0f} - {df['EDAD'].max():.0f} años")

    return df


def limpiar_sexo(df):
    """Estandariza campo SEXO"""
    print("\nLimpiando campo SEXO...")

    df['SEXO'] = df['SEXO'].str.upper().str.strip()

    # Mapeo de valores
    mapeo_sexo = {
        'M': 'M',
        'MASCULINO': 'M',
        'HOMBRE': 'M',
        'F': 'F',
        'FEMENINO': 'F',
        'MUJER': 'F'
    }

    df['SEXO'] = df['SEXO'].replace(mapeo_sexo)

    # Valores validos
    valores_validos = ['M', 'F']
    nulos_antes = df['SEXO'].isna().sum()
    invalidos = (~df['SEXO'].isin(valores_validos) & df['SEXO'].notna()).sum()

    df.loc[~df['SEXO'].isin(valores_validos), 'SEXO'] = np.nan

    print(f"  Nulos: {nulos_antes}, Invalidos corregidos a nulo: {invalidos}")
    print(f"  Distribucion: {df['SEXO'].value_counts().to_dict()}")

    return df


def limpiar_ubicacion_postulante(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia campos de ubicacion geografica usando utilidades"""
    return limpiar_ubicacion(df, columnas_geo=['DEPARTAMENTO', 'PROVINCIA', 'DISTRITO', 'UBIGEO'])


def limpiar_estado_conadis(df):
    """Limpia campo ESTADO_CONADIS"""
    print("\nLimpiando campo ESTADO_CONADIS...")

    df['ESTADO_CONADIS'] = df['ESTADO_CONADIS'].astype(str).str.upper().str.strip()
    df.loc[df['ESTADO_CONADIS'] == 'NAN', 'ESTADO_CONADIS'] = np.nan

    nulos = df['ESTADO_CONADIS'].isna().sum()
    print(f"  Nulos: {nulos} ({nulos/len(df)*100:.2f}%)")

    if df['ESTADO_CONADIS'].notna().sum() > 0:
        print(f"  Valores unicos: {df['ESTADO_CONADIS'].nunique()}")

    return df


def eliminar_duplicados_completos(df):
    """Elimina registros completamente duplicados"""
    print("\nEliminando duplicados completos...")

    inicial = len(df)
    df = df.drop_duplicates()
    eliminados = inicial - len(df)

    print(f"  Duplicados completos eliminados: {eliminados}")

    return df


def validar_integridad_final(df):
    """Valida integridad del dataset limpio"""
    print("\nValidacion final de integridad...")

    # PKs unicas
    assert df['ID_POSTULANTE'].is_unique, "ID_POSTULANTE no es unico"
    assert df['DOC_ID'].is_unique, "DOC_ID no es unico"

    # Sin nulos en PKs
    assert df['ID_POSTULANTE'].notna().all(), "ID_POSTULANTE tiene nulos"
    assert df['DOC_ID'].notna().all(), "DOC_ID tiene nulos"

    print("  Todas las validaciones pasaron correctamente")

    return df


def generar_reporte(df_original, df_limpio):
    """Genera reporte de limpieza"""
    print("\n" + "=" * 80)
    print("REPORTE DE LIMPIEZA - POSTULANTE")
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

    print("\nEstadisticas descriptivas:")
    print(f"  Edad promedio: {df_limpio['EDAD'].mean():.1f} años")
    print(f"  Edad mediana: {df_limpio['EDAD'].median():.1f} años")
    print(f"  Distribucion por sexo:")
    for sexo, count in df_limpio['SEXO'].value_counts().items():
        print(f"    {sexo}: {count:,} ({count/total*100:.2f}%)")



def guardar_datos(df):
    """Guarda el dataset limpio"""
    OUTPUT_PATH = Path(__file__).parent.parent.parent / 'data' / 'cleaned'
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

    output_file = OUTPUT_PATH / 'postulante_clean.csv'
    df.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f"\nArchivo guardado: {output_file}")
    print(f"Tamaño del archivo: {output_file.stat().st_size / 1024:.2f} KB")


def main() -> int:
    """
    Ejecuta el pipeline de limpieza completo para POSTULANTE
    Returns:
        int: 0 si exitoso, 1 si fallo
    """
    print("=" * 80)
    print("LIMPIEZA DE DATOS - POSTULANTE")
    print("=" * 80 + "\n")

    try:
        # 1. Cargar datos
        df = cargar_datos()
        df_original = df.copy()

        # 2. Limpiar columnas
        df = limpiar_columnas(df)

        # 3. Validar y limpiar claves primarias
        df = validar_claves_primarias(df)

        # 4. Limpiar campos individuales
        df = limpiar_edad(df)
        df = limpiar_sexo(df)
        df = limpiar_ubicacion_postulante(df)
        df = limpiar_estado_conadis(df)

        # 5. Eliminar duplicados completos
        df = eliminar_duplicados_completos(df)

        # 6. Validar integridad final
        df = validar_integridad_final(df)

        # 7. Generar reporte
        generar_reporte(df_original, df)

        # 8. Guardar datos limpios
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

