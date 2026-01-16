import pandas as pd
import numpy as np
import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)


def limpiar_ubicacion(
    df: pd.DataFrame,
    columnas_geo: Optional[List[str]] = None
) -> pd.DataFrame:

    if columnas_geo is None:
        columnas_geo = ['DEPARTAMENTO', 'PROVINCIA', 'DISTRITO', 'UBIGEO']

    logger.info("Limpiando campos de ubicacion...")

    for col in columnas_geo:
        if col not in df.columns:
            continue

        if col == 'UBIGEO':
            # UBIGEO debe ser string de 6 dígitos
            df[col] = df[col].astype(str).str.strip().str.zfill(6)
            df.loc[df[col].str.len() != 6, col] = np.nan
            nulos = df[col].isna().sum()
            logger.info(f"  {col} - Nulos: {nulos} ({nulos/len(df)*100:.2f}%)")
        else:
            # Estandarizar texto
            df[col] = df[col].astype(str).str.upper().str.strip()
            df.loc[df[col] == 'NAN', col] = np.nan
            nulos = df[col].isna().sum()
            logger.info(f"  {col} - Nulos: {nulos} ({nulos/len(df)*100:.2f}%)")

    return df


def validar_clave_primaria(
    df: pd.DataFrame,
    columna: str,
    eliminar_duplicados: bool = True,
    eliminar_nulos: bool = True
) -> pd.DataFrame:

    logger.info(f"Validando clave primaria: {columna}")

    if columna not in df.columns:
        logger.error(f"Columna {columna} no encontrada en DataFrame")
        return df

    duplicados = df[columna].duplicated().sum()
    nulos = df[columna].isna().sum()

    logger.info(f"  Duplicados: {duplicados}")
    logger.info(f"  Nulos: {nulos}")

    if duplicados > 0 and eliminar_duplicados:
        logger.warning(f"  Eliminando {duplicados} duplicados en {columna}")
        df = df.drop_duplicates(subset=[columna], keep='first')

    if nulos > 0 and eliminar_nulos:
        logger.warning(f"  Eliminando {nulos} registros sin {columna}")
        df = df.dropna(subset=[columna])

    return df


def limpiar_campo_numerico(
    df: pd.DataFrame,
    columna: str,
    rango_valido: Optional[tuple] = None,
    eliminar_invalidos: bool = True
) -> pd.DataFrame:

    logger.info(f"Limpiando campo numerico: {columna}")

    if columna not in df.columns:
        logger.error(f"Columna {columna} no encontrada")
        return df

    inicial = len(df)

    # Convertir a numérico
    df[columna] = pd.to_numeric(df[columna], errors='coerce')
    nulos = df[columna].isna().sum()

    # Validar rango
    if rango_valido and eliminar_invalidos:
        min_val, max_val = rango_valido
        df = df[(df[columna] >= min_val) & (df[columna] <= max_val)]
        eliminados = inicial - len(df)
        logger.info(f"  Registros eliminados por rango invalido: {eliminados}")
        logger.info(f"  Rango valido: {df[columna].min():.0f} - {df[columna].max():.0f}")

    logger.info(f"  Nulos: {nulos} ({nulos/inicial*100:.2f}%)")

    return df


def limpiar_campo_categorico(
    df: pd.DataFrame,
    columna: str,
    valores_validos: Optional[List[str]] = None,
    mapeo: Optional[Dict[str, str]] = None,
    normalizar: bool = True
) -> pd.DataFrame:

    logger.info(f"Limpiando campo categorico: {columna}")

    if columna not in df.columns:
        logger.error(f"Columna {columna} no encontrada")
        return df

    if normalizar:
        df[columna] = df[columna].astype(str).str.upper().str.strip()
        df.loc[df[columna] == 'NAN', columna] = np.nan

    # Aplicar mapeo
    if mapeo:
        df[columna] = df[columna].replace(mapeo)
        logger.info(f"  Mapeo aplicado: {len(mapeo)} transformaciones")

    # Validar valores
    if valores_validos:
        nulos_antes = df[columna].isna().sum()
        invalidos = (~df[columna].isin(valores_validos) & df[columna].notna()).sum()
        df.loc[~df[columna].isin(valores_validos), columna] = np.nan
        logger.info(f"  Nulos: {nulos_antes}, Invalidos corregidos a nulo: {invalidos}")

    if df[columna].notna().sum() > 0:
        logger.info(f"  Distribucion: {df[columna].value_counts().to_dict()}")

    return df


def eliminar_duplicados_completos(df: pd.DataFrame) -> pd.DataFrame:

    logger.info("Eliminando duplicados completos...")

    inicial = len(df)
    df = df.drop_duplicates()
    eliminados = inicial - len(df)

    if eliminados > 0:
        logger.warning(f"  Eliminados {eliminados} registros duplicados")
    else:
        logger.info("  No se encontraron duplicados completos")

    return df


def generar_resumen_limpieza(
    df_original: pd.DataFrame,
    df_limpio: pd.DataFrame,
    dataset_name: str
) -> Dict:

    registros_eliminados = len(df_original) - len(df_limpio)
    pct_retenido = (len(df_limpio) / len(df_original) * 100) if len(df_original) > 0 else 0

    resumen = {
        'dataset': dataset_name,
        'registros_originales': len(df_original),
        'registros_finales': len(df_limpio),
        'registros_eliminados': registros_eliminados,
        'porcentaje_retenido': pct_retenido,
        'columnas': len(df_limpio.columns)
    }

    logger.info(f"\n{'='*60}")
    logger.info(f"RESUMEN DE LIMPIEZA: {dataset_name}")
    logger.info(f"{'='*60}")
    logger.info(f"  Registros originales: {resumen['registros_originales']:,}")
    logger.info(f"  Registros finales: {resumen['registros_finales']:,}")
    logger.info(f"  Registros eliminados: {resumen['registros_eliminados']:,}")
    logger.info(f"  Porcentaje retenido: {resumen['porcentaje_retenido']:.2f}%")
    logger.info(f"  Columnas: {resumen['columnas']}")
    logger.info(f"{'='*60}\n")

    return resumen


def configurar_logging(nombre_script: str, nivel: str = 'INFO') -> logging.Logger:

    logging.basicConfig(
        level=getattr(logging, nivel),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logger = logging.getLogger(nombre_script)
    logger.info(f"Iniciando script: {nombre_script}")

    return logger

