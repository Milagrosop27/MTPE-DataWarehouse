import sys
from pathlib import Path
import pandas as pd
import logging
from typing import Dict
import importlib.util

print("=== INICIANDO SCRIPT LOAD TO NEONDB ===")

# Configurar paths
project_root = Path(__file__).parent.parent.parent.parent
config_path = project_root / '2_ETL_INTEGRATION' / 'config'
database_file = config_path / 'database.py'

print(f"Project root: {project_root}")
print(f"Config path: {config_path}")
print(f"Database file: {database_file}")
print(f"Database file exists: {database_file.exists()}")

# Cargar módulo database
print("Importando módulo database...")
spec = importlib.util.spec_from_file_location("database", database_file)
database = importlib.util.module_from_spec(spec)
sys.modules['database'] = database
spec.loader.exec_module(database)

# Importar funciones necesarias
NeonDBConnection = database.NeonDBConnection
get_neon_config = database.get_neon_config

print("Módulo database importado correctamente")

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Constantes del proyecto
EXPECTED_TABLES = 13
EXPECTED_DIMENSIONS = 8
EXPECTED_FACTS = 5
EXPECTED_RECORDS = 452732

# Orden de carga  (dimensiones → hechos)
LOAD_ORDER = {
    'dimensiones': [
        'dim_tiempo',           # Compartida
        'dim_ubicacion',        # Compartida
        'dim_postulante',       # Estrella 1: Centro
        'dim_carrera',          # Estrella 1
        'dim_institucion',      # Estrella 1
        'dim_vacante',          # Estrella 2: Centro
        'dim_empresa',          # Estrella 2
        'dim_competencia'       # Estrella 2
    ],
    'hechos': [
        'hechos_postulante',              # Estrella 1
        'hechos_formacion',               # Estrella 1
        'hechos_experiencia',             # Estrella 1
        'hechos_vacante',                 # Estrella 2
        'hechos_competencia_requerida'    # Estrella 2
    ]
}


def verificar_archivos_csv(data_path: Path) -> Dict[str, bool]:
    """Verifica que todos los CSVs necesarios existan"""
    logger.info("Verificando archivos CSV...")

    archivos_faltantes = []
    archivos_ok = []

    for tipo, tablas in LOAD_ORDER.items():
        for tabla in tablas:
            csv_file = data_path / f"{tabla}.csv"
            if csv_file.exists():
                size_mb = csv_file.stat().st_size / (1024 * 1024)
                archivos_ok.append(f"{tabla}.csv ({size_mb:.2f} MB)")
            else:
                archivos_faltantes.append(f"{tabla}.csv")

    if archivos_faltantes:
        logger.error(f"Archivos faltantes: {archivos_faltantes}")
        return {'ok': False, 'faltantes': archivos_faltantes}

    logger.info(f"Todos los archivos CSV encontrados: {len(archivos_ok)}/13")
    return {'ok': True, 'archivos': archivos_ok}


def load_data_warehouse():
    """Carga completa del Data Warehouse a NeonDB"""

    logger.info("=" * 80)
    logger.info("DATA WAREHOUSE MTPE - PROCESO LOAD A NEONDB")
    logger.info("Modelo: Constelación (2 Estrellas) - 13 tablas")
    logger.info("=" * 80)

    # Rutas
    sql_schema = project_root / '2_ETL_INTEGRATION' / 'src' / 'load' / 'create_star_schema.sql'
    data_path = project_root / 'data' / 'integrated'

    # Verificar archivos CSV
    verificacion = verificar_archivos_csv(data_path)
    if not verificacion['ok']:
        logger.error("Faltan archivos CSV. Abortando carga.")
        return False

    # Configuración
    try:
        config = get_neon_config()
        logger.info(f"Conexión configurada: {config.host}/{config.database}")
    except Exception as e:
        logger.error(f"Error obteniendo configuración: {str(e)}")
        logger.error("Verificar archivo .env en: 2_ETL_INTEGRATION/config/.env")
        return False

    try:
        with NeonDBConnection(config) as conn:

            # PASO 1: Crear esquema
            logger.info("\n" + "=" * 80)
            logger.info("PASO 1: Creando esquema del Data Warehouse")
            logger.info("=" * 80)

            if not sql_schema.exists():
                logger.error(f"Archivo SQL no encontrado: {sql_schema}")
                return False

            conn.execute_sql_file(str(sql_schema))
            logger.info("Esquema creado: 13 tablas + 6 vistas analíticas")

            # PASO 2: Cargar dimensiones
            logger.info("\n" + "=" * 80)
            logger.info(f"PASO 2: Cargando {EXPECTED_DIMENSIONS} dimensiones")
            logger.info("=" * 80)

            for i, tabla in enumerate(LOAD_ORDER['dimensiones'], 1):
                csv_file = data_path / f"{tabla}.csv"
                df = pd.read_csv(csv_file)

                logger.info(f"[{i}/{EXPECTED_DIMENSIONS}] Cargando {tabla}...")
                conn.load_dataframe(df, tabla, if_exists='append')

            # PASO 3: Cargar tablas de hechos
            logger.info("\n" + "=" * 80)
            logger.info(f"PASO 3: Cargando {EXPECTED_FACTS} tablas de hechos")
            logger.info("=" * 80)

            for i, tabla in enumerate(LOAD_ORDER['hechos'], 1):
                csv_file = data_path / f"{tabla}.csv"
                df = pd.read_csv(csv_file)

                logger.info(f"[{i}/{EXPECTED_FACTS}] Cargando {tabla}...")
                conn.load_dataframe(df, tabla, if_exists='append')

            # PASO 4: Validar estructura
            logger.info("\n" + "=" * 80)
            logger.info("PASO 4: Validando estructura del Data Warehouse")
            logger.info("=" * 80)

            validation = conn.validate_data_warehouse_structure()

            logger.info(f"Tablas encontradas: {validation['tablas_encontradas']}/{EXPECTED_TABLES}")

            if not validation['estructura_correcta']:
                logger.error("Estructura incompleta")
                if validation['faltantes']:
                    logger.error(f"Faltantes: {validation['faltantes']}")
                if validation['extra']:
                    logger.warning(f"Extra: {validation['extra']}")
                return False

            logger.info("Estructura validada: OK")

            # PASO 5: Estadísticas
            logger.info("\n" + "=" * 80)
            logger.info("PASO 5: Estadísticas de carga")
            logger.info("=" * 80)

            stats = conn.get_table_stats()

            if not stats.empty:
                logger.info("\nDIMENSIONES:")
                dims = stats[stats['tipo'] == 'Dimension'].sort_values('tabla')
                for _, row in dims.iterrows():
                    logger.info(f"   {row['tabla']:<25} {row['registros']:>10,} registros  ({row['tamaño']:>10})")

                logger.info("\nHECHOS:")
                hechos = stats[stats['tipo'] == 'Hecho'].sort_values('tabla')
                for _, row in hechos.iterrows():
                    logger.info(f"   {row['tabla']:<25} {row['registros']:>10,} registros  ({row['tamaño']:>10})")

                total_registros = stats['registros'].sum()
                logger.info("\n" + "-" * 80)
                logger.info(f"TOTAL REGISTROS: {total_registros:,}")
                logger.info(f"ESPERADOS:       {EXPECTED_RECORDS:,}")

                if abs(total_registros - EXPECTED_RECORDS) > 100:
                    logger.warning(f"Diferencia detectada: {total_registros - EXPECTED_RECORDS:,} registros")

            logger.info("\n" + "=" * 80)
            logger.info("CARGA COMPLETADA EXITOSAMENTE")
            logger.info("=" * 80)

            return True

    except Exception as e:
        logger.error(f"Error durante la carga: {str(e)}", exc_info=True)
        return False


def main():
    """Función principal"""
    logger.info("\nIniciando proceso de carga...")
    logger.info(f"Proyecto: {project_root}")

    success = load_data_warehouse()

    if success:
        logger.info("CARGA EXITOSA")
        return 0
    else:
        logger.error("CARGA FALLIDA")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
