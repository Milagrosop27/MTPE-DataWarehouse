import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from dataclasses import dataclass
from typing import List, Dict
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno desde .env
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)


@dataclass
class NeonDBConfig:
    """Configuración de conexión a NeonDB"""
    host: str
    database: str
    user: str
    password: str
    port: int = 5432
    sslmode: str = 'require'

    @classmethod
    def from_env(cls) -> 'NeonDBConfig':
        """Carga configuración desde variables de entorno"""
        return cls(
            host=os.getenv('NEON_HOST', 'your-project.neon.tech'),
            database=os.getenv('NEON_DATABASE', 'neondb'),
            user=os.getenv('NEON_USER', 'your_username'),
            password=os.getenv('NEON_PASSWORD', 'your_password'),
            port=int(os.getenv('NEON_PORT', '5432')),
            sslmode=os.getenv('NEON_SSLMODE', 'require')
        )

    def get_connection_string(self) -> str:
        """Retorna connection string para psycopg2"""
        return (
            f"host={self.host} "
            f"dbname={self.database} "
            f"user={self.user} "
            f"password={self.password} "
            f"port={self.port} "
            f"sslmode={self.sslmode}"
        )

    def get_sqlalchemy_url(self) -> str:
        """Retorna URL para SQLAlchemy/Pandas"""
        return (
            f"postgresql://{self.user}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}?sslmode={self.sslmode}"
        )


class NeonDBConnection:

    def __init__(self, config: NeonDBConfig):
        self.config = config
        self.connection = None
        self.cursor = None

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()

    def connect(self):
        """Establece conexión a NeonDB"""
        try:
            self.connection = psycopg2.connect(
                host=self.config.host,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                port=self.config.port,
                sslmode=self.config.sslmode
            )
            self.cursor = self.connection.cursor()
            logger.info(f"Conexion establecida a NeonDB: {self.config.database}@{self.config.host}")
            return self.connection
        except psycopg2.OperationalError as e:
            logger.error(f"Error de conexion a NeonDB: {str(e)}")
            logger.error("Verificar credenciales en archivo .env")
            raise
        except psycopg2.Error as e:
            logger.error(f"Error de PostgreSQL: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado al conectar: {str(e)}")
            raise

    def close(self):
        """Cierra conexión"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            logger.info("Conexión cerrada exitosamente")

    def execute_sql_file(self, sql_file_path: str):
        """Ejecuta archivo SQL completo"""
        try:
            with open(sql_file_path, 'r', encoding='utf-8') as file:
                sql_script = file.read()

            self.cursor.execute(sql_script)
            self.connection.commit()
            logger.info(f"Archivo SQL ejecutado exitosamente: {Path(sql_file_path).name}")
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error ejecutando archivo SQL: {str(e)}")
            raise

    def execute_query(self, query: str, params: tuple = None):
        """Ejecuta una query y retorna resultados"""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)

            if query.strip().upper().startswith('SELECT'):
                return self.cursor.fetchall()
            else:
                self.connection.commit()
                return self.cursor.rowcount
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error ejecutando query: {str(e)}")
            raise

    def load_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append'):
        """Carga un DataFrame a una tabla en NeonDB usando SQLAlchemy"""
        try:
            from sqlalchemy import create_engine

            logger.info(f"Cargando {len(df):,} registros a {table_name}...")

            engine = create_engine(self.config.get_sqlalchemy_url())

            df.to_sql(
                name=table_name,
                con=engine,
                if_exists=if_exists,
                index=False,
                method='multi',
                chunksize=1000
            )

            logger.info(f"DataFrame cargado exitosamente a {table_name}: {len(df):,} registros")

            engine.dispose()

        except Exception as e:
            logger.error(f"Error cargando DataFrame a {table_name}: {str(e)}")
            raise

    def table_exists(self, table_name: str) -> bool:
        """Verifica si una tabla existe"""
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            );
        """
        result = self.execute_query(query, (table_name,))
        return result[0][0] if result else False

    def get_table_count(self, table_name: str) -> int:
        """Obtiene el numero de registros de una tabla"""
        query = f"SELECT COUNT(*) FROM {table_name};"
        result = self.execute_query(query)
        return result[0][0] if result else 0

    def truncate_table(self, table_name: str):
        """Vacia una tabla (PELIGROSO - usar con precaución)"""
        query = f"TRUNCATE TABLE {table_name} CASCADE;"
        self.execute_query(query)
        logger.warning(f"Tabla {table_name} vaciada (TRUNCATE CASCADE)")

    def get_all_tables(self) -> List[str]:
        """Obtiene lista de todas las tablas en el esquema public"""
        query = """
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = 'public'
            ORDER BY tablename;
        """
        result = self.execute_query(query)
        return [row[0] for row in result] if result else []

    def validate_data_warehouse_structure(self) -> Dict[str, any]:
        """Valida que la estructura del DW esté completa"""
        expected_tables = {
            'dimensiones': [
                'dim_tiempo', 'dim_ubicacion', 'dim_postulante',
                'dim_carrera', 'dim_institucion', 'dim_vacante',
                'dim_empresa', 'dim_competencia'
            ],
            'hechos': [
                'hechos_postulante', 'hechos_formacion', 'hechos_experiencia',
                'hechos_vacante', 'hechos_competencia_requerida'
            ]
        }

        all_tables = self.get_all_tables()

        validation = {
            'tablas_encontradas': len(all_tables),
            'tablas_esperadas': 13,
            'dimensiones_ok': True,
            'hechos_ok': True,
            'faltantes': [],
            'extra': []
        }

        # Verificar dimensiones
        for dim in expected_tables['dimensiones']:
            if dim not in all_tables:
                validation['dimensiones_ok'] = False
                validation['faltantes'].append(dim)

        # Verificar hechos
        for hecho in expected_tables['hechos']:
            if hecho not in all_tables:
                validation['hechos_ok'] = False
                validation['faltantes'].append(hecho)

        # Verificar tablas extra
        expected_all = expected_tables['dimensiones'] + expected_tables['hechos']
        for tabla in all_tables:
            if tabla not in expected_all and not tabla.startswith('v_'):
                validation['extra'].append(tabla)

        validation['estructura_correcta'] = (
            validation['dimensiones_ok'] and
            validation['hechos_ok'] and
            len(validation['faltantes']) == 0
        )

        return validation

    def get_table_stats(self) -> pd.DataFrame:
        """Obtiene estadísticas de todas las tablas"""
        query = """
            SELECT 
                schemaname,
                tablename,
                CASE 
                    WHEN tablename LIKE 'dim_%' THEN 'Dimension'
                    WHEN tablename LIKE 'hechos_%' THEN 'Hecho'
                    ELSE 'Otro'
                END as tipo,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as tamaño
            FROM pg_tables 
            WHERE schemaname = 'public'
            ORDER BY tipo, tablename;
        """
        result = self.execute_query(query)

        if result:
            df = pd.DataFrame(result, columns=['schema', 'tabla', 'tipo', 'tamaño'])

            # Agregar conteo de registros
            counts = []
            for tabla in df['tabla']:
                try:
                    count = self.get_table_count(tabla)
                    counts.append(count)
                except:
                    counts.append(0)

            df['registros'] = counts
            return df

        return pd.DataFrame()


def get_neon_config() -> NeonDBConfig:
    """Obtiene la configuración de NeonDB desde variables de entorno"""
    return NeonDBConfig.from_env()


def test_connection(config: NeonDBConfig = None) -> bool:
    """Prueba la conexión a NeonDB y valida estructura del DW"""
    if config is None:
        config = get_neon_config()

    try:
        logger.info("Probando conexion a NeonDB...")

        with NeonDBConnection(config) as conn:
            # Verificar version de PostgreSQL
            result = conn.execute_query("SELECT version();")
            logger.info(f"PostgreSQL version: {result[0][0].split(',')[0]}")

            # Validar estructura del DW
            validation = conn.validate_data_warehouse_structure()

            logger.info(f"Tablas encontradas: {validation['tablas_encontradas']}/13")

            if validation['estructura_correcta']:
                logger.info("Estructura del Data Warehouse: OK")
            else:
                if validation['faltantes']:
                    logger.warning(f"Tablas faltantes: {validation['faltantes']}")
                if validation['extra']:
                    logger.warning(f"Tablas extra: {validation['extra']}")

            return True

    except Exception as e:
        logger.error(f"Test de conexion FALLIDO: {str(e)}")
        return False


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("TEST DE CONEXIÓN - DATA WAREHOUSE MTPE")
    logger.info("=" * 60)

    success = test_connection()

    if success:
        logger.info("=" * 60)
        logger.info("CONEXIÓN EXITOSA")
        logger.info("=" * 60)
    else:
        logger.error("=" * 60)
        logger.error("CONEXIÓN FALLIDA - Verificar credenciales en .env")
        logger.error("=" * 60)


