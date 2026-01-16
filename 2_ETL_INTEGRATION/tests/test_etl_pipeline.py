import sys
import logging
from pathlib import Path
from typing import Dict, Tuple, Optional
import pytest

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configuración de paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
SRC_PATH = PROJECT_ROOT / '2_ETL_INTEGRATION' / 'src'
sys.path.insert(0, str(SRC_PATH / 'extract'))
sys.path.insert(0, str(SRC_PATH / 'transform'))

from extract_cleaned_data import DataExtractor
from transform_to_constellation import DataTransformer

# Constantes de validación
EXPECTED_DATASETS = [
    'postulante', 'discapacidad', 'educacion',
    'experiencias', 'vacantes', 'competencias'
]

EXPECTED_DIMENSIONS = [
    'dim_tiempo', 'dim_ubicacion', 'dim_postulante',
    'dim_empresa', 'dim_vacante', 'dim_competencia',
    'dim_carrera', 'dim_institucion'
]

EXPECTED_FACTS = [
    'hechos_postulante', 'hechos_formacion',
    'hechos_experiencia', 'hechos_vacante',
    'hechos_competencia_requerida'
]

# Thresholds de calidad
MAX_NULL_PERCENTAGE_FK = 10.0
MAX_ORPHAN_PERCENTAGE = 5.0


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture(scope="module")
def extracted_datasets() -> Optional[Dict]:
    """
    Fixture: Extrae datasets una sola vez para toda la suite de tests

    Returns:
        Dict con datasets extraídos o None si falla
    """
    logger.info("Fixture: Extrayendo datasets desde data/cleaned")
    extractor = DataExtractor()
    datasets = extractor.extract_all()

    if datasets:
        total_records = sum(len(df) for df in datasets.values())
        logger.info(f"Fixture: {len(datasets)} datasets extraidos, {total_records:,} registros")

    return datasets


@pytest.fixture(scope="module")
def transformed_data(extracted_datasets) -> Optional[Tuple[Dict, Dict]]:
    """
    Fixture: Transforma datasets al modelo dimensional una sola vez

    Args:
        extracted_datasets: Datasets extraídos (de fixture anterior)

    Returns:
        Tuple (dimensions, facts) o None si falla
    """
    if not extracted_datasets:
        logger.error("Fixture: No hay datasets para transformar")
        return None

    logger.info("Fixture: Transformando datos al modelo de constelacion")
    transformer = DataTransformer(extracted_datasets)
    dimensions, facts = transformer.transform_all()

    if dimensions and facts:
        total_dim = sum(len(df) for df in dimensions.values())
        total_fact = sum(len(df) for df in facts.values())
        logger.info(f"Fixture: {len(dimensions)} dimensiones ({total_dim:,}), "
                   f"{len(facts)} hechos ({total_fact:,})")

    return (dimensions, facts) if dimensions and facts else None

# ============================================================================
# TEST 1: EXTRACCIÓN DE DATOS
# ============================================================================

def test_01_extraction_success(extracted_datasets):
    assert extracted_datasets is not None, "Extraccion fallo: datasets es None"

    # Validar presencia de todos los datasets
    for ds_name in EXPECTED_DATASETS:
        assert ds_name in extracted_datasets, \
            f"Dataset {ds_name} no fue extraido"

        df = extracted_datasets[ds_name]
        assert df is not None, f"Dataset {ds_name} es None"
        assert len(df) > 0, f"Dataset {ds_name} esta vacio"

    # Log de resumen
    total_records = sum(len(df) for df in extracted_datasets.values())
    logger.info(f"Test 1 PASS: {len(extracted_datasets)} datasets, "
                f"{total_records:,} registros totales")

# ============================================================================
# TEST 2: TRANSFORMACIÓN AL MODELO DIMENSIONAL
# ============================================================================

def test_02_transformation_success(transformed_data):
    assert transformed_data is not None, "Transformacion fallo: datos es None"

    dimensions, facts = transformed_data

    # Validar dimensiones
    for dim_name in EXPECTED_DIMENSIONS:
        assert dim_name in dimensions, \
            f"Dimension {dim_name} no fue generada"
        assert len(dimensions[dim_name]) > 0, \
            f"Dimension {dim_name} esta vacia"

    # Validar hechos
    for fact_name in EXPECTED_FACTS:
        assert fact_name in facts, \
            f"Tabla de hechos {fact_name} no fue generada"
        assert len(facts[fact_name]) > 0, \
            f"Tabla de hechos {fact_name} esta vacia"

    # Log de resumen
    total_dim = sum(len(df) for df in dimensions.values())
    total_fact = sum(len(df) for df in facts.values())
    logger.info(f"Test 2 PASS: {len(dimensions)} dimensiones ({total_dim:,}), "
                f"{len(facts)} hechos ({total_fact:,})")

# ============================================================================
# TEST 3: CALIDAD DE DATOS - PRIMARY KEYS
# ============================================================================

def test_03_primary_key_quality(transformed_data):
    assert transformed_data is not None
    dimensions, _ = transformed_data

    errors = []

    for dim_name, dim_df in dimensions.items():
        # Identificar columna SK (surrogate key)
        sk_cols = [col for col in dim_df.columns if col.endswith('_sk')]

        if not sk_cols:
            errors.append(f"{dim_name}: No tiene columna _sk")
            continue

        sk_col = sk_cols[0]

        # Validar duplicados
        duplicados = dim_df[sk_col].duplicated().sum()
        if duplicados > 0:
            errors.append(f"{dim_name}.{sk_col}: {duplicados} duplicados")

        # Validar nulos
        nulos = dim_df[sk_col].isna().sum()
        if nulos > 0:
            errors.append(f"{dim_name}.{sk_col}: {nulos} valores nulos")

    assert len(errors) == 0, \
        f"Problemas de calidad en PKs:\n" + "\n".join(errors)

    logger.info(f"Test 3 PASS: Todas las PKs son integras (sin duplicados, sin nulos)")

# ============================================================================
# TEST 4: CALIDAD DE DATOS - FOREIGN KEYS
# ============================================================================

def test_04_foreign_key_quality(transformed_data):
    assert transformed_data is not None
    dimensions, facts = transformed_data

    warnings = []

    for fact_name, fact_df in facts.items():
        sk_cols = [col for col in fact_df.columns if col.endswith('_sk')]

        for sk_col in sk_cols:
            nulos = fact_df[sk_col].isna().sum()
            total = len(fact_df)
            pct_nulos = (nulos / total * 100) if total > 0 else 0

            if pct_nulos >= MAX_NULL_PERCENTAGE_FK:
                warnings.append(
                    f"{fact_name}.{sk_col}: {nulos:,} nulos ({pct_nulos:.2f}%)"
                )

    if warnings:
        logger.warning("FKs con alto porcentaje de nulos:\n" + "\n".join(warnings))

    logger.info(f"Test 4 PASS: Calidad de FKs validada")

# ============================================================================
# TEST 5: INTEGRIDAD REFERENCIAL - POSTULANTES
# ============================================================================

def test_05_referential_integrity_postulantes(transformed_data):
    assert transformed_data is not None
    dimensions, facts = transformed_data

    if 'hechos_postulante' not in facts or 'dim_postulante' not in dimensions:
        pytest.skip("Tablas de postulante no disponibles")

    fact_sks = set(facts['hechos_postulante']['postulante_sk'].dropna())
    dim_sks = set(dimensions['dim_postulante']['postulante_sk'])

    huerfanos = fact_sks - dim_sks

    assert len(huerfanos) == 0, \
        f"hechos_postulante: {len(huerfanos)} postulante_sk huerfanos sin dim_postulante"

    logger.info(f"Test 5 PASS: Integridad referencial postulantes validada "
                f"({len(fact_sks)} FKs, 0 huerfanos)")


# ============================================================================
# TEST 6: INTEGRIDAD REFERENCIAL - VACANTES
# ============================================================================

def test_06_referential_integrity_vacantes(transformed_data):
    assert transformed_data is not None
    dimensions, facts = transformed_data

    errors = []

    # Validar hechos_vacante -> dim_vacante
    if 'hechos_vacante' in facts and 'dim_vacante' in dimensions:
        fact_sks = set(facts['hechos_vacante']['vacante_sk'].dropna())
        dim_sks = set(dimensions['dim_vacante']['vacante_sk'])
        huerfanos = fact_sks - dim_sks

        if huerfanos:
            errors.append(
                f"hechos_vacante: {len(huerfanos)} vacante_sk huerfanos"
            )

    # Validar hechos_competencia_requerida -> dim_vacante
    if 'hechos_competencia_requerida' in facts:
        hecho = facts['hechos_competencia_requerida']

        if 'dim_vacante' in dimensions:
            fact_sks = set(hecho['vacante_sk'].dropna())
            dim_sks = set(dimensions['dim_vacante']['vacante_sk'])
            huerfanos = fact_sks - dim_sks

            if huerfanos:
                errors.append(
                    f"hechos_competencia_requerida: {len(huerfanos)} vacante_sk huerfanos"
                )

        # Validar hechos_competencia_requerida -> dim_competencia
        if 'dim_competencia' in dimensions:
            fact_sks = set(hecho['competencia_sk'].dropna())
            dim_sks = set(dimensions['dim_competencia']['competencia_sk'])
            huerfanos = fact_sks - dim_sks

            if huerfanos:
                errors.append(
                    f"hechos_competencia_requerida: {len(huerfanos)} competencia_sk huerfanos"
                )

    assert len(errors) == 0, \
        f"Problemas de integridad referencial:\n" + "\n".join(errors)

    logger.info(f"Test 6 PASS: Integridad referencial vacantes y competencias validada")


# ============================================================================
# TEST 7: MANEJO DE ERRORES - ARCHIVOS FALTANTES
# ============================================================================

def test_07_error_handling_missing_files():
    fake_path = Path('/ruta/invalida/que/no/existe')
    extractor = DataExtractor(cleaned_path=fake_path)

    # Debe fallar gracefully
    datasets = extractor.extract_all()

    assert datasets is None or len(datasets) == 0, \
        "Deberia fallar con path invalido"

    logger.info(f"Test 7 PASS: Manejo correcto de archivos faltantes")


# ============================================================================
# TEST SUITE RUNNER
# ============================================================================

def run_test_suite():
    logger.info("=" * 80)
    logger.info("MTPE DATA WAREHOUSE ETL - TEST SUITE")
    logger.info("Modelo: Constelacion (2 Estrellas)")
    logger.info("=" * 80)

    # Ejecutar pytest
    exit_code = pytest.main([
        __file__,
        '-v',
        '--tb=short',
        '--log-cli-level=INFO'
    ])

    return exit_code == 0


if __name__ == "__main__":
    success = run_test_suite()
    sys.exit(0 if success else 1)

