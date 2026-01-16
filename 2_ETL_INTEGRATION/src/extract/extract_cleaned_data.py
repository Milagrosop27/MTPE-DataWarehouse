import pandas as pd
import logging
from pathlib import Path
import sys
from typing import Dict, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class DataExtractor:
    """Extrae y valida datos limpios desde archivos CSV"""

    EXPECTED_FILES = {
        'postulante': 'postulante_clean.csv',
        'discapacidad': 'discapacidad_clean.csv',
        'educacion': 'educacion_clean.csv',
        'experiencias': 'experiencias_clean.csv',
        'vacantes': 'vacantes_clean.csv',
        'competencias': 'competencias_clean.csv'
    }

    def __init__(self, cleaned_path: Optional[Path] = None):
        if cleaned_path:
            self.cleaned_path = cleaned_path
        else:
            self.base_path = Path(__file__).parent.parent.parent.parent
            self.cleaned_path = self.base_path / 'data' / 'cleaned'

        self.datasets: Dict[str, pd.DataFrame] = {}
        self._validate_paths()

    def _validate_paths(self):
        """Valida que existan las rutas necesarias"""
        if not self.cleaned_path.exists():
            raise FileNotFoundError(f"Ruta de datos limpios no encontrada: {self.cleaned_path}")

    def extract_all(self) -> Optional[Dict[str, pd.DataFrame]]:
        """Extrae todos los datasets limpios"""
        logger.info("Iniciando extracción de datos limpios")

        try:
            for name, filename in self.EXPECTED_FILES.items():
                self.datasets[name] = self._load_csv(filename, name)

            self._validate_extraction()
            self._log_summary()

            logger.info("Extraccion completada exitosamente")
            return self.datasets

        except Exception as e:
            logger.error(f"Error durante la extraccion: {str(e)}", exc_info=True)
            return None

    def _load_csv(self, filename: str, dataset_name: str) -> pd.DataFrame:
        """Carga un archivo CSV individual con validaciones"""
        filepath = self.cleaned_path / filename

        if not filepath.exists():
            raise FileNotFoundError(f"Archivo no encontrado: {filepath}")

        df = pd.read_csv(filepath, encoding='utf-8-sig')

        if df.empty:
            raise ValueError(f"Dataset {dataset_name} esta vacio")

        logger.info(f"{dataset_name}: {len(df):,} registros, {len(df.columns)} columnas")

        return df

    def _validate_extraction(self):
        """Valida integridad de los datos extraídos"""
        for name in self.EXPECTED_FILES.keys():
            if name not in self.datasets:
                raise ValueError(f"Dataset {name} no fue cargado")

            df = self.datasets[name]
            if df.empty:
                raise ValueError(f"Dataset {name} esta vacio")

    def _log_summary(self):
        """Genera resumen de extracción"""
        total_records = sum(len(df) for df in self.datasets.values())
        total_memory = sum(df.memory_usage(deep=True).sum() for df in self.datasets.values()) / (1024 ** 2)

        logger.info(f"Resumen: {len(self.datasets)} datasets, {total_records:,} registros totales, {total_memory:.2f} MB en memoria")

    def get_dataset(self, name: str) -> Optional[pd.DataFrame]:
        """Obtiene un dataset especifico"""
        return self.datasets.get(name)

    def get_summary(self) -> Dict:
        """Retorna resumen detallado de los datasets"""
        return {
            name: {
                'records': len(df),
                'columns': len(df.columns),
                'memory_mb': df.memory_usage(deep=True).sum() / (1024 ** 2),
                'columns_list': df.columns.tolist()
            }
            for name, df in self.datasets.items()
        }


def main():
    """Función principal"""
    try:
        extractor = DataExtractor()
        datasets = extractor.extract_all()

        if datasets:
            logger.info("Proceso de extraccion finalizado correctamente")
            return 0, datasets
        else:
            logger.error("Fallo en la extraccion de datos")
            return 1, None

    except Exception as e:
        logger.error(f"Error critico: {str(e)}", exc_info=True)
        return 1, None


if __name__ == "__main__":
    exit_code, _ = main()
    sys.exit(exit_code)

