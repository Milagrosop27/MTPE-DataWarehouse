import pandas as pd
import numpy as np
from pathlib import Path
import logging
import sys
from typing import Dict, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class DataTransformer:
    """Transforma datos limpios al modelo constelación (2 estrellas)"""

    def __init__(self, datasets: Dict[str, pd.DataFrame]):
        self.datasets = datasets
        self.dimensions = {}
        self.facts = {}
        self.orphan_stats = {}

    def transform_all(self) -> Tuple[Optional[Dict], Optional[Dict]]:
        """Ejecuta transformación"""
        logger.info("Iniciando proceso de transformación")

        try:
            logger.info("Fase 1: Construcción de dimensiones")
            self._build_all_dimensions()

            logger.info("Fase 2: Reconciliación de registros huérfanos")
            self._reconcile_orphans()

            logger.info("Fase 3: Construcción de tablas de hechos")
            self._build_all_facts()

            self._log_summary()
            logger.info("Transformación completada exitosamente")

            return self.dimensions, self.facts

        except Exception as e:
            logger.error(f"Error durante la transformacion: {str(e)}", exc_info=True)
            return None, None

    def _build_all_dimensions(self):
        """Construye dimensiones"""
        self._build_dim_tiempo()
        self._build_dim_ubicacion()
        self._build_dim_postulante()
        self._build_dim_empresa()
        self._build_dim_vacante()
        self._build_dim_competencia()
        self._build_dim_carrera()
        self._build_dim_institucion()

    def _build_all_facts(self):
        """Construye hechos"""
        self._build_hechos_postulante()
        self._build_hechos_formacion()
        self._build_hechos_experiencia()
        self._build_hechos_vacante()
        self._build_hechos_competencia_requerida()

    def _reconcile_orphans(self):
        if 'competencias' not in self.datasets or 'vacantes' not in self.datasets:
            logger.warning("Datasets insuficientes para reconciliación de huérfanos")
            return

        df_comp = self.datasets['competencias'].copy()
        df_vac = self.datasets['vacantes'].copy()

        avisos_comp = set(df_comp['AVISOID'].dropna().unique())
        avisos_vac = set(df_vac['AVISOID'].dropna().unique())

        huerfanos = avisos_comp - avisos_vac
        huerfanos_count = len(huerfanos)
        total_comp = len(avisos_comp)
        pct_huerfanos = (huerfanos_count / total_comp * 100) if total_comp > 0 else 0

        if huerfanos_count > 0:
            logger.warning(f"Detectados {huerfanos_count} AVISOIDs huerfanos en competencias ({pct_huerfanos:.2f}%)")

            import datetime
            fecha_placeholder = datetime.datetime.now().date()

            vacante_placeholder = pd.DataFrame({
                'AVISOID': list(huerfanos),
                'NOMBREAVISO': 'REGISTRO_HUERFANO_PRESERVADO',
                'VACANTES': 0,
                'SECTOR': 'SIN_CLASIFICAR',
                'UBIGEO': '000000',
                'SINEXPERIENCIA': 'NO',
                'TIEMPOEXPERIENCIA': 0,
                'IDEMPRESA': 0,
                'DEPARTAMENTO': 'SIN_ESPECIFICAR',
                'PROVINCIA': 'SIN_ESPECIFICAR',
                'DISTRITO': 'SIN_ESPECIFICAR',
                'FECHAINICIO': fecha_placeholder,
                'FECHAFIN': fecha_placeholder,
                'FECHACREACION': fecha_placeholder,
                'ACTIVO': False
            })

            self.datasets['vacantes'] = pd.concat([df_vac, vacante_placeholder], ignore_index=True)
            logger.info(f"Creados {huerfanos_count} registros placeholder marcados como 'HUERFANO' para preservar datos reales")

        self.orphan_stats['competencias_huerfanos'] = huerfanos_count
        self.orphan_stats['competencias_total'] = total_comp
        self.orphan_stats['competencias_pct'] = pct_huerfanos


    def _build_dim_tiempo(self):
        """Construye dimension tiempo desde fechas de vacantes y educación"""
        fechas = []

        if 'vacantes' in self.datasets:
            df_vac = self.datasets['vacantes']
            for col in ['FECHAINICIO', 'FECHAFIN', 'FECHACREACION']:
                if col in df_vac.columns:
                    fechas.extend(pd.to_datetime(df_vac[col], errors='coerce').dropna().tolist())

        if 'educacion' in self.datasets:
            df_edu = self.datasets['educacion']
            for col in ['FECHAINICIO', 'FECHAFIN']:
                if col in df_edu.columns:
                    fechas.extend(pd.to_datetime(df_edu[col], errors='coerce').dropna().tolist())

        fechas_normalizadas = [pd.Timestamp(f.date()) for f in fechas]
        fechas_unicas = sorted(set(fechas_normalizadas))

        dim_tiempo = pd.DataFrame({
            'fecha_sk': range(1, len(fechas_unicas) + 1),
            'fecha': fechas_unicas
        })

        dim_tiempo['anio'] = dim_tiempo['fecha'].dt.year
        dim_tiempo['mes'] = dim_tiempo['fecha'].dt.month
        dim_tiempo['dia'] = dim_tiempo['fecha'].dt.day
        dim_tiempo['trimestre'] = dim_tiempo['fecha'].dt.quarter
        dim_tiempo['semestre'] = np.where(dim_tiempo['trimestre'] <= 2, 1, 2)
        dim_tiempo['dia_semana'] = dim_tiempo['fecha'].dt.dayofweek + 1

        meses_es = {
            1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril',
            5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto',
            9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
        }
        dias_es = {
            1: 'Lunes', 2: 'Martes', 3: 'Miércoles', 4: 'Jueves',
            5: 'Viernes', 6: 'Sábado', 7: 'Domingo'
        }

        dim_tiempo['nombre_mes'] = dim_tiempo['mes'].map(meses_es)
        dim_tiempo['nombre_dia'] = dim_tiempo['dia_semana'].map(dias_es)
        dim_tiempo['es_fin_semana'] = dim_tiempo['dia_semana'].isin([6, 7])

        self.dimensions['dim_tiempo'] = dim_tiempo
        logger.info(f"dim_tiempo: {len(dim_tiempo):,} registros, rango {dim_tiempo['fecha'].min()} a {dim_tiempo['fecha'].max()}")


    def _build_dim_ubicacion(self):
        """Construye dimension ubicación consolidada preservando todos los registros"""
        ubicaciones = []

        if 'postulante' in self.datasets:
            df = self.datasets['postulante']
            if all(col in df.columns for col in ['DEPARTAMENTO', 'PROVINCIA', 'DISTRITO', 'UBIGEO']):
                ub = df[['DEPARTAMENTO', 'PROVINCIA', 'DISTRITO', 'UBIGEO']].copy()
                ub['fuente'] = 'postulante'
                ubicaciones.append(ub)

        if 'vacantes' in self.datasets:
            df = self.datasets['vacantes']
            if all(col in df.columns for col in ['DEPARTAMENTO', 'PROVINCIA', 'DISTRITO', 'UBIGEO']):
                ub = df[['DEPARTAMENTO', 'PROVINCIA', 'DISTRITO', 'UBIGEO']].copy()
                ub['fuente'] = 'vacante'
                ubicaciones.append(ub)

        dim_ubicacion = pd.concat(ubicaciones, ignore_index=True)
        dim_ubicacion = dim_ubicacion.drop_duplicates(subset=['UBIGEO']).reset_index(drop=True)

        dim_ubicacion['DISTRITO'] = dim_ubicacion['DISTRITO'].fillna('SIN_ESPECIFICAR').infer_objects(copy=False)
        dim_ubicacion['DEPARTAMENTO'] = dim_ubicacion['DEPARTAMENTO'].fillna('SIN_ESPECIFICAR').infer_objects(copy=False)
        dim_ubicacion['PROVINCIA'] = dim_ubicacion['PROVINCIA'].fillna('SIN_ESPECIFICAR').infer_objects(copy=False)

        dim_ubicacion.insert(0, 'ubicacion_sk', range(1, len(dim_ubicacion) + 1))
        dim_ubicacion.columns = ['ubicacion_sk', 'departamento', 'provincia', 'distrito', 'ubigeo', 'fuente']

        self.dimensions['dim_ubicacion'] = dim_ubicacion
        logger.info(f"dim_ubicacion: {len(dim_ubicacion):,} registros (valores NULL reemplazados por marcadores)")

    def _build_dim_postulante(self):
        """Construye dimension postulante con surrogate keys"""
        if 'postulante' not in self.datasets:
            logger.warning("Dataset postulante no disponible")
            return

        df_post = self.datasets['postulante'].copy()
        cols_needed = ['ID_POSTULANTE', 'EDAD', 'SEXO', 'UBIGEO', 'ESTADO_CONADIS']
        cols_available = [col for col in cols_needed if col in df_post.columns]

        dim_postulante = df_post[cols_available].drop_duplicates(subset=['ID_POSTULANTE']).reset_index(drop=True)
        dim_postulante.insert(0, 'postulante_sk', range(1, len(dim_postulante) + 1))

        dim_postulante.rename(columns={
            'ID_POSTULANTE': 'id_postulante_original',
            'EDAD': 'edad',
            'SEXO': 'sexo',
            'UBIGEO': 'ubigeo',
            'ESTADO_CONADIS': 'estado_conadis'
        }, inplace=True)

        self.dimensions['dim_postulante'] = dim_postulante
        logger.info(f"dim_postulante: {len(dim_postulante):,} registros")

    def _build_dim_empresa(self):
        """Construye dimension empresa con surrogate keys"""
        if 'vacantes' not in self.datasets:
            logger.warning("Dataset vacantes no disponible")
            return

        df = self.datasets['vacantes']
        if 'IDEMPRESA' not in df.columns:
            logger.warning("Columna IDEMPRESA no encontrada")
            return

        dim_empresa = df[['IDEMPRESA']].drop_duplicates().reset_index(drop=True)
        dim_empresa.insert(0, 'empresa_sk', range(1, len(dim_empresa) + 1))
        dim_empresa.rename(columns={'IDEMPRESA': 'id_empresa_original'}, inplace=True)

        self.dimensions['dim_empresa'] = dim_empresa
        logger.info(f"dim_empresa: {len(dim_empresa):,} registros")

    def _build_dim_vacante(self):
        """Construye dimension vacante preservando todos los registros con marcadores profesionales"""
        if 'vacantes' not in self.datasets:
            logger.warning("Dataset vacantes no disponible")
            return

        df_vac = self.datasets['vacantes'].copy()
        dim_vacante = df_vac[['AVISOID']].drop_duplicates().reset_index(drop=True)
        dim_vacante.columns = ['id_vacante_original']
        dim_vacante.insert(0, 'vacante_sk', range(1, len(dim_vacante) + 1))

        dim_vacante = dim_vacante.merge(df_vac, left_on='id_vacante_original', right_on='AVISOID', how='left')

        cols_to_keep = ['vacante_sk', 'id_vacante_original']
        optional_cols = ['NOMBREAVISO', 'VACANTES', 'SECTOR', 'UBIGEO', 'SINEXPERIENCIA', 'TIEMPOEXPERIENCIA']
        cols_to_keep.extend([col for col in optional_cols if col in dim_vacante.columns])

        dim_vacante = dim_vacante[cols_to_keep]

        if 'TIEMPOEXPERIENCIA' in dim_vacante.columns:
            dim_vacante['TIEMPOEXPERIENCIA'] = pd.to_numeric(dim_vacante['TIEMPOEXPERIENCIA'], errors='coerce')
            dim_vacante['TIEMPOEXPERIENCIA'] = dim_vacante['TIEMPOEXPERIENCIA'].fillna(0).infer_objects(copy=False)

        if 'SINEXPERIENCIA' in dim_vacante.columns:
            dim_vacante['SINEXPERIENCIA'] = dim_vacante['SINEXPERIENCIA'].map({
                'SI': True, 'NO': False, 'S': True, 'N': False,
                1: True, 0: False, '1': True, '0': False
            })
            dim_vacante['SINEXPERIENCIA'] = dim_vacante['SINEXPERIENCIA'].fillna(False).infer_objects(copy=False)

        dim_vacante.rename(columns={
            'NOMBREAVISO': 'nombre_aviso',
            'VACANTES': 'num_vacantes',
            'SECTOR': 'sector',
            'UBIGEO': 'ubigeo',
            'SINEXPERIENCIA': 'sin_experiencia',
            'TIEMPOEXPERIENCIA': 'tiempo_experiencia'
        }, inplace=True)

        self.dimensions['dim_vacante'] = dim_vacante
        logger.info(f"dim_vacante: {len(dim_vacante):,} registros (NULLs reemplazados por valores por defecto)")

    def _build_dim_competencia(self):
        """Construye dimension competencia con surrogate keys"""
        if 'competencias' not in self.datasets:
            logger.warning("Dataset competencias no disponible")
            return

        df = self.datasets['competencias']
        if 'NOMBRECOMPETENCIA' not in df.columns:
            logger.warning("Columna NOMBRECOMPETENCIA no encontrada")
            return

        dim_competencia = df[['NOMBRECOMPETENCIA']].drop_duplicates().reset_index(drop=True)
        dim_competencia.insert(0, 'competencia_sk', range(1, len(dim_competencia) + 1))
        dim_competencia.rename(columns={'NOMBRECOMPETENCIA': 'nombre_competencia'}, inplace=True)

        self.dimensions['dim_competencia'] = dim_competencia
        logger.info(f"dim_competencia: {len(dim_competencia):,} registros")

    def _build_dim_carrera(self):
        """Construye dimension carrera con surrogate keys"""
        if 'educacion' not in self.datasets:
            logger.warning("Dataset educacion no disponible")
            return

        df = self.datasets['educacion']
        if 'CARRERA' not in df.columns:
            logger.warning("Columna CARRERA no encontrada")
            return

        dim_carrera = df[['CARRERA']].drop_duplicates().dropna().reset_index(drop=True)
        dim_carrera.insert(0, 'carrera_sk', range(1, len(dim_carrera) + 1))
        dim_carrera.rename(columns={'CARRERA': 'nombre_carrera'}, inplace=True)

        if 'GRADO' in df.columns:
            grado_por_carrera = df.groupby('CARRERA')['GRADO'].first().reset_index()
            dim_carrera = dim_carrera.merge(grado_por_carrera, left_on='nombre_carrera', right_on='CARRERA', how='left')
            dim_carrera.drop('CARRERA', axis=1, inplace=True)

        self.dimensions['dim_carrera'] = dim_carrera
        logger.info(f"dim_carrera: {len(dim_carrera):,} registros unicos (normalizados en fase de limpieza)")

    def _build_dim_institucion(self):
        """Construye dimension institución con surrogate keys"""
        if 'educacion' not in self.datasets:
            logger.warning("Dataset educacion no disponible")
            return

        df = self.datasets['educacion']
        if 'INSTITUCION' not in df.columns:
            logger.warning("Columna INSTITUCION no encontrada")
            return

        dim_institucion = df[['INSTITUCION']].drop_duplicates().dropna().reset_index(drop=True)
        dim_institucion.insert(0, 'institucion_sk', range(1, len(dim_institucion) + 1))
        dim_institucion.rename(columns={'INSTITUCION': 'nombre_institucion'}, inplace=True)

        self.dimensions['dim_institucion'] = dim_institucion
        logger.info(f"dim_institucion: {len(dim_institucion):,} registros unicos (normalizados en fase de limpieza)")


    def _build_hechos_postulante(self):
        """Construye tabla de hechos postulante con FKs"""
        if 'postulante' not in self.datasets:
            logger.warning("Dataset postulante no disponible")
            return

        df_post = self.datasets['postulante'].copy()
        dim_post = self.dimensions['dim_postulante']

        hechos = df_post.merge(
            dim_post[['postulante_sk', 'id_postulante_original']],
            left_on='ID_POSTULANTE',
            right_on='id_postulante_original',
            how='left'
        )

        if 'dim_ubicacion' in self.dimensions and 'UBIGEO' in hechos.columns:
            dim_ub = self.dimensions['dim_ubicacion']
            hechos = hechos.merge(dim_ub[['ubicacion_sk', 'ubigeo']], left_on='UBIGEO', right_on='ubigeo', how='left')

        cols_final = ['postulante_sk']
        if 'ubicacion_sk' in hechos.columns:
            cols_final.append('ubicacion_sk')

        if 'dim_tiempo' in self.dimensions:
            primera_fecha_sk = self.dimensions['dim_tiempo']['fecha_sk'].min()
            hechos['fecha_registro_sk'] = primera_fecha_sk
            cols_final.append('fecha_registro_sk')

        hechos = hechos[[col for col in cols_final if col in hechos.columns]].drop_duplicates()
        self.facts['hechos_postulante'] = hechos
        logger.info(f"hechos_postulante: {len(hechos):,} registros")


    def _build_hechos_formacion(self):
        """Construye tabla de hechos formación con FKs"""
        if 'educacion' not in self.datasets:
            logger.warning("Dataset educacion no disponible")
            return

        df_edu = self.datasets['educacion'].copy()
        dim_post = self.dimensions['dim_postulante']

        hechos = df_edu.merge(
            dim_post[['postulante_sk', 'id_postulante_original']],
            left_on='ID_POSTULANTE',
            right_on='id_postulante_original',
            how='inner'
        )

        if 'dim_carrera' in self.dimensions and 'CARRERA' in hechos.columns:
            dim_carrera = self.dimensions['dim_carrera']
            hechos = hechos.merge(
                dim_carrera[['carrera_sk', 'nombre_carrera']],
                left_on='CARRERA',
                right_on='nombre_carrera',
                how='left'
            )

        if 'dim_institucion' in self.dimensions and 'INSTITUCION' in hechos.columns:
            dim_inst = self.dimensions['dim_institucion']
            hechos = hechos.merge(
                dim_inst[['institucion_sk', 'nombre_institucion']],
                left_on='INSTITUCION',
                right_on='nombre_institucion',
                how='left'
            )

        cols_final = ['postulante_sk']
        if 'carrera_sk' in hechos.columns:
            cols_final.append('carrera_sk')
        if 'institucion_sk' in hechos.columns:
            cols_final.append('institucion_sk')

        hechos = hechos[[col for col in cols_final if col in hechos.columns]].drop_duplicates()
        self.facts['hechos_formacion'] = hechos
        logger.info(f"hechos_formacion: {len(hechos):,} registros")

    def _build_hechos_experiencia(self):
        """Construye tabla de hechos experiencia con FKs"""
        if 'experiencias' not in self.datasets:
            logger.warning("Dataset experiencias no disponible")
            return

        df_exp = self.datasets['experiencias'].copy()
        dim_post = self.dimensions['dim_postulante']

        hechos = df_exp.merge(
            dim_post[['postulante_sk', 'id_postulante_original']],
            left_on='ID_POSTULANTE',
            right_on='id_postulante_original',
            how='inner'
        )

        hechos = hechos[['postulante_sk']].drop_duplicates()
        hechos = hechos.dropna(subset=['postulante_sk'])
        hechos['postulante_sk'] = hechos['postulante_sk'].astype('int64')

        self.facts['hechos_experiencia'] = hechos
        logger.info(f"hechos_experiencia: {len(hechos):,} registros")

    def _build_hechos_vacante(self):
        """Construye tabla de hechos vacante con FKs"""
        if 'vacantes' not in self.datasets:
            logger.warning("Dataset vacantes no disponible")
            return

        df_vac = self.datasets['vacantes'].copy()
        dim_vac = self.dimensions['dim_vacante']

        hechos = df_vac.merge(
            dim_vac[['vacante_sk', 'id_vacante_original']],
            left_on='AVISOID',
            right_on='id_vacante_original',
            how='inner'
        )

        if 'dim_ubicacion' in self.dimensions and 'UBIGEO' in hechos.columns:
            dim_ub = self.dimensions['dim_ubicacion']
            hechos = hechos.merge(dim_ub[['ubicacion_sk', 'ubigeo']], left_on='UBIGEO', right_on='ubigeo', how='left')

        if 'dim_empresa' in self.dimensions and 'IDEMPRESA' in hechos.columns:
            dim_emp = self.dimensions['dim_empresa']
            hechos = hechos.merge(
                dim_emp[['empresa_sk', 'id_empresa_original']],
                left_on='IDEMPRESA',
                right_on='id_empresa_original',
                how='left'
            )

        cols_final = ['vacante_sk']
        if 'ubicacion_sk' in hechos.columns:
            cols_final.append('ubicacion_sk')
        if 'empresa_sk' in hechos.columns:
            cols_final.append('empresa_sk')

        if 'dim_tiempo' in self.dimensions and 'FECHACREACION' in hechos.columns:
            dim_tiempo = self.dimensions['dim_tiempo']
            hechos['FECHACREACION'] = pd.to_datetime(hechos['FECHACREACION'], errors='coerce')
            hechos['fecha_normalizada'] = hechos['FECHACREACION'].dt.normalize()

            hechos = hechos.merge(dim_tiempo[['fecha_sk', 'fecha']], left_on='fecha_normalizada', right_on='fecha', how='left')

            primera_fecha_sk = dim_tiempo['fecha_sk'].min()
            hechos['fecha_publicacion_sk'] = hechos['fecha_sk'].fillna(primera_fecha_sk).infer_objects(copy=False).astype('int64')
            cols_final.append('fecha_publicacion_sk')

        if 'ACTIVO' in hechos.columns:
            cols_final.append('ACTIVO')
            hechos.rename(columns={'ACTIVO': 'activo'}, inplace=True)
        else:
            hechos['activo'] = True
            cols_final.append('activo')

        hechos = hechos[[col for col in cols_final if col in hechos.columns]].drop_duplicates()

        hechos = hechos.dropna()

        if 'vacante_sk' in hechos.columns:
            hechos['vacante_sk'] = hechos['vacante_sk'].astype('int64')
        if 'ubicacion_sk' in hechos.columns:
            hechos['ubicacion_sk'] = hechos['ubicacion_sk'].astype('int64')
        if 'empresa_sk' in hechos.columns:
            hechos['empresa_sk'] = hechos['empresa_sk'].astype('int64')

        self.facts['hechos_vacante'] = hechos
        logger.info(f"hechos_vacante: {len(hechos):,} registros")

    def _build_hechos_competencia_requerida(self):
        """Construye tabla de hechos competencia requerida con FKs"""
        if 'competencias' not in self.datasets:
            logger.warning("Dataset competencias no disponible")
            return

        df_comp = self.datasets['competencias'].copy()
        dim_vac = self.dimensions['dim_vacante']

        hechos = df_comp.merge(
            dim_vac[['vacante_sk', 'id_vacante_original']],
            left_on='AVISOID',
            right_on='id_vacante_original',
            how='inner'
        )

        if 'dim_competencia' in self.dimensions and 'NOMBRECOMPETENCIA' in hechos.columns:
            dim_comp = self.dimensions['dim_competencia']
            hechos = hechos.merge(dim_comp, left_on='NOMBRECOMPETENCIA', right_on='nombre_competencia', how='inner')

        hechos = hechos[['vacante_sk', 'competencia_sk']].dropna()
        hechos['vacante_sk'] = hechos['vacante_sk'].astype('int64')
        hechos['competencia_sk'] = hechos['competencia_sk'].astype('int64')

        self.facts['hechos_competencia_requerida'] = hechos
        logger.info(f"hechos_competencia_requerida: {len(hechos):,} registros")

    def _log_summary(self):
        """Registra resumen de transformacion"""
        total_dim_records = sum(len(df) for df in self.dimensions.values())
        total_fact_records = sum(len(df) for df in self.facts.values())

        logger.info(f"Resumen transformacion: {len(self.dimensions)} dimensiones ({total_dim_records:,} registros), "
                   f"{len(self.facts)} hechos ({total_fact_records:,} registros)")

        if self.orphan_stats:
            logger.info(f"Estadisticas de huerfanos: {self.orphan_stats}")


def main():
    """Funcion principal"""
    src_path = Path(__file__).parent.parent
    sys.path.insert(0, str(src_path))

    from extract.extract_cleaned_data import DataExtractor

    extractor = DataExtractor()
    datasets = extractor.extract_all()

    if datasets is None:
        logger.error("No se pudieron extraer los datos")
        return 1, None, None

    transformer = DataTransformer(datasets)
    dimensions, facts = transformer.transform_all()

    if dimensions is None or facts is None:
        logger.error("No se pudieron transformar los datos")
        return 1, None, None

    output_dir = Path(__file__).parent.parent.parent.parent / 'data' / 'integrated'
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Guardando archivos en {output_dir}")

    for nombre, df in dimensions.items():
        filepath = output_dir / f"{nombre}.csv"
        df.to_csv(filepath, index=False, encoding='utf-8')
        logger.info(f"{nombre}.csv guardado ({len(df):,} registros)")

    for nombre, df in facts.items():
        filepath = output_dir / f"{nombre}.csv"
        df.to_csv(filepath, index=False, encoding='utf-8')
        logger.info(f"{nombre}.csv guardado ({len(df):,} registros)")

    logger.info(f"Total: {len(dimensions) + len(facts)} archivos guardados exitosamente")

    return 0, dimensions, facts


if __name__ == "__main__":
    exit_code, _, _ = main()
    sys.exit(exit_code)

