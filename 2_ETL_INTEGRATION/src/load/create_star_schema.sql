DROP TABLE IF EXISTS hechos_competencia_requerida CASCADE;
DROP TABLE IF EXISTS hechos_vacante CASCADE;
DROP TABLE IF EXISTS hechos_experiencia CASCADE;
DROP TABLE IF EXISTS hechos_formacion CASCADE;
DROP TABLE IF EXISTS hechos_postulante CASCADE;
DROP TABLE IF EXISTS dim_competencia CASCADE;
DROP TABLE IF EXISTS dim_vacante CASCADE;
DROP TABLE IF EXISTS dim_empresa CASCADE;
DROP TABLE IF EXISTS dim_institucion CASCADE;
DROP TABLE IF EXISTS dim_carrera CASCADE;
DROP TABLE IF EXISTS dim_postulante CASCADE;
DROP TABLE IF EXISTS dim_ubicacion CASCADE;
DROP TABLE IF EXISTS dim_tiempo CASCADE;

-- =============================================================================
-- DIMENSIONES COMPARTIDAS (2 tablas)
-- Puentes entre ESTRELLA 1 (Oferta) y ESTRELLA 2 (Demanda)
-- =============================================================================

CREATE TABLE dim_tiempo (
    fecha_sk SERIAL PRIMARY KEY,
    fecha DATE NOT NULL UNIQUE,
    anio INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    dia INTEGER NOT NULL,
    trimestre INTEGER NOT NULL,
    semestre INTEGER NOT NULL,
    dia_semana INTEGER NOT NULL,
    nombre_mes VARCHAR(20),
    nombre_dia VARCHAR(20),
    es_fin_semana BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_dim_tiempo_fecha ON dim_tiempo(fecha);
CREATE INDEX idx_dim_tiempo_anio_mes ON dim_tiempo(anio, mes);

COMMENT ON TABLE dim_tiempo IS 'Dimension temporal - Compartida entre ambas estrellas';

CREATE TABLE dim_ubicacion (
    ubicacion_sk SERIAL PRIMARY KEY,
    ubigeo VARCHAR(10) NOT NULL UNIQUE,
    departamento VARCHAR(100) NOT NULL DEFAULT 'SIN_ESPECIFICAR',
    provincia VARCHAR(100) NOT NULL DEFAULT 'SIN_ESPECIFICAR',
    distrito VARCHAR(100) NOT NULL DEFAULT 'SIN_ESPECIFICAR',
    fuente VARCHAR(20)
);

CREATE INDEX idx_dim_ubicacion_ubigeo ON dim_ubicacion(ubigeo);
CREATE INDEX idx_dim_ubicacion_departamento ON dim_ubicacion(departamento);
CREATE INDEX idx_dim_ubicacion_provincia ON dim_ubicacion(provincia);

COMMENT ON TABLE dim_ubicacion IS 'Geografia Peru - Dimension COMPARTIDA entre ambas estrellas';
COMMENT ON COLUMN dim_ubicacion.fuente IS 'Origen: postulante o vacante';

-- ESTRELLA 1: OFERTA DE TALENTO - Dimensiones

CREATE TABLE dim_postulante (
    postulante_sk SERIAL PRIMARY KEY,
    id_postulante_original VARCHAR(100) NOT NULL UNIQUE,
    edad INTEGER,
    sexo VARCHAR(10),
    ubigeo VARCHAR(10),
    estado_conadis VARCHAR(50)
);

CREATE INDEX idx_dim_postulante_id ON dim_postulante(id_postulante_original);
CREATE INDEX idx_dim_postulante_conadis ON dim_postulante(estado_conadis);
CREATE INDEX idx_dim_postulante_sexo ON dim_postulante(sexo);
CREATE INDEX idx_dim_postulante_edad ON dim_postulante(edad);

COMMENT ON TABLE dim_postulante IS 'Postulantes consolidados - Centro ESTRELLA 1: Oferta de Talento';
COMMENT ON COLUMN dim_postulante.estado_conadis IS 'Certificacion CONADIS para analisis de inclusion laboral';
COMMENT ON COLUMN dim_postulante.id_postulante_original IS 'ID original del sistema fuente (trazabilidad)';

CREATE TABLE dim_carrera (
    carrera_sk SERIAL PRIMARY KEY,
    nombre_carrera TEXT NOT NULL,
    nivel_educativo VARCHAR(100)
);

CREATE INDEX idx_dim_carrera_nombre ON dim_carrera USING gin(to_tsvector('spanish', nombre_carrera));
CREATE INDEX idx_dim_carrera_nivel ON dim_carrera(nivel_educativo);

COMMENT ON TABLE dim_carrera IS 'Catalogo de carreras y programas educativos NORMALIZADOS - ESTRELLA 1';
COMMENT ON COLUMN dim_carrera.nombre_carrera IS 'Nombre de carrera representativo del grupo normalizado';
COMMENT ON COLUMN dim_carrera.nivel_educativo IS 'Nivel: Técnico, Universitario, Postgrado, etc.';

CREATE TABLE dim_institucion (
    institucion_sk SERIAL PRIMARY KEY,
    nombre_institucion TEXT NOT NULL
);

CREATE INDEX idx_dim_institucion_nombre ON dim_institucion USING gin(to_tsvector('spanish', nombre_institucion));

COMMENT ON TABLE dim_institucion IS 'Catalogo de instituciones educativas - ESTRELLA 1';

-- ESTRELLA 2: DEMANDA LABORAL - Dimensiones

CREATE TABLE dim_vacante (
    vacante_sk SERIAL PRIMARY KEY,
    id_vacante_original INTEGER NOT NULL UNIQUE,
    nombre_aviso TEXT,
    num_vacantes INTEGER DEFAULT 0,
    sector VARCHAR(200),
    ubigeo VARCHAR(10),
    sin_experiencia BOOLEAN DEFAULT FALSE,
    tiempo_experiencia NUMERIC DEFAULT 0
);

CREATE INDEX idx_dim_vacante_id ON dim_vacante(id_vacante_original);
CREATE INDEX idx_dim_vacante_sector ON dim_vacante(sector);
CREATE INDEX idx_dim_vacante_ubigeo ON dim_vacante(ubigeo);

COMMENT ON TABLE dim_vacante IS 'Vacantes consolidadas - Centro ESTRELLA 2: Demanda Laboral';
COMMENT ON COLUMN dim_vacante.sin_experiencia IS 'TRUE si no requiere experiencia, FALSE por defecto';
COMMENT ON COLUMN dim_vacante.tiempo_experiencia IS 'Tiempo de experiencia requerido en meses (0 si no aplica)';
COMMENT ON COLUMN dim_vacante.id_vacante_original IS 'AVISOID original del sistema fuente (trazabilidad)';

CREATE TABLE dim_empresa (
    empresa_sk SERIAL PRIMARY KEY,
    id_empresa_original VARCHAR(100) NOT NULL UNIQUE
);

CREATE INDEX idx_dim_empresa_id ON dim_empresa(id_empresa_original);

COMMENT ON TABLE dim_empresa IS 'Catalogo de empresas publicantes - ESTRELLA 2';

CREATE TABLE dim_competencia (
    competencia_sk SERIAL PRIMARY KEY,
    nombre_competencia TEXT NOT NULL
);

CREATE INDEX idx_dim_competencia_nombre ON dim_competencia USING gin(to_tsvector('spanish', nombre_competencia));

COMMENT ON TABLE dim_competencia IS 'Catalogo de competencias y habilidades requeridas - ESTRELLA 2';

-- =============================================================================
-- TABLAS DE HECHOS (5 tablas)
-- =============================================================================

-- ESTRELLA 1: OFERTA DE TALENTO - Tablas de Hechos

CREATE TABLE hechos_postulante (
    postulante_sk INTEGER NOT NULL,
    ubicacion_sk INTEGER NOT NULL,
    fecha_registro_sk INTEGER NOT NULL,

    CONSTRAINT pk_hechos_postulante PRIMARY KEY (postulante_sk),
    CONSTRAINT fk_hp_postulante FOREIGN KEY (postulante_sk)
        REFERENCES dim_postulante(postulante_sk) ON DELETE CASCADE,
    CONSTRAINT fk_hp_ubicacion FOREIGN KEY (ubicacion_sk)
        REFERENCES dim_ubicacion(ubicacion_sk) ON DELETE CASCADE,
    CONSTRAINT fk_hp_fecha FOREIGN KEY (fecha_registro_sk)
        REFERENCES dim_tiempo(fecha_sk) ON DELETE CASCADE
);

CREATE INDEX idx_hp_postulante ON hechos_postulante(postulante_sk);
CREATE INDEX idx_hp_ubicacion ON hechos_postulante(ubicacion_sk);
CREATE INDEX idx_hp_fecha ON hechos_postulante(fecha_registro_sk);

COMMENT ON TABLE hechos_postulante IS 'Hechos principales de postulantes - ESTRELLA 1 - Granularidad: 1 registro por postulante';
COMMENT ON COLUMN hechos_postulante.postulante_sk IS 'FK a dim_postulante (PK, 1:1)';
COMMENT ON COLUMN hechos_postulante.ubicacion_sk IS 'FK a dim_ubicacion (ubicacion del postulante)';
COMMENT ON COLUMN hechos_postulante.fecha_registro_sk IS 'FK a dim_tiempo (fecha de registro)';

CREATE TABLE hechos_formacion (
    postulante_sk INTEGER NOT NULL,
    carrera_sk INTEGER NOT NULL,
    institucion_sk INTEGER NOT NULL,

    CONSTRAINT fk_hf_postulante FOREIGN KEY (postulante_sk)
        REFERENCES dim_postulante(postulante_sk) ON DELETE CASCADE,
    CONSTRAINT fk_hf_carrera FOREIGN KEY (carrera_sk)
        REFERENCES dim_carrera(carrera_sk) ON DELETE CASCADE,
    CONSTRAINT fk_hf_institucion FOREIGN KEY (institucion_sk)
        REFERENCES dim_institucion(institucion_sk) ON DELETE CASCADE
);

CREATE INDEX idx_hf_postulante ON hechos_formacion(postulante_sk);
CREATE INDEX idx_hf_carrera ON hechos_formacion(carrera_sk);
CREATE INDEX idx_hf_institucion ON hechos_formacion(institucion_sk);

COMMENT ON TABLE hechos_formacion IS 'Detalle de formacion academica - ESTRELLA 1 - Granularidad: N formaciones por postulante';

CREATE TABLE hechos_experiencia (
    postulante_sk INTEGER NOT NULL,

    CONSTRAINT fk_he_postulante FOREIGN KEY (postulante_sk)
        REFERENCES dim_postulante(postulante_sk) ON DELETE CASCADE
);

CREATE INDEX idx_he_postulante ON hechos_experiencia(postulante_sk);

COMMENT ON TABLE hechos_experiencia IS 'Detalle de experiencias laborales - ESTRELLA 1 - Granularidad: N experiencias por postulante';

-- ESTRELLA 2: DEMANDA LABORAL - Tablas de Hechos

CREATE TABLE hechos_vacante (
    vacante_sk INTEGER NOT NULL,
    ubicacion_sk INTEGER NOT NULL,
    empresa_sk INTEGER NOT NULL,
    fecha_publicacion_sk INTEGER NOT NULL,
    activo BOOLEAN DEFAULT TRUE,

    CONSTRAINT pk_hechos_vacante PRIMARY KEY (vacante_sk),
    CONSTRAINT fk_hv_vacante FOREIGN KEY (vacante_sk)
        REFERENCES dim_vacante(vacante_sk) ON DELETE CASCADE,
    CONSTRAINT fk_hv_ubicacion FOREIGN KEY (ubicacion_sk)
        REFERENCES dim_ubicacion(ubicacion_sk) ON DELETE CASCADE,
    CONSTRAINT fk_hv_empresa FOREIGN KEY (empresa_sk)
        REFERENCES dim_empresa(empresa_sk) ON DELETE CASCADE,
    CONSTRAINT fk_hv_fecha FOREIGN KEY (fecha_publicacion_sk)
        REFERENCES dim_tiempo(fecha_sk) ON DELETE CASCADE
);

CREATE INDEX idx_hv_vacante ON hechos_vacante(vacante_sk);
CREATE INDEX idx_hv_ubicacion ON hechos_vacante(ubicacion_sk);
CREATE INDEX idx_hv_empresa ON hechos_vacante(empresa_sk);
CREATE INDEX idx_hv_fecha ON hechos_vacante(fecha_publicacion_sk);
CREATE INDEX idx_hv_activo ON hechos_vacante(activo);

COMMENT ON TABLE hechos_vacante IS 'Hechos principales de vacantes - ESTRELLA 2 - Granularidad: 1 registro por vacante';
COMMENT ON COLUMN hechos_vacante.activo IS 'Estado de la vacante (TRUE=activa, FALSE=inactiva/cerrada)';

CREATE TABLE hechos_competencia_requerida (
    vacante_sk INTEGER NOT NULL,
    competencia_sk INTEGER NOT NULL,

    CONSTRAINT fk_hcr_vacante FOREIGN KEY (vacante_sk)
        REFERENCES dim_vacante(vacante_sk) ON DELETE CASCADE,
    CONSTRAINT fk_hcr_competencia FOREIGN KEY (competencia_sk)
        REFERENCES dim_competencia(competencia_sk) ON DELETE CASCADE
);

CREATE INDEX idx_hcr_vacante ON hechos_competencia_requerida(vacante_sk);
CREATE INDEX idx_hcr_competencia ON hechos_competencia_requerida(competencia_sk);

COMMENT ON TABLE hechos_competencia_requerida IS 'Detalle de competencias requeridas - ESTRELLA 2 - Granularidad: N competencias por vacante';

-- RESUMEN FINAL DEL ESQUEMA
COMMENT ON SCHEMA public IS 'Data Warehouse MTPE -  452,732 registros';

