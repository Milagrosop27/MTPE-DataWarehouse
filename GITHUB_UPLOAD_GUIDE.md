# 🚀 Guía Rápida: Subir a GitHub

## ✅ Pre-requisitos Completados

- ✅ Credenciales eliminadas (`.env`)
- ✅ Archivos cache limpiados (`__pycache__`)
- ✅ `.gitignore` configurado
- ✅ Documentación actualizada
- ✅ Script de verificación creado

## 📦 Pasos para Subir

### 1. Verificar Seguridad (Última vez)

```bash
python check_security.py
```

**Debe mostrar**: ✅ TODAS LAS VERIFICACIONES PASARON

### 2. Inicializar Git (si no está inicializado)

```bash
git init
```

### 3. Agregar Todos los Archivos

```bash
git add .
```

### 4. Verificar qué se va a subir

```bash
git status
```

**Asegúrate que NO aparezcan**:
- ❌ `.env`
- ❌ `__pycache__/`
- ❌ Archivos `.pyc`
- ❌ Archivos grandes `.csv` (excepto diccionarios)

### 5. Hacer el Primer Commit

```bash
git commit -m "Initial commit: Data Warehouse MTPE - ETL Pipeline y Modelo Constelación"
```

### 6. Crear Repositorio en GitHub

1. Ve a: https://github.com/new
2. Nombre: `MTPE-DataWarehouse` (o el que prefieras)
3. Descripción: `Data Warehouse MTPE - Modelo de Constelación para análisis del mercado laboral peruano`
4. **NO** inicializar con README (ya tienes uno)
5. **NO** agregar .gitignore (ya tienes uno)
6. Click en **"Create repository"**

### 7. Conectar y Subir

Copia y pega los comandos que GitHub te muestra:

```bash
git remote add origin https://github.com/TU_USUARIO/MTPE-DataWarehouse.git
git branch -M main
git push -u origin main
```

## 🎯 Post-Upload

### Configurar el Repositorio

1. **Agregar Topics** (en GitHub):
   - `data-warehouse`
   - `etl`
   - `postgresql`
   - `python`
   - `power-bi`
   - `neondb`
   - `star-schema`

2. **Actualizar About** (en GitHub):
   - Descripción: "Data Warehouse empresarial con modelo de constelación para análisis del mercado laboral peruano. ETL en Python + PostgreSQL + Power BI"
   - Website: (si tienes)

3. **Configurar README Shield Badges** (ya incluidos en README.md)

### Para Colaboradores

Envía estas instrucciones a tu equipo:

1. **Clonar el repositorio**:
   ```bash
   git clone https://github.com/TU_USUARIO/MTPE-DataWarehouse.git
   cd MTPE-DataWarehouse
   ```

2. **Crear entorno virtual**:
   ```bash
   python -m venv .venv
   .venv\Scripts\activate  # Windows
   ```

3. **Instalar dependencias**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Configurar credenciales**:
   ```bash
   # Copiar template
   cp 2_ETL_INTEGRATION/config/.env.example 2_ETL_INTEGRATION/config/.env
   
   # Editar .env con credenciales reales
   # Solicitar credenciales al admin del proyecto
   ```

## ⚠️ Recordatorios Importantes

### Antes de CADA Commit Futuro

```bash
# 1. Verificar seguridad
python check_security.py

# 2. Revisar cambios
git status
git diff

# 3. Agregar y commitear
git add .
git commit -m "Descripción clara del cambio"

# 4. Subir
git push
```

### Si Accidentalmente Subes .env

**ACCIÓN INMEDIATA**:

1. **Remover del repositorio**:
   ```bash
   git rm --cached 2_ETL_INTEGRATION/config/.env
   git commit -m "Remove sensitive .env file"
   git push
   ```

2. **Rotar credenciales** en NeonDB inmediatamente

3. **Considerar** hacer el repositorio privado temporalmente

## 📊 Datos en el Repositorio

### ✅ Incluidos
- 📄 Código Python (scripts, módulos)
- 📓 Notebooks Jupyter
- 📋 SQL (schema creation)
- 📖 Documentación (README, markdown)
- 📊 Diccionarios de datos (6 archivos .xlsx)
- ⚙️ Configuraciones (requirements.txt, pyproject.toml)

### ❌ Excluidos (por .gitignore)
- 🔒 Credenciales (.env)
- 📦 Datos CSV (128 MB)
- 🐍 Cache Python (__pycache__, .pyc)
- 💻 Configuraciones de IDE (.vscode, .idea)
- 🗃️ Bases de datos locales (.db, .sqlite)

## 🎓 Git Flow Recomendado

### Para Desarrollo

```bash
# Crear rama de feature
git checkout -b feature/nueva-funcionalidad

# Desarrollar y commitear
git add .
git commit -m "Descripción del cambio"

# Subir rama
git push origin feature/nueva-funcionalidad

# Crear Pull Request en GitHub
# Revisar → Aprobar → Merge
```

## 📞 Soporte

Si tienes problemas:
1. Revisa `SECURITY_AUDIT_REPORT.md`
2. Ejecuta `python check_security.py`
3. Verifica `.gitignore`

---

**¡Listo!** Tu proyecto está preparado profesionalmente para GitHub 🎉

