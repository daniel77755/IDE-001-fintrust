# IDE-001-fintrust

Pipeline ETL/ELT de cartera crediticia para Fintrust, con capa analítica lista para Power BI y agente conversacional sobre el modelo de datos.

---

## Estructura del proyecto

```
sql/
  01-raw/               DDL de tablas fuente
  02-staging/           Limpieza, estandarización y validación referencial
  03-analytics/         Data mart y vistas temáticas para BI
  04-queries-negocio/   Consultas de negocio listas para ejecutar
python/
  pipeline.py           Orquestador ETL: raw → staging → analytics → exports
  validations.py        Suite de calidad de datos sobre tablas raw
  requirements.txt      Dependencias del pipeline
bonus/LLMs/
  agente.py             Agente conversacional (Streamlit + Gemini)
  knowledge.txt         Conocimiento del agente (editable)
  requirements.txt      Dependencias del agente
docs/
  decisiones-tecnicas.md   Justificación de cada decisión de arquitectura
  evidencia-calidad-datos.md  Reglas de calidad y anomalías encontradas
exports/                Archivos .parquet generados por el pipeline
fintrust.duckdb         Base de datos local (generada al ejecutar el pipeline)
```

---

## Requisitos previos

### 1. Python 3.11+

Verifica con:
```bash
python --version
```

### 2. DuckDB CLI (opcional, para consultas directas)

Descarga el ejecutable desde:
```
https://duckdb.org/docs/installation
```
Selecciona tu sistema operativo y descarga la versión CLI. No requiere instalación — es un único ejecutable.

Para abrir la base de datos del proyecto:
```bash
duckdb fintrust.duckdb
```

También puedes lanzar la interfaz web integrada de DuckDB para explorar los datos visualmente:
```bash
duckdb -ui
```
Esto abre automáticamente un navegador con una UI donde puedes navegar esquemas, tablas y ejecutar consultas SQL sobre `fintrust.duckdb`.

### 3. API Key de Google Gemini (solo para el agente)

1. Ingresa a `https://aistudio.google.com/app/apikey`
2. Inicia sesión con tu cuenta Google
3. Crea una nueva API key (el plan gratuito es suficiente)
4. Copia la key y pégala en la variable `GEMINI_API_KEY` del archivo `bonus/LLMs/agente.py`

> **Importante:** nunca compartas ni subas tu API key a repositorios públicos.

---

## Instalación

```bash
# Dependencias del pipeline ETL
pip install -r python/requirements.txt

# Dependencias del agente (solo si vas a usarlo)
pip install -r bonus/LLMs/requirements.txt
```

---

## Ejecución del pipeline

```bash
# Carga completa: raw → staging → analytics → exports/
python python/pipeline.py
```

El pipeline genera `fintrust.duckdb` y los archivos `.parquet` en `exports/`.

---

## Ejecución del agente conversacional

```bash
streamlit run bonus/LLMs/agente.py
```

Se abre automáticamente en `http://localhost:8501`.

El agente responde preguntas sobre las tablas y métricas del modelo analytics.
No expone SQL ni estructura técnica interna.

Para más detalles sobre el agente, consulta [bonus/LLMs/llm_proposal.md](bonus/LLMs/llm_proposal.md).

---

## Conexión a Power BI

1. Abre Power BI Desktop
2. `Obtener datos → Parquet`
3. Apunta a la carpeta `exports/`
4. Cada archivo `.parquet` aparece como una tabla independiente lista para usar

Archivos disponibles en `exports/`:

| Archivo | Contenido |
|---|---|
| `dm_cartera.parquet` | Detalle por cuota — base de todos los análisis |
| `vw_daily_snapshot.parquet` | KPIs ejecutivos del día |
| `vw_desembolsos_dia_ciudad_segmento.parquet` | Desembolso por día y ciudad |
| `vw_saldo_segmento.parquet` | Saldo vigente y vencido por segmento |
| `vw_recaudo_mora.parquet` | Recaudo diario por canal y producto |
| `vw_cohort_deterioro.parquet` | Deterioro por cohorte de originación |
| `vw_top10_creditos_atraso.parquet` | Top 10 créditos en mora |

---

## Decisiones clave

| Decisión | Razón |
|---|---|
| DuckDB en lugar de BigQuery | Entorno local sin servidor; SQL 100% compatible con BQ para migración futura |
| Vistas en lugar de tablas físicas en staging y analytics | Recalculo siempre fresco; sin necesidad de truncar y recargar |
| Flag `inconsistency` en staging | Conserva registros con problemas para auditoría sin bloquear el pipeline |
| Un `.parquet` por vista en `exports/` | Power BI conecta a tablas ya agregadas; evita transformaciones en el lado BI |
| `knowledge.txt` separado del agente | El contenido del agente se actualiza sin tocar código |

Para el detalle completo de cada decisión ver [docs/decisiones-tecnicas.md](docs/decisiones-tecnicas.md).

---

## Calidad de datos

Las reglas de validación y las anomalías encontradas en los datos fuente están documentadas en [docs/evidencia-calidad-datos.md](docs/evidencia-calidad-datos.md).
