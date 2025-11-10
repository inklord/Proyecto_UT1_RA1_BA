# Proyecto UT1 - Ingesta y Análisis de Datos de Ventas

Este proyecto implementa un pipeline de datos para procesar, analizar y reportar datos de ventas.

## Características

### 1. Ingestión y Almacenamiento
- Ingesta batch con idempotencia
- Trazabilidad completa (_ingest_ts, _source_file, _batch_id)
- Almacenamiento en Parquet (particionado por año/mes)
- SQLite como base de datos relacional
- Cuarentena para datos inválidos

### 2. Limpieza y Modelado
- Validación de tipos de datos y rangos
- Deduplicación basada en clave natural
- Política "último gana" por _ingest_ts
- Cálculos derivados (importe total)

### 3. Reportes
- KPIs con definiciones claras
- Análisis temporal y por producto
- Contexto y conclusiones
- Formato Markdown para fácil publicación

## Requisitos

- Python 3.11+
- Dependencias en `environment.yml`:
  - pandas
  - pyarrow
  - sqlite3
  - tabulate

## Estructura del Proyecto

```
project/
├── data/          # Datos fuente
├── docs/          # Documentación
├── ingest/        # Scripts de ingesta
├── notebooks/     # Notebooks de análisis
├── output/        # Resultados y reportes
└── sql/          # Esquemas y consultas SQL
```

## Cómo Ejecutar

1. Crear el ambiente conda:
   ```
   conda env create -f project/environment.yml
   conda activate ut1
   ```

2. Ejecutar notebooks en orden:
   - `01_ingesta.ipynb`: Procesa datos fuente
   - `02_limpieza.ipynb`: Limpia y modela datos
   - `03_reporte.ipynb`: Genera reporte final

## Decisiones de Diseño

1. **Idempotencia**: Se implementa mediante:
   - Batch ID único por ejecución
   - Timestamp de ingesta
   - Política "último gana" en deduplicación

2. **Particionado**: 
   - Temporal (año/mes) para optimizar consultas
   - Facilita retención y archivado

3. **Calidad de Datos**:
   - Validación temprana en ingesta
   - Cuarentena con razón de rechazo
   - Tipos de datos estrictos

## Supuestos

1. **Datos Fuente**:
   - CSV en formato específico
   - Campos requeridos: fecha, id_cliente, id_producto, unidades, precio_unitario
   - Una fila por transacción

2. **Negocio**:
   - Precio unitario y unidades deben ser positivos
   - No se permiten transacciones duplicadas (misma fecha/cliente/producto)
   - Última versión es la correcta en caso de duplicados

## Extensiones Posibles

1. **Procesamiento**:
   - Paralelización de ingesta
   - Validaciones adicionales
   - Más cálculos derivados

2. **Almacenamiento**:
   - Migración a base de datos distribuida
   - Compresión de datos históricos
   - Políticas de retención

3. **Reportes**:
   - Visualizaciones interactivas
   - Alertas automáticas
   - APIs de consulta · Solución de ingestión, almacenamiento y reporte (UT1 · RA1)

Este repositorio contiene:
- **project/**: código reproducible (ingesta → clean → oro → reporte Markdown).
- **site/**: web pública con **Quartz 4** (GitHub Pages). El reporte UT1 se publica en `site/content/reportes/`.

## Ejecución rápida
```bash
# 1) Dependencias (elige uno)
python -m venv .venv
.venv\Scripts\activate  # (o source .venv/bin/activate)
pip install -r project/requirements.txt
# o: conda env create -f project/environment.yml && conda activate ut1

# 2) (Opcional) Generar datos de ejemplo
python project/ingest/get_data.py

# 3) Pipeline fin-a-fin (ingesta→clean→oro→reporte.md)
python project/ingest/run.py

# 4) Copiar el reporte a la web Quartz
python project/tools/copy_report_to_site.py

# 5) (Opcional) Previsualizar la web en local
cd site
npx quartz build --serve   # abre http://localhost:8080
```

## Publicación web (GitHub Pages)
- En **Settings → Pages**, selecciona **Source = GitHub Actions**.
- El workflow `./.github/workflows/deploy-pages.yml` compila `site/` y despliega.

## Flujo de datos
Bronce (`raw`) → Plata (`clean`) → Oro (`analytics`).  
Idempotencia por `batch_id` (batch) o `event_id` (stream).  
Deduplicación “último gana” por `_ingest_ts`.  
Reporte Markdown: `project/output/reporte.md` → `site/content/reportes/reporte-UT1.md`.
# BDA_Proyecto_UT1_RA1


