---
title: Proyecto UT1 · RA1 · Big Data Aplicado
---
# Proyecto UT1 – Ingestión, Almacenamiento y Reporte

Publicación web de los resultados de la práctica.

- Autor: mario cascado nieto
- 👉 [Reporte UT1](./reportes/reporte-UT1)
- 🧩 [Metodología](./metodologia)
- 📚 Documentación: [Diseño de ingesta](./docs/02-diseno-ingesta) · [Limpieza y calidad](./docs/03-limpieza-calidad) · [Modelado oro](./docs/04-modelado-oro) · [Reporte/KPIs](./docs/05-reporte-plantilla)

## Resumen rápido

- Fuente de datos: CSV/NDJSON en `project/data/drops/`
- Persistencia: Parquet + SQLite (`project/output/`)
- Hechos y dimensiones: `fact_ventas`, `dim_producto`
- Idempotencia y deduplicación por `_ingest_ts`

## Cómo se generó

1) Ingesta y limpieza: convierte tipos, valida rangos, y cruza catálogo  
2) Upserts en SQLite + Parquet en `output/`  
3) Reporte Markdown con KPIs y tablas → [Reporte UT1](./reportes/reporte-UT1)  

> Si quieres reproducir: ver el archivo `README.md` del repositorio y el cuaderno `project/notebooks/03_reporte.ipynb`.
