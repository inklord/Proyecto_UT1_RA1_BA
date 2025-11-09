---
title: "Plantilla de reporte · Mini‑DWH"
tags: ["UT1","docs"]
author: "Mario CAscado Nieto"
---

# Plantilla de reporte · Mini‑DWH (catálogo + ventas)

## Contexto y objetivo
- Caso: Mini‑DWH con catálogo de productos y ventas (modelado mínimo).
- Objetivo: practicar joins de dimensiones y medidas.

## Insumos
- `productos.csv` (id, nombre, categoría, precio_lista)
- `ventas.csv` (fecha, id_cliente, id_producto, unidades, precio_unitario)

## Ingestión (batch)
- Lectura desde `project/data/drops/`.
- Validación: `id_producto` debe existir en el catálogo.
- Trazabilidad: `_ingest_ts`, `_source_file`, `_batch_id`.

## Limpieza y modelado (silver → dim/fact)
- Tipado: fechas y numéricos (no negativos).
- Deduplicación: clave natural `(fecha, id_cliente, id_producto)`, “último gana” por `_ingest_ts`.
- Dimensión: `dim_producto` (última versión por `id`).
- Hechos: `fact_ventas` con `importe = unidades × precio_unitario`.

## Capa oro
- Vista: `ventas_diarias_producto (fecha × producto)` con:
  - `importe`, `unidades`, `ticket_medio = importe / líneas`.

## Almacenamiento
- SQL: SQLite (`project/output/ut1.db`) con `raw_*`, `clean_*`, `dim_producto`, `fact_ventas` y vistas.
- Analítica: Parquet (`project/output/parquet/`).

## Estructura del reporte (rellenar)

### 1) Titular
- Resumen en una línea (qué pasa, por qué importa, qué hacemos).

### 2) KPIs (con definiciones)
- Ingresos: __ €  — Σ(`unidades × precio_unitario`).
- Ticket medio: __ €  — `ingresos / nº líneas` (aprox. transacciones).
- Unidades: __  — Σ(`unidades`).

### 3) Por categoría
| categoría | importe | unidades |
|:----------|--------:|---------:|
| …         |     …   |     …    |

### 4) Por día (global)
| fecha | importe_total | transacciones |
|:------|--------------:|--------------:|
| …     |           …   |          …    |

### 5) Por día y categoría
| fecha | categoría | importe | unidades |
|:------|:----------|--------:|---------:|
| …     | …         |     …   |     …    |

### 6) Calidad y cobertura
- Bronce: __ · Plata: __ · Quarantine: __
- Motivos principales de quarantine: …

### 7) Supuestos
- Catálogo puede cambiar; se materializa `dim_producto` (último gana).
- `precio_unitario` manda para `importe`; `precio_lista` informativo.
- Línea ≈ transacción (no hay ID de pedido).

### 8) Conclusiones / Acciones
- …


