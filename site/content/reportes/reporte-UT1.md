# Reporte UT1 · Mini-DWH (Catálogo + Ventas)
**Periodo:** 2025-01-03 a 2025-01-05 · **Fuente:** FACT (Parquet/SQLite) · **Generado:** 2025-11-08T17:11:33.177120+00:00

Autor: mario cascado nieto

## 1. Titular
Ingresos totales 69.50 €; producto líder: Papeleria.

## 2. KPIs (definiciones)
- **Ingresos netos:** 69.50 € — suma de `unidades × precio_unitario` en FACT.
- **Ticket medio:** 17.38 € — ingresos / nº de líneas.
- **Transacciones:** 4

## 3. Top productos
| categoria   |   importe |   unidades |
|:------------|----------:|-----------:|
| Papeleria   |      69.5 |          7 |

## 4. Resumen por día
| fecha      |   importe_total |   transacciones |
|:-----------|----------------:|----------------:|
| 2025-01-03 |            25   |               1 |
| 2025-01-04 |            36.5 |               2 |
| 2025-01-05 |             8   |               1 |

## 5. Por día y categoría
| fecha      | categoria   |   importe |   unidades |
|:-----------|:------------|----------:|-----------:|
| 2025-01-03 | Papeleria   |      25   |          2 |
| 2025-01-04 | Papeleria   |      36.5 |          4 |
| 2025-01-05 | Papeleria   |       8   |          1 |

## 6. Calidad y trazabilidad
- Quarantine productos: 0 · Quarantine ventas: 2 · Batch: pdf-final

## 7. Persistencia
- Parquet: C:\Users\mario\OneDrive\Documentos\trabajos\BDA_Proyecto_UT1_RA1\project\output\parquet\clean_ventas.parquet, C:\Users\mario\OneDrive\Documentos\trabajos\BDA_Proyecto_UT1_RA1\project\output\parquet\dim_producto.parquet, C:\Users\mario\OneDrive\Documentos\trabajos\BDA_Proyecto_UT1_RA1\project\output\parquet\fact_ventas_by_fecha
- SQLite : C:\Users\mario\OneDrive\Documentos\trabajos\BDA_Proyecto_UT1_RA1\project\output\ut1.db (tablas: raw_ventas, clean_ventas, dim_producto, fact_ventas; vistas: ventas_diarias, ventas_diarias_producto)

## 8. Supuestos
- Ticket medio: 17.38 € (por línea; no hay ID de pedido)
- `precio_unitario` domina para `importe`; `precio_lista` es informativo.
- Catálogo puede cambiar; materializamos `dim_producto` (último gana).

## 9) Resumen de decisiones (ver /docs)
- Ingestión (02-diseno-ingesta): batch desde `project/data/drops/` (CSV/NDJSON), trazabilidad (`_ingest_ts`, `_source_file`, `BATCH_ID`) y validación de `id_producto` contra catálogo.
- Limpieza (03-limpieza-calidad): tipado y rangos, dedupe `(fecha,id_cliente,id_producto)` con política “último gana”, quarantine con motivo.
- Modelado (04-modelado-oro): `clean_productos`, `clean_ventas`, `dim_producto`, `fact_ventas`; vistas `ventas_diarias` y `ventas_diarias_producto`.
- KPIs/Reporte (05-reporte-plantilla): Ingresos, Ticket medio, Unidades; reporte en Markdown leído desde Parquet.

## 10) Lecciones aprendidas (ver 06-lecciones-aprendidas)
- Separar UPSERTs en SQLite; normalizar `_ingest_ts` tras joins (`_x/_y`).
- Borrar en `fact_ventas` por clave natural antes de reingestar para evitar duplicados.
- Para Quartz: ejecutar dentro de `site/` o usar `--prefix site`; usar `--port 8081` si 8080 está ocupado.

## 11) URL de la página (Quartz/Pages)
https://inklord.github.io/Proyecto_UT1_RA1_BA
