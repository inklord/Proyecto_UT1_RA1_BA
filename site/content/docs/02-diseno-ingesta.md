---
title: "Diseño de ingestión"
tags: ["UT1","docs"]
author: "Mario CAscado Nieto"
---

# Diseño de ingestión

## Resumen
Describe cómo entran los datos, la frecuencia y las garantías mínimas.

## Fuente
- Origen: `data/drops/*.csv` / NDJSON
- Formato: CSV / JSONL
- Frecuencia: batch

## Estrategia
- Modo: batch
- Incremental: por ficheros nuevos en `drops/`
- Particionado: por fecha (opcional)

## Idempotencia y deduplicación
- `batch_id`: hash sencillo del nombre de archivo (o pasado por ENV)
- Clave natural: `(fecha, id_cliente, id_producto)` o `event_id`
- Política: último gana por `_ingest_ts`

## Checkpoints y trazabilidad
- Trazas: `_ingest_ts`, `_source_file`, `_batch_id`
- Quarantine: CSV en `output/quality/` con causa


