---
title: "Limpieza y calidad"
tags: ["UT1","docs"]
author: "Mario CAscado Nieto"
---

# Reglas de limpieza y calidad

## Tipos y formatos
- `fecha`: ISO (`YYYY-MM-DD`)
- `unidades`: entero ≥ 0
- `precio_unitario`: decimal ≥ 0

## Nulos
- Obligatorios: `fecha`, `id_cliente`, `id_producto`, `unidades`, `precio_unitario`
- Tratamiento: filas inválidas → quarantine con causa

## Rangos y dominios
- `unidades >= 0`
- `precio_unitario >= 0`
- `id_producto` debe existir en catálogo

## Deduplicación
- Clave natural: `(fecha, id_cliente, id_producto)`
- Política: último gana por `_ingest_ts`

## Trazabilidad
- Mantener `_ingest_ts`, `_source_file`, `_batch_id`


