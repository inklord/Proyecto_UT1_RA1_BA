---
title: "Definición de métricas y tablas oro"
---

# Modelo de negocio (oro)

## Tablas oro
- `clean_ventas` (fuente): línea de venta
- `ventas_diarias` (vista): día
- `ventas_diarias_producto` (vista): día × producto

## Métricas
- Ingresos: Σ(`unidades * precio_unitario`)
- Ticket medio: `ingresos / líneas`
- Unidades: Σ(`unidades`)

## Consultas base (SQL)
```sql
SELECT fecha, SUM(unidades*precio_unitario) AS importe_total, COUNT(*) AS lineas
FROM clean_ventas
GROUP BY fecha;
```


