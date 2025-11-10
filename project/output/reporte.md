# Reporte UT1 · Ventas
<<<<<<< HEAD
**Periodo:** 2025-10-11 a 2025-11-10 · **Fuente:** clean_ventas (Parquet) · **Generado:** 2025-11-10T16:43:57.122935+00:00

## 1. Titular
Ingresos totales 4183.51 €; producto líder: P004.

## 2. KPIs
- **Ingresos netos:** 4183.51 €
- **Ticket medio:** 89.01 €
- **Transacciones:** 47
=======
**Periodo:** 2025-01-03 a 2025-01-05 · **Fuente:** clean_ventas (Parquet) · **Generado:** 2025-10-23T10:33:01.765159+00:00

## 1. Titular
Ingresos totales 69.50 €; producto líder: P10.

## 2. KPIs
- **Ingresos netos:** 69.50 €
- **Ticket medio:** 17.38 €
- **Transacciones:** 4
>>>>>>> 7bdfc871baa9bcef1032f7aef3e635b35571e00b

## 3. Top productos
| id_producto   |   importe | pct   |
|:--------------|----------:|:------|
<<<<<<< HEAD
| P004          |   1419.61 | 34%   |
| P002          |    372    | 9%    |
| P006          |    365    | 9%    |
| P009          |    335.76 | 8%    |
| P001          |    309.69 | 7%    |
| P013          |    297    | 7%    |
| P005          |    202.5  | 5%    |
| P015          |    194.87 | 5%    |
| P010          |    157.5  | 4%    |
| P014          |    148    | 4%    |
| P003          |    102    | 2%    |
| P999          |     81.99 | 2%    |
| P012          |     74.85 | 2%    |
| P011          |     74.25 | 2%    |
| P008          |     42.5  | 1%    |
| P007          |      5.99 | 0%    |
=======
| P10           |      37.5 | 54%   |
| P20           |      32   | 46%   |
>>>>>>> 7bdfc871baa9bcef1032f7aef3e635b35571e00b

## 4. Resumen por día
| fecha      |   importe_total |   transacciones |
|:-----------|----------------:|----------------:|
<<<<<<< HEAD
| 2025-10-11 |            4.99 |               1 |
| 2025-10-12 |          588.9  |               2 |
| 2025-10-13 |           49.2  |               2 |
| 2025-10-14 |          158    |               2 |
| 2025-10-15 |           45    |               1 |
| 2025-10-16 |          102.46 |               2 |
| 2025-10-17 |          414.84 |               5 |
| 2025-10-20 |           29.97 |               1 |
| 2025-10-21 |           91.24 |               3 |
| 2025-10-22 |          112.5  |               1 |
| 2025-10-23 |          159.99 |               2 |
| 2025-10-24 |           67.5  |               1 |
| 2025-10-25 |          243.41 |               2 |
| 2025-10-26 |          186.16 |               4 |
| 2025-10-27 |          669.9  |               1 |
| 2025-10-28 |          182.5  |               1 |
| 2025-10-29 |          100.25 |               2 |
| 2025-10-30 |           59.94 |               1 |
| 2025-10-31 |           13.5  |               1 |
| 2025-11-02 |           29.97 |               1 |
| 2025-11-03 |           80.92 |               2 |
| 2025-11-05 |           99.9  |               1 |
| 2025-11-06 |           89.91 |               1 |
| 2025-11-07 |          153.98 |               2 |
| 2025-11-08 |          108.75 |               2 |
| 2025-11-09 |          174.83 |               2 |
| 2025-11-10 |          165    |               1 |

## 5. Calidad y cobertura
- Filas bronce: 65 · Plata: 47 · Cuarentena: 13

## 6. Persistencia
- Parquet: C:\Users\mario\Desktop\BDA_Proyecto_UT1_RA1-main\project\output\parquet\clean_ventas.parquet
- SQLite : C:\Users\mario\Desktop\BDA_Proyecto_UT1_RA1-main\project\output\ut1.db (tablas: raw_ventas, clean_ventas; vista: ventas_diarias)
=======
| 2025-01-03 |            25   |               1 |
| 2025-01-04 |            36.5 |               2 |
| 2025-01-05 |             8   |               1 |

## 5. Calidad y cobertura
- Filas bronce: 6 · Plata: 4 · Cuarentena: 2

## 6. Persistencia
- Parquet: D:\Proyectos\work\ESP-IA_BA-25-26\Proyecto_UT1_RA1_BA\project\output\parquet\clean_ventas.parquet
- SQLite : D:\Proyectos\work\ESP-IA_BA-25-26\Proyecto_UT1_RA1_BA\project\output\ut1.db (tablas: raw_ventas, clean_ventas; vista: ventas_diarias)
>>>>>>> 7bdfc871baa9bcef1032f7aef3e635b35571e00b

## 7. Conclusiones
- Reponer producto líder según demanda.
- Revisar filas en cuarentena (rangos/tipos).
- Valorar particionado por fecha para crecer.
