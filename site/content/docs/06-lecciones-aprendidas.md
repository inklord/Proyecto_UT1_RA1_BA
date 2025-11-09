---
title: "Lecciones aprendidas"
tags: ["UT1","docs"]
author: "Mario CAscado Nieto"
---

# Lecciones aprendidas

Esto ha sido bastante más largo de lo que parecía. Pensaba “leo el CSV y ya”, y al final cada paso tenía algún mini-drama. Aun así, quedó funcionando y entendí por qué en estos proyectos todo el mundo insiste con “idempotencia” y “trazabilidad”.

## Lo que me dio guerra
- CSV “simplicísimo” con valores raros: un “doce” donde debía ir un número. Directo a quarantine y a seguir. Aprendido: siempre castear con cuidado y asumir que algo vendrá roto.
- SQLite y sus errores crípticos: metí varias sentencias juntas y me soltó “You can only execute one statement at a time”. La solución fue ejecutar cada UPSERT por separado. Cansino, pero ya no se queja.
- El `_ingest_ts` desaparecido: después del `merge` con el catálogo, se partió en `_ingest_ts_x/_y`. Estuve un rato persiguiendo por qué el FACT no tenía timestamp. Lo dejé normalizado a `_ingest_ts` y borré las columnas auxiliares.
- Duplicados al reprocesar: la FACT se duplicaba al segundo intento. Añadí un `DELETE` por `(fecha,id_cliente,id_producto)` antes de insertar y listo. Importante: acordarte de la idempotencia también en FACT.
- Quartz haciendo de las suyas: lo corrí desde la raíz del repo y me dijo que no encontraba el ejecutable. Era “entra en `site/`”. También el puerto 8080 ya estaba pillado, así que 8081 y fuera.

## Cosas que sí salieron bien
- Pipeline fin a fin: CSV → silver → dim/fact → vistas → reporte en Markdown.
- Regla “último gana por `_ingest_ts`” en silver y limpieza previa en FACT para no duplicar.
- Leer el reporte desde Parquet fue rápido y me evitó estados raros de la base de datos.

## Qué haría distinto la próxima vez
- Empezar el repo con una lista de “invariantes” obligatorios: trazar `_ingest_ts`, `_source_file`, `_batch_id` desde el minuto 1; definir la clave natural y probar reejecuciones el mismo día.
- Escribir un test tontísimo de conteos: “si reingesto N veces, las filas de FACT no cambian”. Ahorraría tiempo.
- Poner tipos “de verdad” para dinero en DB (DECIMAL/enteros en céntimos) en vez de REAL, aunque para la práctica el REAL me valió.

## Mini‑chuleta que me guardo
- Si haces `merge`, revisa columnas duplicadas con sufijo `_x/_y` y decide la buena antes de seguir.
- Si algo falla “sin motivo”, mira `_source_file` y `_batch_id`: suelen revelar el problema.
- Si `npx quartz` falla, ejecuta dentro de `site/` o usa `--prefix site`, y cambia el puerto si está ocupado.

## Conclusión
Ha sido un poco coñazo por los detalles, pero justo ahí está el aprendizaje: si ignoras los “pequeños” (idempotencia, trazas, casts, joins), el proyecto se te vuelve loco al segundo intento. Ahora tengo claro el orden: traza todo, define la clave, prueba reprocesos, y recién después te preocupas por lo bonito.

---

## Diario del proyecto 
Al principio pensaba que esto era “leer un CSV y ya”, pero fue un poco pesado porque cada cosa tenía su truco:
- Leer CSV: fácil… hasta que uno tenía valores raros (“doce” en vez de número). A quarantine y a seguir.
- SQLite: parecía sencillo, pero te suelta errores crípticos si metes varias sentencias a la vez. Tocó separar los UPSERTs y listo.
- Join con el catálogo: mágicamente desapareció `_ingest_ts` por el `merge` y se partió en `_x/_y`. Me rayé un rato hasta que lo normalicé.
- Reprocesar: duplicaba la FACT y dije “¿por qué?”. Porque no borraba antes por la clave. Borrado previo y arreglado.
- Quartz: lo ejecuté desde la raíz y me gritó que no encontraba el ejecutable. Era “entra en `site/` pesado”. También el puerto 8080 estaba pillado: 8081 y fuera dramas.

## Lo que más costó
- Acordarme de la idempotencia en TODAS las capas. Si te olvidas en FACT, se nota al segundo reintento.
- Entender que “último gana por `_ingest_ts`” no es poesía, es la diferencia entre datos bien y un churro.
- No pelearse con Windows/PowerShell por variables de entorno y permisos del venv.

## Trucos que me guardo
- Siempre guardar `_source_file`, `_ingest_ts`, `_batch_id`. Cuando algo sale mal, esas columnas son tu mapa.
- Si haces `merge`, revisa las columnas duplicadas con sufijo `_x/_y` y decide cuál quieres conservar antes de seguir.
- Para REPORTES, leer de Parquet: es más rápido y no te lían los estados intermedios de la DB.

