# Sincronizador de Stock Bsale

Agente que virtualiza el stock de la Bodega Distribuidora en Bsale,
tomando el máximo disponible de las bodegas físicas reales.

Desde v6 también sincroniza valores:
- **Costos**: las recepciones se crean con el costo real de la variante
  (última recepción con costo > 0, o el costo promedio como respaldo),
  evitando que la Distribuidora quede con costo unitario $0.
- **Precios (v7)**: los productos con precio $0 en la Distribuidora se corrigen
  cruzando por código de barras con la lista `cm_price_list_name` de **Casa Matriz**
  (empresa separada, token separado), aplicando un descuento configurable (default 5%).
- **Packs (v8)**: los productos tipo Pack (cajas/displays, `classification == 3` que
  *no controlan stock*) que están en $0 reciben precio calculado desde su contenido:
  `precio_pack = Σ (precio_unitario_componente × cantidad)`. Se corrige tanto en la lista
  de la Distribuidora como en la de Casa Matriz. Los packs abiertos (variante variable) o
  con algún componente sin precio quedan listados como "pendientes" en el resumen.

## Variables de entorno requeridas

Configúralas en Railway → Variables:

| Variable | Descripción |
|---|---|
| `BSALE_API_TOKEN` | Token de la empresa **Distribuidora** en Bsale |
| `BSALE_CM_TOKEN` | Token de la empresa **Casa Matriz** en Bsale (fuente de precios) |
| `distribuidora_name` | Nombre parcial de la bodega distribuidora (ej. `distribuidora`) |
| `price_list_name` | Lista de precios de la Distribuidora (ej. `DISTRIBUIDORA PRECIOS`) |
| `cm_price_list_name` | Lista de precios de Casa Matriz a usar como fuente (default: `sala de ventas precios`) |
| `price_discount_pct` | Descuento % sobre el precio de Casa Matriz (default: `5`) |
| `sync_costs` | `true` (default) para enviar costo real en las recepciones |
| `sync_prices` | `true` (default) para corregir precios $0 desde Casa Matriz |
| `sync_packs` | `true` (default) para calcular y corregir precios de packs en $0 |
| `packs_only` | `true` para ejecutar **solo** la corrección de packs (sin tocar stock ni precios unitarios) |
| `pack_discount_pct` | Descuento % sobre el precio del pack calculado (default `0` = exacto) |
| `sync_all` | `true` para sincronizar todo el catálogo |
| `dry_run` | `true` para simular sin hacer cambios reales |
| `GMAIL_USER` | Tu correo Gmail para enviar resumen |
| `GMAIL_APP_PASSWORD` | Contraseña de aplicación de Gmail |
| `NOTIFY_EMAIL` | Correo donde recibes el resumen |

## Modo "solo packs" (corrección puntual)

Para llenar los precios de los packs que hoy están en $0 **sin** correr toda la
sincronización de stock, ejecuta con:

```
packs_only=true dry_run=true   # primero simula y revisa el log/email
packs_only=true dry_run=false  # luego aplica los cambios reales
```

Recorre la lista de la Distribuidora y la de Casa Matriz, calcula el precio de cada
pack desde su contenido y deja un log en `files/packs_log_*.json`. En modo normal
(sin `packs_only`) el mismo paso corre integrado al final de cada sincronización diaria
mientras `sync_packs=true`.

## Horario

Se ejecuta automáticamente todos los días a las **21:00 hora Chile** (00:00 UTC).
