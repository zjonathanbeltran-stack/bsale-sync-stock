# Sincronizador de Stock Bsale

Agente que virtualiza el stock de la Bodega Distribuidora en Bsale,
tomando el máximo disponible de las bodegas físicas reales.

## Variables de entorno requeridas

Configúralas en Railway → Variables:

| Variable | Descripción |
|---|---|
| `BSALE_API_TOKEN` | Token de acceso a la API de Bsale |
| `distribuidora_name` | Nombre parcial de la bodega distribuidora (ej. `distribuidora`) |
| `price_list_name` | Nombre de la lista de precios (ej. `DISTRIBUIDORA PRECIOS`) |
| `sync_all` | `true` para sincronizar todo el catálogo |
| `dry_run` | `true` para simular sin hacer cambios reales |
| `GMAIL_USER` | Tu correo Gmail para enviar resumen |
| `GMAIL_APP_PASSWORD` | Contraseña de aplicación de Gmail |
| `NOTIFY_EMAIL` | Correo donde recibes el resumen |

## Horario

Se ejecuta automáticamente todos los días a las **21:00 hora Chile** (00:00 UTC).
