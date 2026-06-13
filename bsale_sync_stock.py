#!/usr/bin/env python3
"""
Sincronizador Stock Bsale v7
- Descarga paralela por bodega (aiohttp)
- Reintentos automáticos en 401/429/503 con backoff exponencial
- Pausas inteligentes entre bloques de POST para evitar rate limit
- Nombres completos de productos (expand=product)
- v6: recepciones con costo real (campo `cost`)
- v7: precios $0 corregidos desde CASA MATRIZ (token separado),
  cruce por código de barras, descuento configurable (default 5%)
"""
import os, json, asyncio, aiohttp, time, math
from datetime import datetime
from collections import defaultdict
import requests

# ── Configuración ──────────────────────────────────────────────
API_TOKEN             = os.environ.get("BSALE_API_TOKEN", "")       # token Distribuidora
CM_TOKEN              = os.environ.get("BSALE_CM_TOKEN", "")        # token Casa Matriz (fuente de precios)
BASE_URL              = "https://api.bsale.cl/v1"
HEADERS               = {"access_token": API_TOKEN, "Content-Type": "application/json"}
HEADERS_CM            = {"access_token": CM_TOKEN,  "Content-Type": "application/json"}
DISTRIBUIDORA_KEYWORD = os.environ.get("distribuidora_name", "distribuidora").strip().lower()
PRICE_LIST_NAME       = os.environ.get("price_list_name", "DISTRIBUIDORA PRECIOS").strip().lower()
CM_PRICE_LIST_NAME    = os.environ.get("cm_price_list_name", "SALA DE VENTAS PRECIOS").strip().lower()
PRICE_DISCOUNT_PCT    = float(os.environ.get("price_discount_pct", "5"))  # % descuento sobre CM
PRICE_FACTOR          = 1 - PRICE_DISCOUNT_PCT / 100.0
DRY_RUN               = os.environ.get("dry_run", "false").lower() == "true"
SYNC_ALL              = os.environ.get("sync_all", "false").lower() == "true"
PRODUCT_LIST_RAW      = os.environ.get("product_list", "")
PRODUCTS              = [p.strip() for p in PRODUCT_LIST_RAW.split("\n") if p.strip()]
EXCLUDED_OFFICES_RAW  = os.environ.get("excluded_offices", "").strip().lower()
EXCLUDED_OFFICES      = [e.strip() for e in EXCLUDED_OFFICES_RAW.split(",") if e.strip()]
# v6/v7: sincronización de valores
SYNC_COSTS            = os.environ.get("sync_costs", "true").lower() == "true"
SYNC_PRICES           = os.environ.get("sync_prices", "true").lower() == "true"

# Parámetros de rate limiting
POST_BATCH_SIZE       = 30     # Aplicar pausa cada N POSTs
POST_BATCH_SLEEP      = 3.0    # Segundos de pausa entre bloques de POSTs
GET_PAGE_SLEEP        = 0.25   # Pausa entre páginas en GETs secuenciales
MAX_RETRIES           = 5      # Reintentos máximos por llamada

# ── Helpers sync con backoff exponencial ──────────────────────
def api_get(endpoint, params=None):
    params = {**(params or {})}
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(f"{BASE_URL}{endpoint}", headers=HEADERS,
                             params=params, timeout=45)
            if r.status_code == 429:
                wait = min(60, 5 * 2**attempt)
                print(f"  ⏳ Rate limit (429), esperando {wait}s...", flush=True)
                time.sleep(wait); continue
            if r.status_code == 401:
                wait = 5 * (attempt + 1)
                print(f"  🔑 Token refresh (401), reintento {attempt+1}/{MAX_RETRIES} en {wait}s...", flush=True)
                time.sleep(wait); continue
            if r.status_code in (500, 503):
                wait = 10 * (attempt + 1)
                print(f"  🔄 Error servidor ({r.status_code}), reintento en {wait}s...", flush=True)
                time.sleep(wait); continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.Timeout:
            wait = 10 * (attempt + 1)
            print(f"  ⏱ Timeout, reintento {attempt+1}/{MAX_RETRIES} en {wait}s...", flush=True)
            time.sleep(wait)
        except Exception as e:
            if attempt == MAX_RETRIES - 1: raise
            time.sleep(5 * (attempt + 1))
    return {}

def api_post(endpoint, payload):
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.post(f"{BASE_URL}{endpoint}", headers=HEADERS,
                              json=payload, timeout=30)
            if r.status_code == 429:
                wait = min(60, 10 * 2**attempt)
                print(f"  ⏳ Rate limit POST (429), esperando {wait}s...", flush=True)
                time.sleep(wait); continue
            if r.status_code == 401:
                wait = 5 * (attempt + 1)
                print(f"  🔑 Token refresh POST (401), reintento en {wait}s...", flush=True)
                time.sleep(wait); continue
            if r.status_code in (500, 503):
                time.sleep(10 * (attempt + 1)); continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.Timeout:
            time.sleep(10 * (attempt + 1))
        except Exception as e:
            if attempt == MAX_RETRIES - 1: raise
            time.sleep(5 * (attempt + 1))
    return {}

def api_get_cm(endpoint, params=None):
    """GET contra la empresa Casa Matriz (token separado)."""
    params = {**(params or {})}
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(f"{BASE_URL}{endpoint}", headers=HEADERS_CM,
                             params=params, timeout=45)
            if r.status_code == 429:
                wait = min(60, 5 * 2**attempt)
                print(f"  ⏳ CM Rate limit (429), esperando {wait}s...", flush=True)
                time.sleep(wait); continue
            if r.status_code in (401, 500, 503):
                time.sleep(5 * (attempt + 1)); continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.Timeout:
            time.sleep(10 * (attempt + 1))
        except Exception:
            if attempt == MAX_RETRIES - 1: raise
            time.sleep(5 * (attempt + 1))
    return {}

def load_cm_prices():
    """Descarga SALA DE VENTAS PRECIOS de Casa Matriz y retorna
    dict {barcode -> precio_neto} para cruzar con Distribuidora."""
    if not CM_TOKEN:
        print("  ⚠ BSALE_CM_TOKEN no configurado — no se corregirán precios desde Casa Matriz.")
        return {}
    pls = api_get_cm("/price_lists.json", {"limit": 50}).get("items", [])
    pl  = next((p for p in pls if CM_PRICE_LIST_NAME in p.get("name", "").lower()), None)
    if not pl:
        print(f"  ⚠ Lista '{CM_PRICE_LIST_NAME}' no encontrada en Casa Matriz.")
        return {}
    print(f"  📋 Cargando precios de '{pl['name']}' (Casa Matriz)...", flush=True)
    params = {"limit": 50, "offset": 0, "expand": "[variant]"}
    cm_prices = {}
    while True:
        data  = api_get_cm(f"/price_lists/{pl['id']}/details.json", dict(params))
        batch = data.get("items", [])
        for d in batch:
            v  = d.get("variant") or {}
            bc = (v.get("barCode") or "").strip()
            val = float(d.get("variantValue", 0) or 0)
            if bc and val > 0:
                cm_prices[bc] = val
        params["offset"] += len(batch)
        if not batch or params["offset"] >= data.get("count", 0):
            break
        time.sleep(GET_PAGE_SLEEP)
    print(f"  ✅ {len(cm_prices)} precios cargados desde Casa Matriz")
    return cm_prices

def api_put(endpoint, payload):
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.put(f"{BASE_URL}{endpoint}", headers=HEADERS,
                             json=payload, timeout=30)
            if r.status_code == 429:
                wait = min(60, 10 * 2**attempt)
                print(f"  ⏳ Rate limit PUT (429), esperando {wait}s...", flush=True)
                time.sleep(wait); continue
            if r.status_code == 401:
                wait = 5 * (attempt + 1)
                print(f"  🔑 Token refresh PUT (401), reintento en {wait}s...", flush=True)
                time.sleep(wait); continue
            if r.status_code in (500, 503):
                time.sleep(10 * (attempt + 1)); continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.Timeout:
            time.sleep(10 * (attempt + 1))
        except Exception as e:
            if attempt == MAX_RETRIES - 1: raise
            time.sleep(5 * (attempt + 1))
    return {}

def get_variant_cost(vid):
    """Costo real de una variante para usar en recepciones.
    Prefiere la última recepción con costo > 0: el averageCost global
    queda diluido por el stock virtual de la Distribuidora a costo $0."""
    data = api_get(f"/variants/{vid}/costs.json")
    best_cost, best_date = 0.0, -1
    for h in (data.get("history") or []):
        c = float(h.get("cost", 0) or 0)
        d = int(h.get("admissionDate", 0) or 0)
        if c > 0 and d >= best_date:
            best_cost, best_date = c, d
    if best_cost > 0:
        return best_cost
    return float(data.get("averageCost", 0) or 0)

def fetch_all_pages(endpoint, params=None, label=""):
    params = {**(params or {}), "limit": 50, "offset": 0}
    items = []
    while True:
        data  = api_get(endpoint, dict(params))
        batch = data.get("items", [])
        items.extend(batch)
        total = data.get("count", 0)
        if label and len(items) % 1000 < 50 and len(items) > 0:
            print(f"  → {label}: {len(items)}/{total}", flush=True)
        params["offset"] += len(batch)
        if not batch or len(items) >= total:
            break
        time.sleep(GET_PAGE_SLEEP)
    return items

# ── Descarga async paralela por bodega ────────────────────────
async def fetch_office_async(session, office_id, office_name, semaphore):
    items, offset, limit = [], 0, 50
    async with semaphore:
        while True:
            params = {"officeid": office_id, "limit": limit, "offset": offset}
            for attempt in range(MAX_RETRIES):
                try:
                    async with session.get(
                        f"{BASE_URL}/stocks.json",
                        headers=HEADERS, params=params,
                        timeout=aiohttp.ClientTimeout(total=45)
                    ) as r:
                        if r.status == 429:
                            wait = min(60, 5 * 2**attempt)
                            print(f"  ⏳ Rate limit async '{office_name}', {wait}s...", flush=True)
                            await asyncio.sleep(wait); continue
                        if r.status == 401:
                            await asyncio.sleep(5 * (attempt + 1)); continue
                        data  = await r.json()
                        batch = data.get("items", [])
                        items.extend(batch)
                        total = data.get("count", 0)
                        if not batch or len(items) >= total:
                            print(f"  ✅ '{office_name}': {len(items)} registros", flush=True)
                            return office_id, items
                        offset += len(batch)
                        await asyncio.sleep(0.1)
                        break
                except Exception as e:
                    if attempt == MAX_RETRIES - 1:
                        print(f"  ❌ Error '{office_name}': {e}")
                        return office_id, items
                    await asyncio.sleep(5 * (attempt + 1))
    return office_id, items

async def download_offices_parallel(all_ids, offices):
    semaphore  = asyncio.Semaphore(3)   # Máx 3 descargas simultáneas
    connector  = aiohttp.TCPConnector(limit=8)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            fetch_office_async(
                session, oid,
                next((n for n,o in offices.items() if int(o["id"])==oid), str(oid)),
                semaphore
            )
            for oid in all_ids
        ]
        return dict(await asyncio.gather(*tasks))


# ── Envío de resumen por email ─────────────────────────────────
def send_summary_email(subject, body_html, body_text):
    """Envía email de resumen vía Resend API"""
    resend_key = os.environ.get("RESEND_API_KEY", "")
    to_addr    = os.environ.get("NOTIFY_EMAIL", "zjonathanbeltran@gmail.com")

    if not resend_key:
        print("  ⚠ RESEND_API_KEY no configurado. Email no enviado.")
        return False

    try:
        r = requests.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {resend_key}", "Content-Type": "application/json"},
            json={"from": "Agente Bsale <onboarding@resend.dev>",
                  "to": [to_addr], "subject": subject,
                  "html": body_html, "text": body_text},
            timeout=30
        )
        r.raise_for_status()
        print(f"  ✅ Email enviado a {to_addr}")
        return True
    except Exception as e:
        print(f"  ❌ Error enviando email: {e}")
        return False

# ── Main ──────────────────────────────────────────────────────
def main():
    if not API_TOKEN:
        print("❌ No se encontró BSALE_API_TOKEN. Guárdalo en Settings → Secrets."); return
    if not SYNC_ALL and not PRODUCTS:
        print("❌ Activa 'Sincronizar Todo' o escribe productos en la lista."); return

    t0 = time.time()
    print(f"🚀 Inicio: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(f"{'🔍 DRY RUN (sin cambios)' if DRY_RUN else '⚡ MODO REAL'} | "
          f"{'🗂️  TODO el catálogo' if SYNC_ALL else f'📋 {len(PRODUCTS)} producto(s)'}")
    print("="*60)

    # ── 1. Bodegas ───────────────────────────────────────────
    print("\n📦 Cargando bodegas...")
    raw = api_get("/offices.json", {"limit": 100})
    offices = {o["name"].lower(): o for o in raw.get("items", [])}
    dist    = next((o for n,o in offices.items() if DISTRIBUIDORA_KEYWORD in n), None)
    if not dist:
        print(f"❌ No encontré bodega con '{DISTRIBUIDORA_KEYWORD}'.")
        print(f"   Disponibles: {', '.join(offices.keys())}"); return
    dist_id    = int(dist["id"])
    source_ids = [
        int(o["id"]) for n,o in offices.items()
        if int(o["id"]) != dist_id
        and not any(excl in n for excl in EXCLUDED_OFFICES)
    ]
    excluded_names = [n for n,o in offices.items() if any(excl in n for excl in EXCLUDED_OFFICES)]
    print(f"✅ Distribuidora: '{dist['name']}' (ID {dist_id})")
    if excluded_names:
        print(f"🚫 Bodegas excluidas: {', '.join(excluded_names)}")
    print(f"📂 Bodegas fuente ({len(source_ids)}): "
          f"{', '.join(n for n,o in offices.items() if int(o['id'])!=dist_id and not any(excl in n for excl in EXCLUDED_OFFICES))}")

    # ── 2. Stocks en paralelo ────────────────────────────────
    print(f"\n⚡ Descargando stock de {len(source_ids)+1} bodegas EN PARALELO...")
    all_ids  = source_ids + [dist_id]
    results  = asyncio.run(download_offices_parallel(all_ids, offices))
    stock_map = defaultdict(lambda: defaultdict(float))
    for oid, stocks in results.items():
        for s in stocks:
            vid = int(s.get("variant", {}).get("id", 0) or s.get("variantId", 0))
            qty = float(s.get("quantity", 0))
            if vid: stock_map[vid][oid] = qty
    elapsed_dl = time.time() - t0
    print(f"\n✅ Stocks cargados en {elapsed_dl:.0f}s — {len(stock_map)} variantes únicas")

    # ── 3. Nombres de variantes (expand=product) ─────────────
    print(f"\n🏷️  Cargando nombres de productos...")
    variants_info = {}
    if SYNC_ALL:
        all_v = fetch_all_pages("/variants.json", {"expand": "product"}, label="Variantes")
        for v in all_v:
            prod = v.get("product") or {}
            pname = prod.get("name","") if isinstance(prod, dict) else ""
            vdesc = v.get("description","")
            full  = f"{pname} — {vdesc}".strip(" —") if pname else vdesc
            variants_info[int(v["id"])] = full
        target_ids = list(stock_map.keys())
    else:
        target_ids = []
        for pname in PRODUCTS:
            batch = fetch_all_pages("/variants.json", {"search": pname, "expand": "product"})
            for v in batch:
                vid   = int(v["id"])
                prod  = v.get("product") or {}
                pn    = prod.get("name","") if isinstance(prod,dict) else ""
                vd    = v.get("description","")
                variants_info[vid] = f"{pn} — {vd}".strip(" —") if pn else vd
                target_ids.append(vid)
        target_ids = list(set(target_ids))
    print(f"✅ {len(target_ids)} variantes a procesar")

    # ── 4. Calcular diferencias y aplicar cambios ────────────
    print(f"\n⚙️  Calculando y aplicando cambios...")
    entradas = salidas = sin_cambio = errores_count = 0
    post_count = costos_aplicados = ya_en_cero = 0
    cambios = []; errores = []; stock_cero = []; sin_costo = []
    cost_cache = {}

    for i, vid in enumerate(target_ids, 1):
        try:
            max_stock = max((stock_map[vid].get(oid,0) for oid in source_ids), default=0)
            current   = stock_map[vid].get(dist_id, 0)
            diff      = max_stock - current
            nombre    = variants_info.get(vid, f"Variante ID {vid}")

            if abs(diff) < 0.01:
                sin_cambio += 1
                # Ya estaba en 0 de ejecuciones anteriores: solo contar, no listar
                if max_stock == 0 and current == 0:
                    ya_en_cero += 1
                continue

            accion = "ENTRADA" if diff > 0 else "SALIDA"
            cambios.append({"variante": nombre, "id": vid, "accion": accion,
                            "antes": current, "despues": max_stock, "diff": abs(diff)})
            if max_stock == 0:
                stock_cero.append({"variante": nombre, "id": vid})

            if not DRY_RUN:
                note = f"Sync {datetime.now().strftime('%d/%m/%Y')} — {nombre[:45]}"
                if diff > 0:
                    cost = 0.0
                    if SYNC_COSTS:
                        if vid not in cost_cache:
                            cost_cache[vid] = get_variant_cost(vid)
                        cost = cost_cache[vid]
                        if cost > 0:
                            costos_aplicados += 1
                        else:
                            sin_costo.append({"variante": nombre, "id": vid})
                    api_post("/stocks/receptions.json", {
                        "admissionDate": int(time.time()),
                        "document": "Ingreso por API", "note": note,
                        "officeId": dist_id,
                        "details": [{"quantity": diff, "variantId": vid, "cost": cost}]
                    })
                    entradas += 1
                else:
                    api_post("/stocks/consumptions.json", {
                        "consumptionDate": int(time.time()),
                        "document": "Salida por API", "note": note,
                        "officeId": dist_id,
                        "details": [{"quantity": abs(diff), "variantId": vid}]
                    })
                    salidas += 1
                post_count += 1
                # Pausa cada POST_BATCH_SIZE operaciones
                if post_count % POST_BATCH_SIZE == 0:
                    print(f"  ⏸  Pausa {POST_BATCH_SLEEP}s tras {post_count} cambios...", flush=True)
                    time.sleep(POST_BATCH_SLEEP)
            else:
                if diff > 0: entradas += 1
                else: salidas += 1

            if i % 500 == 0:
                print(f"  → {i}/{len(target_ids)} variantes procesadas...", flush=True)

        except Exception as e:
            errores_count += 1
            errores.append({"variante": variants_info.get(vid, str(vid)), "error": str(e)})

    # ── 5. Verificar y corregir precios $0 (v7: fuente = Casa Matriz) ──
    print(f"\n💰 Verificando precios $0 en lista '{PRICE_LIST_NAME}'...")
    pls = api_get("/price_lists.json", {"limit": 50}).get("items", [])
    pl  = next((p for p in pls if PRICE_LIST_NAME in p.get("name","").lower()), None)
    precio_cero = []; precio_corregido = []
    if pl:
        pl_details = fetch_all_pages(f"/price_lists/{pl['id']}/details.json",
                                     {"expand": "[variant]"}, label="Precios")

        # Cargar precios de Casa Matriz (barcode -> neto) una sola vez
        cm_prices = load_cm_prices() if SYNC_PRICES else {}

        # Construir mapa barcode -> detail_id para la lista Distribuidora
        for d in pl_details:
            if float(d.get("variantValue", 1) or 0) != 0: continue
            v = d.get("variant")
            if not isinstance(v, dict): continue
            vid = int(v.get("id", 0))
            if not vid: continue
            bc = (v.get("barCode") or "").strip()
            if stock_map[vid].get(dist_id, 0) > 0:
                precio_cero.append({
                    "id": vid, "detail_id": d.get("id"),
                    "barcode": bc,
                    "nombre": variants_info.get(vid, f"ID {vid}"),
                    "stock_dist": stock_map[vid].get(dist_id, 0)
                })

        # Corregir usando precio CM * factor (descuento)
        if precio_cero and SYNC_PRICES and cm_prices:
            pendientes = []
            put_count = 0
            for item in precio_cero:
                bc = item.get("barcode", "")
                cm_val = cm_prices.get(bc, 0)
                target = round(cm_val * PRICE_FACTOR) if cm_val > 0 else 0
                if target <= 0 or not item.get("detail_id"):
                    pendientes.append(item); continue
                if DRY_RUN:
                    precio_corregido.append({**item, "precio": target}); continue
                try:
                    # BSale exige el campo id dentro del body, no solo en la URL
                    api_put(f"/price_lists/{pl['id']}/details/{item['detail_id']}.json",
                            {"id": item['detail_id'], "variantValue": target})
                    precio_corregido.append({**item, "precio": target})
                    put_count += 1
                    if put_count % POST_BATCH_SIZE == 0:
                        print(f"  ⏸  Pausa {POST_BATCH_SLEEP}s tras {put_count} precios...", flush=True)
                        time.sleep(POST_BATCH_SLEEP)
                except Exception as e:
                    errores_count += 1
                    errores.append({"variante": item["nombre"], "error": f"precio: {e}"})
                    pendientes.append(item)
            precio_cero = pendientes
            if precio_corregido:
                print(f"  ✅ {len(precio_corregido)} precio(s) "
                      f"{'simulado(s)' if DRY_RUN else 'corregido(s)'} "
                      f"(CM -{PRICE_DISCOUNT_PCT:.0f}%)")
        elif precio_cero and SYNC_PRICES and not cm_prices:
            print("  ⚠ Sin precios de Casa Matriz — configura BSALE_CM_TOKEN y cm_price_list_name.")
    else:
        print(f"  ⚠ Lista '{PRICE_LIST_NAME}' no encontrada.")

    # ── 6. Resumen ───────────────────────────────────────────
    elapsed = time.time() - t0
    print("\n" + "="*60)
    print(f"{'🔍 SIMULACIÓN' if DRY_RUN else '✅ SINCRONIZACIÓN'} COMPLETADA — {elapsed/60:.1f} min")
    print(f"   Variantes procesadas   : {len(target_ids)}")
    print(f"   ✅ Entradas (subida)    : {entradas}")
    print(f"   🔻 Salidas  (bajada)    : {salidas}")
    print(f"   ➡️  Sin cambio           : {sin_cambio}")
    print(f"   💲 Costos aplicados     : {costos_aplicados}")
    print(f"   💰 Precios corregidos   : {len(precio_corregido)}")
    print(f"   ❌ Errores              : {errores_count}")

    if sin_costo:
        print(f"\n⚠️  {len(sin_costo)} entrada(s) sin costo conocido (quedaron a $0):")
        for p in sin_costo[:20]:
            print(f"   • {p['variante']}")
        if len(sin_costo) > 20:
            print(f"   ... y {len(sin_costo)-20} más (ver log)")

    if precio_corregido:
        print(f"\n💰 {len(precio_corregido)} precio(s) {'a corregir (dry run)' if DRY_RUN else 'corregido(s)'}:")
        for p in precio_corregido[:30]:
            print(f"   • {p['nombre']}  → ${p['precio']:,.0f} (neto)")
        if len(precio_corregido) > 30:
            print(f"   ... y {len(precio_corregido)-30} más (ver log)")

    if precio_cero:
        print(f"\n⚠️  {len(precio_cero)} producto(s) con STOCK en Distribuidora pero PRECIO $0 en '{pl['name'] if pl else PRICE_LIST_NAME}':")
        for p in precio_cero[:30]:
            print(f"   • {p['nombre']}  (stock: {p['stock_dist']:.0f})")
        if len(precio_cero) > 30:
            print(f"   ... y {len(precio_cero)-30} más (ver log)")
    else:
        print(f"\n✅ Sin productos con precio $0 en Distribuidora.")

    if stock_cero:
        print(f"\n🔴 {len(stock_cero)} producto(s) APAGADOS HOY (pasaron a stock 0 en esta ejecución):")
        for p in stock_cero[:50]:
            print(f"   • {p['variante']}")
        if len(stock_cero) > 50:
            print(f"   ... y {len(stock_cero)-50} más (ver log)")
    else:
        print(f"\n✅ Ningún producto se apagó hoy.")
    if ya_en_cero:
        print(f"   ℹ️  Además {ya_en_cero} producto(s) siguen en stock 0 de días anteriores (no listados).")

    if errores:
        print(f"\n❌ Primeros errores:")
        for e in errores[:5]:
            print(f"   • {e['variante']}: {e['error']}")

    # ── 7. Guardar log JSON ───────────────────────────────────
    out_dir = os.path.join(os.getcwd(), "files")
    os.makedirs(out_dir, exist_ok=True)
    fname = f"sync_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    log = {
        "fecha": datetime.now().isoformat(), "dry_run": DRY_RUN, "sync_all": SYNC_ALL,
        "elapsed_min": round(elapsed/60, 1), "distribuidora": dist["name"],
        "total_variantes": len(target_ids), "entradas": entradas, "salidas": salidas,
        "sin_cambio": sin_cambio, "errores_count": errores_count,
        "costos_aplicados": costos_aplicados, "sin_costo": sin_costo,
        "precio_corregido": precio_corregido, "ya_en_cero": ya_en_cero,
        "cambios": cambios[:300], "precio_cero": precio_cero, "errores": errores
    }
    with open(os.path.join(out_dir, fname), "w", encoding="utf-8") as f:
        json.dump(log, f, ensure_ascii=False, indent=2)
    print(json.dumps({"type": "generated_file",
                      "path": os.path.join(out_dir, fname), "name": fname}))
    print(f"\n📁 Log guardado: {fname}")
    # ── 8. Enviar resumen por email ───────────────────────────────
    print("\n📧 Enviando resumen por email...")
    modo_str  = "🔍 SIMULACIÓN (Dry Run)" if DRY_RUN else "⚡ Ejecución Real"
    fecha_str = datetime.now().strftime("%d/%m/%Y %H:%M")
    icono     = "⚠️" if errores_count > 0 else "✅"

    # Construir sección de precios corregidos
    if precio_corregido:
        corr_rows_html = "".join(
            f"<tr><td style='padding:6px 12px;border-bottom:1px solid #f0f0f0'>{p['nombre']}</td>"
            f"<td style='padding:6px 12px;border-bottom:1px solid #f0f0f0;text-align:right'>${p['precio']:,.0f}</td></tr>"
            for p in precio_corregido[:50]
        )
        if len(precio_corregido) > 50:
            corr_rows_html += f"<tr><td colspan=2 style='padding:6px 12px;color:#888'>... y {len(precio_corregido)-50} más (ver log)</td></tr>"
        corr_section_html = f"""
        <div style="margin-top:24px">
          <h3 style="color:#38a169;margin-bottom:8px">💰 {len(precio_corregido)} precio(s) {'a corregir (dry run)' if DRY_RUN else 'corregido(s)'}</h3>
          <p style="color:#666;font-size:13px">Casa Matriz '{CM_PRICE_LIST_NAME}' -{PRICE_DISCOUNT_PCT:.0f}% (valor neto).</p>
          <table style="width:100%;border-collapse:collapse;font-size:13px">
            <thead><tr style="background:#f0fff4">
              <th style="padding:8px 12px;text-align:left;color:#38a169">Producto / Variante</th>
              <th style="padding:8px 12px;text-align:right;color:#38a169">Precio neto</th>
            </tr></thead>
            <tbody>{corr_rows_html}</tbody>
          </table>
        </div>"""
        corr_section_text = f"\n💰 {len(precio_corregido)} PRECIOS CORREGIDOS:\n" + "\n".join(
            f"  • {p['nombre']} → ${p['precio']:,.0f}" for p in precio_corregido[:50]
        )
    else:
        corr_section_html = ""
        corr_section_text = ""

    # Construir sección de precio $0
    if precio_cero:
        cero_rows_html = "".join(
            f"<tr><td style='padding:6px 12px;border-bottom:1px solid #f0f0f0'>{p['nombre']}</td>"
            f"<td style='padding:6px 12px;border-bottom:1px solid #f0f0f0;text-align:center'>{p['stock_dist']:.0f}</td></tr>"
            for p in precio_cero[:50]
        )
        if len(precio_cero) > 50:
            cero_rows_html += f"<tr><td colspan=2 style='padding:6px 12px;color:#888'>... y {len(precio_cero)-50} más (ver log)</td></tr>"
        cero_section_html = f"""
        <div style="margin-top:24px">
          <h3 style="color:#e53e3e;margin-bottom:8px">⚠️ {len(precio_cero)} producto(s) con PRECIO $0 en {pl['name'] if pl else PRICE_LIST_NAME}</h3>
          <p style="color:#666;font-size:13px">Estos productos tienen stock en Distribuidora pero precio $0 y no se pudieron corregir (sin precio en la lista base). Pueden venderse sin precio.</p>
          <table style="width:100%;border-collapse:collapse;font-size:13px">
            <thead><tr style="background:#fff5f5">
              <th style="padding:8px 12px;text-align:left;color:#e53e3e">Producto / Variante</th>
              <th style="padding:8px 12px;text-align:center;color:#e53e3e">Stock Dist.</th>
            </tr></thead>
            <tbody>{cero_rows_html}</tbody>
          </table>
        </div>"""
        cero_section_text = f"\n⚠️ {len(precio_cero)} PRODUCTOS CON PRECIO $0:\n" + "\n".join(
            f"  • {p['nombre']} (stock: {p['stock_dist']:.0f})" for p in precio_cero[:50]
        )
    else:
        cero_section_html = "<p style='color:#38a169;margin-top:16px'>✅ Sin productos con precio $0 activos en Distribuidora.</p>"
        cero_section_text = "\n✅ Sin productos con precio $0 en Distribuidora."

    # Construir sección de stock 0
    if stock_cero:
        cero0_rows_html = "".join(
            f"<tr><td style='padding:5px 12px;border-bottom:1px solid #f0f0f0;font-size:13px'>{p['variante']}</td></tr>"
            for p in stock_cero[:100]
        )
        if len(stock_cero) > 100:
            cero0_rows_html += f"<tr><td style='padding:5px 12px;color:#888;font-size:12px'>... y {len(stock_cero)-100} más</td></tr>"
        stock_cero_section_html = f"""
        <div style="margin-top:24px">
          <h3 style="color:#c05621;margin-bottom:8px">🔴 {len(stock_cero)} producto(s) apagados HOY</h3>
          <p style="color:#666;font-size:13px">Pasaron a stock 0 en la Distribuidora en esta ejecución (se agotaron en todas las bodegas físicas).
          Además {ya_en_cero} producto(s) siguen en stock 0 de días anteriores (no listados).</p>
          <table style="width:100%;border-collapse:collapse;font-size:13px">
            <thead><tr style="background:#fff5f0">
              <th style="padding:8px 12px;text-align:left;color:#c05621">Producto / Variante</th>
            </tr></thead>
            <tbody>{cero0_rows_html}</tbody>
          </table>
        </div>"""
        stock_cero_section_text = f"\n🔴 {len(stock_cero)} PRODUCTOS APAGADOS HOY (+{ya_en_cero} ya estaban en 0):\n" + "\n".join(
            f"  • {p['variante']}" for p in stock_cero[:100]
        )
    else:
        stock_cero_section_html = f"<p style='color:#38a169;margin-top:16px'>✅ Ningún producto se apagó hoy ({ya_en_cero} siguen en stock 0 de días anteriores).</p>"
        stock_cero_section_text = f"\n✅ Ningún producto se apagó hoy ({ya_en_cero} siguen en stock 0 de días anteriores)."

    body_html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="font-family:Arial,sans-serif;background:#f7f7f7;padding:20px">
  <div style="max-width:640px;margin:0 auto;background:#fff;border-radius:10px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.08)">
    <div style="background:#2b6cb0;padding:24px 32px">
      <h1 style="color:#fff;margin:0;font-size:20px">📦 Sincronizador de Stock Bsale</h1>
      <p style="color:#bee3f8;margin:4px 0 0;font-size:13px">{fecha_str} · {modo_str}</p>
    </div>
    <div style="padding:28px 32px">
      <table style="width:100%;border-collapse:collapse;font-size:15px">
        <tr style="background:#ebf8ff">
          <td style="padding:10px 14px;font-weight:bold">🗂️ Variantes procesadas</td>
          <td style="padding:10px 14px;text-align:right;font-weight:bold">{len(target_ids)}</td>
        </tr>
        <tr>
          <td style="padding:10px 14px;color:#38a169">✅ Entradas (stock subió)</td>
          <td style="padding:10px 14px;text-align:right;color:#38a169;font-weight:bold">{entradas}</td>
        </tr>
        <tr style="background:#fffaf0">
          <td style="padding:10px 14px;color:#c05621">🔻 Salidas (stock bajó)</td>
          <td style="padding:10px 14px;text-align:right;color:#c05621;font-weight:bold">{salidas}</td>
        </tr>
        <tr>
          <td style="padding:10px 14px;color:#718096">➡️ Sin cambio</td>
          <td style="padding:10px 14px;text-align:right;color:#718096">{sin_cambio}</td>
        </tr>
        <tr style="background:#f0fff4">
          <td style="padding:10px 14px;color:#38a169">💲 Costos aplicados en entradas</td>
          <td style="padding:10px 14px;text-align:right;color:#38a169;font-weight:bold">{costos_aplicados}</td>
        </tr>
        <tr>
          <td style="padding:10px 14px;color:#38a169">💰 Precios corregidos</td>
          <td style="padding:10px 14px;text-align:right;color:#38a169;font-weight:bold">{len(precio_corregido)}</td>
        </tr>
        <tr style="background:#fff5f5">
          <td style="padding:10px 14px;color:#e53e3e">❌ Errores</td>
          <td style="padding:10px 14px;text-align:right;color:#e53e3e;font-weight:bold">{errores_count}</td>
        </tr>
        <tr>
          <td style="padding:10px 14px;color:#718096">⏱️ Tiempo total</td>
          <td style="padding:10px 14px;text-align:right;color:#718096">{elapsed/60:.1f} min</td>
        </tr>
      </table>
      {corr_section_html}
      {cero_section_html}
      {stock_cero_section_html}
    </div>
    <div style="background:#f7fafc;padding:14px 32px;font-size:12px;color:#a0aec0;text-align:center">
      Agente Sincronizador de Stock Bsale · CREAO · Ejecución automática 21:00 hrs
    </div>
  </div>
</body></html>"""

    body_text = f"""SINCRONIZADOR DE STOCK BSALE — {fecha_str}
{modo_str}
{'='*50}
Variantes procesadas : {len(target_ids)}
Entradas (subió)     : {entradas}
Salidas  (bajó)      : {salidas}
Sin cambio           : {sin_cambio}
Costos aplicados     : {costos_aplicados}
Precios corregidos   : {len(precio_corregido)}
Errores              : {errores_count}
Tiempo total         : {elapsed/60:.1f} min
{corr_section_text}
{cero_section_text}
{stock_cero_section_text}
"""
    subject = (f"{icono} Stock Bsale {fecha_str} — {entradas}↑ {salidas}↓ "
               f"{len(precio_corregido)}💰 {len(stock_cero)}🔴hoy")
    send_summary_email(subject, body_html, body_text)


if __name__ == "__main__":
    main()
