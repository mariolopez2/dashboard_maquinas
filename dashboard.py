# -*- coding: utf-8 -*-
import io
import time
import json
import requests
import pandas as pd
import math
import os, uuid

import streamlit as st
from datetime import datetime, date
from typing import Dict, List
from copy import deepcopy
from streamlit_drawable_canvas import st_canvas

# =========================
# Configuración y helpers
# =========================

DATA_DIR   = os.path.join(os.getcwd(), "offline_queue")
QUEUE_FILE = os.path.join(DATA_DIR, "queue.json")
os.makedirs(DATA_DIR, exist_ok=True)

def _queue_load():
    if os.path.exists(QUEUE_FILE):
        try:
            with open(QUEUE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []
    return []

def _queue_save(q):
    with open(QUEUE_FILE, "w", encoding="utf-8") as f:
        json.dump(q, f, ensure_ascii=False, indent=2)

def queue_add(entry: dict):
    q = _queue_load()
    q.append(entry)
    _queue_save(q)

def queue_count() -> int:
    return len(_queue_load())

def _save_signature_local(content: bytes) -> str:
    fn = os.path.join(DATA_DIR, f"firma_{uuid.uuid4().hex}.png")
    with open(fn, "wb") as f:
        f.write(content)
    return fn

def sync_queue(settings, token):
    """Intenta subir todo lo pendiente. Devuelve (subidos, pendientes)."""
    from copy import deepcopy
    q = _queue_load()
    remaining = []
    uploaded = 0

    for entry in q:
        try:
            et = entry.get("type")
            if et == "list_add":
                # subir directo
                base = site_base(settings)
                lid  = _list_id_for(settings, token, entry["list_title"])
                url  = f"{base}/lists/{lid}/items"
                graph_post_json(url, token, {"fields": entry["fields"]})
                uploaded += 1

            elif et == "list_update":
                base = site_base(settings)
                lid  = _list_id_for(settings, token, entry["list_title"])
                url  = f"{base}/lists/{lid}/items/{entry['item_id']}/fields"
                graph_patch_json(url, token, entry["fields"])
                uploaded += 1

            elif et == "capture_batch":
                # 1) subir firma
                with open(entry["signature_file"], "rb") as f:
                    sig = f.read()
                fname = os.path.basename(entry["signature_file"])
                firma_url = upload_signature_png(settings, token, fname, sig)
                # 2) crear capturas
                for it in entry["items"]:
                    fields = deepcopy(it["fields"])
                    fields["firma_url"] = firma_url
                    base = site_base(settings)
                    lid  = _list_id_for(settings, token, "Capturas")
                    url  = f"{base}/lists/{lid}/items"
                    graph_post_json(url, token, {"fields": fields})
                uploaded += 1

            else:
                # tipo desconocido → lo dejamos
                remaining.append(entry)

        except Exception:
            # si falla, se queda en la cola
            remaining.append(entry)

    _queue_save(remaining)
    return uploaded, len(remaining)

def refresh_data():
    st.cache_data.clear()
    st.rerun()

def _rutas_maps(rutas_df: pd.DataFrame):
    """Devuelve (route_names, name2id, id2name) normalizando IDs como string entero."""
    df = rutas_df.copy()
    if "id_ruta" in df.columns:
        df["id_ruta"] = pd.to_numeric(df["id_ruta"], errors="coerce").fillna(0).astype(int).astype(str)
    df["nombre"] = df.get("nombre", "").astype(str).fillna("")
    id2name = {}
    for _, r in df.iterrows():
        rid = r.get("id_ruta", "")
        nm  = (r.get("nombre", "") or "").strip() or f"Ruta {rid}"
        id2name[rid] = nm
    name2id = {v:k for k,v in id2name.items()}
    route_names = list(name2id.keys())
    return route_names, name2id, id2name


def _scol(df: pd.DataFrame, name: str, default=""):
    """Serie segura: si la columna no existe, devuelve una serie con default."""
    if name in df.columns:
        return df[name]
    return pd.Series([default]*len(df), index=df.index)

st.set_page_config(page_title="Dashboard - Nexus Solutions", page_icon="🎰", layout="wide")

PRIMARY_COLOR = "#161F35"

def _get_settings():
    graph = st.secrets.get("graph", {})
    app = st.secrets.get("app", {})
    return {
        "tenant_id": (graph.get("tenant_id","") or "").strip(),
        "client_id": (graph.get("client_id","") or "").strip(),
        "client_secret": (graph.get("client_secret","") or "").strip(),
        "hostname": graph.get("hostname","").strip(),
        "site_path": graph.get("site_path","").strip(),
        "firmas_library_path": graph.get("firmas_library_path", "/Shared Documents/Firmas"),
        "authority": (graph.get("authority","") or "").strip(),
        "tenant_name": (graph.get("tenant_name","") or "").strip(),
        "redirect_uri": app.get("redirect_uri", "http://localhost:8501/redirect"),
        "admin_group": app.get("admin_group", "MES-Admin"),
        "operator_group": app.get("operator_group", "MES-Operador"),
    }

# =========================
# Autenticación (MSAL SSO)
# =========================

AUTH_SCOPES = [
    "User.Read",
    "GroupMember.Read.All",
    "Sites.ReadWrite.All",
    "Files.ReadWrite.All",
]

def msal_app(settings):
    import msal

    # Construye la authority priorizando 'authority' explícita,
    # luego tenant_name y por último tenant_id.
    auth = (settings.get("authority") or "").strip()
    if not auth:
        tenant_name = (settings.get("tenant_name") or "").strip()
        if tenant_name:
            auth = f"https://login.microsoftonline.com/{tenant_name}"
        else:
            tid = (settings.get("tenant_id") or "").strip()
            auth = f"https://login.microsoftonline.com/{tid}"

    # Valida que la metadata OIDC exista (evita el ValueError de MSAL con mensaje críptico)
    oidc = auth.rstrip("/") + "/v2.0/.well-known/openid-configuration"
    try:
        r = requests.get(oidc, timeout=10)
        r.raise_for_status()
    except Exception as e:
        st.error(f"Authority inválida o no accesible: {oidc}\n{e}")
        # Recomendación visual para depurar rápido:
        st.code(f"authority = {auth!r}", language="text")
        raise

    return msal.ConfidentialClientApplication(
        client_id=settings["client_id"],
        authority=auth,
        client_credential=settings["client_secret"],
    )

def login_sso(settings) -> dict:
    """
    Maneja el flujo Authorization Code en Streamlit.
    Devuelve el token dict (con access_token y expires_at) en session_state["token"].
    """
    if "token" in st.session_state:
        tok = st.session_state["token"]
        if tok and tok.get("expires_at", 0) > time.time() + 60:
            return tok

    app = msal_app(settings)

    qp = st.query_params
    if "code" not in qp:
        # Mostrar botón para ir a login de Microsoft
        auth_url = app.get_authorization_request_url(AUTH_SCOPES, redirect_uri=settings["redirect_uri"])
        st.markdown(
            f'<a href="{auth_url}" target="_self" style="display:block;background:{PRIMARY_COLOR};'
            'color:white;text-align:center;padding:12px;border-radius:8px;text-decoration:none;font-weight:600">'
            'Iniciar sesión con Microsoft</a>',
            unsafe_allow_html=True,
        )
        st.stop()

    code = qp["code"]
    result = app.acquire_token_by_authorization_code(code, scopes=AUTH_SCOPES, redirect_uri=settings["redirect_uri"])
    if "access_token" not in result:
        st.error(f"Login falló: {result.get('error')}: {result.get('error_description')}")
        st.stop()

    result["expires_at"] = time.time() + result.get("expires_in", 3600)
    st.session_state["token"] = result

    # limpiar query params y refrescar
    st.query_params.clear()
    refresh_data()

# ====== utilidades para listas ======
def get_list_id_by_name(settings, token, list_title: str) -> str | None:
    base = site_base(settings)
    url = f"{base}/lists?$filter=displayName eq '{list_title}'&$select=id,displayName"
    data = graph_get(url, token)
    vals = data.get("value", [])
    return vals[0]["id"] if vals else None

def ensure_list_with_columns(settings, token, list_title: str, columns: list[dict]):
    base = site_base(settings)

    # 1) crear la lista si no existe
    list_id = get_list_id_by_name(settings, token, list_title)
    if not list_id:
        created = graph_post_json(f"{base}/lists", token, {
            "displayName": list_title,
            "list": {"template": "genericList"},
        })
        list_id = created["id"]

    # 2) leer columnas existentes (¡sin dataType!)
    try:
        existing = graph_get(
            f"{base}/lists/{list_id}/columns?$select=id,name,required", token
        ).get("value", [])
    except Exception:
        # fallback sin $select por si algún tenant se pone dificil
        existing = graph_get(f"{base}/lists/{list_id}/columns", token).get("value", [])

    existing_names = {c.get("name") for c in existing if c.get("name")}

    # 3) sanitizar definiciones antes de crearlas
    def _sanitize(col: dict) -> dict:
        col = deepcopy(col)
        if "number" in col:
            col["number"].pop("decimalPlaces", None)
        if "choice" in col:
            ch = col["choice"]
            if "choices" in ch:
                if isinstance(ch["choices"], (set, tuple)):
                    ch["choices"] = list(ch["choices"])
                ch["choices"] = [str(x) for x in ch["choices"]]
            ch.setdefault("allowTextEntry", True)
        return col

    # 4) crear sólo las que falten
    for col in columns:
        name = col.get("name")
        if not name or name in existing_names:
            continue
        safe_col = _sanitize(col)
        graph_post_json(f"{base}/lists/{list_id}/columns", token, safe_col)

    return list_id



def add_or_update_by_field(settings, token, list_title: str, match_field: str, row: dict):
    """Upsert por un campo (numérico o texto)."""
    base = site_base(settings)                       # -> https://graph.microsoft.com/v1.0/sites/{site-id}
    lid  = _list_id_for(settings, token, list_title) # -> GUID de la lista (cacheado)

    # valor con el que vamos a buscar coincidencia
    val = row.get(match_field)

    def _try_filter(expr: str):
        # Siempre usamos el GUID de la lista
        url = f"{base}/lists/{lid}/items?$filter={expr}&expand=fields&$top=1"
        return graph_get(url, token).get("value", [])

    # Construimos el filtro según tipo (numérico vs texto)
    items = []
    if val is not None and val != "":
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            expr = f"fields/{match_field} eq {val}"
        else:
            escaped = str(val).replace("'", "''")  # OData: duplicar comillas simples
            expr = f"fields/{match_field} eq '{escaped}'"
        items = _try_filter(expr)

    # Si existe -> update; si no -> insert
    if items:
        item_id = items[0]["id"]
        return list_update(settings, token, list_title, item_id, row)
    else:
        return list_add(settings, token, list_title, row)

NUMERIC_FIELDS = {
    "id_ruta","id_cliente","id_maquina","comision","fondo",
    "score","valor_real","ganancia_cliente","ganancia_owner","id_captura"
}

def to_int_or_none(v):
    """Convierte '1', '1.0', 1, 1.0 → 1. Si está vacío o no es numérico, devuelve None."""
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    try:
        # permite '1.0' y similares
        return int(float(s))
    except Exception:
        return None


def normalize_fields_for_sp(list_title: str, row: dict) -> dict:
    f = dict(row)

    # Teléfono como texto
    if "telefono" in f and isinstance(f["telefono"], (int, float)):
        try:
            f["telefono"] = str(int(f["telefono"]))
        except Exception:
            f["telefono"] = str(f["telefono"])

    # Números → None cuando estén vacíos / NaN; enteros cuando aplique
    for k in list(f.keys()):
        v = f[k]
        if k in NUMERIC_FIELDS:
            if v in ("", None) or (isinstance(v, float) and math.isnan(v)):
                f[k] = None
            elif isinstance(v, float) and v.is_integer():
                f[k] = int(v)

    # ===== Ajustes específicos por lista =====
    if list_title == "Maquinas":
        # Textos: asegúrate de enviar strings
        for k in ("numero_maquina", "numero_permiso", "tipo_maquina"):
            if k in f and f[k] is not None:
                f[k] = "" if f[k] == "" else str(f[k])

        # id_cliente == 0 → None (sin cliente)
        if str(f.get("id_cliente", "")).strip() in {"0", ""}:
            f["id_cliente"] = None

        # Derivar 'estatus' válido (choice) si viene algo no permitido (p.ej. "activo")
        allowed = {"libre", "asignada", "inactiva"}
        est_in = str(f.get("estatus", "")).strip().lower()
        if est_in not in allowed:
            # si hay cliente → asignada, si no → libre
            f["estatus"] = "asignada" if f.get("id_cliente") not in (None, "", 0, "0") else "libre"

        # Derivar 'activa' si no viene: por defecto activa salvo que 'inactiva'
        if not f.get("activa"):
            f["activa"] = "inactiva" if f.get("estatus") == "inactiva" else "activa"

    # Title por defecto
    if "Title" not in f or not f["Title"]:
        if list_title == "Rutas":
            f["Title"] = f.get("nombre") or f"Ruta {f.get('id_ruta','')}"
        elif list_title == "Clientes":
            base = (f.get("nombre") or "").strip()
            ap   = (f.get("apellido") or "").strip()
            f["Title"] = (base + (" " + ap if ap else "")).strip() or f"Cliente {f.get('id_cliente','')}"
        elif list_title == "Maquinas":
            f["Title"] = f"M{f.get('numero_maquina','')}"
        else:
            f["Title"] = f"{list_title}"

    # Choice vacíos → string vacío (no None)
    if "estatus" in f and f["estatus"] is None:
        f["estatus"] = ""

    return f


# ====== página de migración ======
def page_migrar_excel(settings, token):
    st.header("Migrar datos desde Excel a Listas de SharePoint")
    st.caption("Sube el Excel original (con hojas: rutas, clientes, maquinas, capturas, bitacora, usuariosextras).")

    up = st.file_uploader("Excel .xlsx", type=["xlsx"])
    overwrite = st.checkbox("Actualizar (upsert) por ID si ya existe", value=True)
    if not up:
        return

    # 1) Definir columnas de cada lista (tipos simples)
    rutas_cols = [
        {"name":"id_ruta","number":{}},
        {"name":"nombre","text":{}},
        {"name":"estatus","choice":{"choices":["activo","inactivo"],"allowTextEntry":True}},
    ]

    clientes_cols = [
        {"name":"id_cliente","number":{}},
        {"name":"nombre","text":{}},{"name":"apellido","text":{}},
        {"name":"direccion","text":{}},{"name":"telefono","text":{}},{"name":"correo","text":{}},
        {"name":"comision","number":{}},{"name":"id_ruta","number":{}},
        {"name":"estatus","choice":{"choices":["activo","inactivo"],"allowTextEntry":True}},
    ]
    maquinas_cols = [
        {"name":"id_maquina","number":{}},
        {"name":"numero_maquina","text":{}},{"name":"numero_permiso","text":{}},
        {"name":"tipo_maquina","text":{}},{"name":"fondo","number":{}},
        {"name":"id_ruta","number":{}},{"name":"id_cliente","number":{}},
        {"name":"estatus","choice":{"choices":["libre","asignada","inactiva"],"allowTextEntry":True}},
        {"name":"activa","choice":{"choices":["activa","inactiva"],"allowTextEntry":True}},
    ]
    capturas_cols = [
        {"name":"id_captura","number":{}},
        {"name":"fecha","dateTime":{"format":"dateTime"}},{"name":"user_upn","text":{}},
        {"name":"id_ruta","number":{}},{"name":"id_cliente","number":{}},
        {"name":"id_maquina","number":{}},{"name":"score","number":{}},
        {"name":"valor_real","number":{}},{"name":"comision_cliente","number":{}},
        {"name":"ganancia_cliente","number":{}},{"name":"ganancia_owner","number":{}},
        {"name":"observaciones","text":{}},{"name":"firma_url","text":{}},
    ]
    bitacora_cols = [
        {"name":"fecha_hora","dateTime":{"format":"dateTime"}},{"name":"user_upn","text":{}},
        {"name":"modulo","text":{}},{"name":"accion","text":{}},{"name":"detalle","text":{}},
    ]
    uextras_cols = [
        {"name":"user_upn","text":{}},{"name":"id_ruta","number":{"decimalPlaces":0}},
    ]

    mapping = {
        "Rutas":        ("rutas", rutas_cols, "id_ruta"),
        "Clientes":     ("clientes", clientes_cols, "id_cliente"),
        "Maquinas":     ("maquinas", maquinas_cols, "id_maquina"),
        "Capturas":     ("capturas", capturas_cols, "id_captura"),
        "Bitacora":     ("bitacora", bitacora_cols, "fecha_hora"),       
        "UsuariosExtras": ("usuariosextras", uextras_cols, "user_upn"),
    }

    xls = pd.ExcelFile(up)
    total = 0
    for list_title, (sheet_name, cols, key_field) in mapping.items():
        if sheet_name not in xls.sheet_names:
            st.warning(f"No encontré hoja '{sheet_name}', salto {list_title}.")
            continue
        df = pd.read_excel(xls, sheet_name=sheet_name).fillna("")
        # 2) asegurar lista y columnas
        list_id = ensure_list_with_columns(settings, token, list_title, cols)
        st.write(f"Lista **{list_title}** lista (id {list_id}). Importando {len(df)} filas…")
        # 3) importar filas
        ok = 0
        for _, row in df.iterrows():
            fields = {k: (None if (isinstance(v, float) and pd.isna(v)) else v) for k, v in row.to_dict().items()}
            try:
                fields = normalize_fields_for_sp(list_title, row.to_dict())
                if overwrite and key_field in fields:
                    add_or_update_by_field(settings, token, list_title, key_field, fields)
                else:
                    list_add(settings, token, list_title, fields)
                ok += 1
            except Exception as e:
                st.warning(f"{list_title}: error con fila {row.to_dict()}: {e}")
        total += ok
        st.success(f"{list_title}: {ok} filas migradas.")

    st.success(f"¡Migración terminada! Total migrado: {total} filas.")


def graph_get(url: str, token: dict):
    headers = {"Authorization": f"Bearer {token['access_token']}"}
    if "$filter=" in url:
        headers["Prefer"] = "HonorNonIndexedQueriesWarningMayFailRandomly"
    r = requests.get(url, headers=headers, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"GET {url} -> {r.status_code}: {r.text[:300]}")
    return r.json()



def _raise_for_graph(method: str, url: str, r: requests.Response):
    try:
        data = r.json()
    except Exception:
        data = {"text": r.text[:500]}
    msg = data.get("error", {}).get("message") or data.get("text") or ""
    inner = data.get("error", {}).get("innerError", {})
    detalle = f"{msg} | inner: {inner}" if inner else msg
    raise RuntimeError(f"{method} {url} -> {r.status_code}: {detalle[:500]}")

def graph_post_json(url: str, token: dict, body: dict):
    h = {"Authorization": f"Bearer {token['access_token']}", "Content-Type": "application/json"}
    r = requests.post(url, headers=h, json=body, timeout=60)
    if r.status_code >= 400: _raise_for_graph("POST", url, r)
    return r.json()

def graph_patch_json(url: str, token: dict, body: dict):
    h = {"Authorization": f"Bearer {token['access_token']}", "Content-Type": "application/json"}
    r = requests.patch(url, headers=h, json=body, timeout=60)
    if r.status_code >= 400: _raise_for_graph("PATCH", url, r)
    return r.json()

# --- Cache de list-ids por nombre ---
def _list_id_for(settings, token, list_title: str) -> str:
    cache = st.session_state.setdefault("list_id_cache", {})
    if list_title in cache:
        return cache[list_title]
    base = site_base(settings)  # <-- ya resuelve sites/{site-id}
    title = list_title.replace("'", "''")
    url = f"{base}/lists?$filter=displayName eq '{title}'&$select=id,displayName"
    data = graph_get(url, token)
    vals = data.get("value", [])
    if not vals:
        raise RuntimeError(f"La lista '{list_title}' no existe en el sitio.")
    lid = vals[0]["id"]
    cache[list_title] = lid
    return lid

def list_items_all(settings, token, list_title: str, select="*", expand_fields=True) -> list[dict]:
    base = site_base(settings)
    lid = _list_id_for(settings, token, list_title)
    url = f"{base}/lists/{lid}/items"
    if expand_fields:
        url += "?expand=fields"
        if select and select != "*":
            url += f"&select={select}"
    items = []
    while url:
        data = graph_get(url, token)
        items += data.get("value", [])
        url = data.get("@odata.nextLink", "")
    return items

def list_add(settings, token, list_title: str, fields: dict) -> dict:
    base = site_base(settings)
    lid  = _list_id_for(settings, token, list_title)
    url  = f"{base}/lists/{lid}/items"
    try:
        return graph_post_json(url, token, {"fields": fields})
    except Exception as e:
        if st.session_state.get("offline_mode", False):
            queue_add({"type": "list_add", "list_title": list_title, "fields": fields})
            st.info(f"{list_title}: sin conexión, agregado a la cola local.")
            return {"queued": True}
        raise

def list_update(settings, token, list_title: str, item_id: str, fields: dict) -> dict:
    base = site_base(settings)
    lid  = _list_id_for(settings, token, list_title)
    url  = f"{base}/lists/{lid}/items/{item_id}/fields"
    try:
        return graph_patch_json(url, token, fields)
    except Exception as e:
        if st.session_state.get("offline_mode", False):
            queue_add({"type": "list_update", "list_title": list_title, "item_id": item_id, "fields": fields})
            st.info(f"{list_title}: actualización en cola local.")
            return {"queued": True}
        raise


def _resolve_site_id(settings, token) -> str:
    """
    Devuelve el site_id real del sitio de SharePoint.
    - Primero intenta resolver por path exacto (hostname + site_path).
    - Si falla, hace búsqueda por nombre y elige el más cercano.
    Cachea el resultado en st.session_state['site_id'].
    """
    if "site_id" in st.session_state and st.session_state["site_id"]:
        return st.session_state["site_id"]

    host = settings["hostname"].strip()
    path = settings["site_path"].strip()

    # 1) Intento directo por path
    try:
        url = f"https://graph.microsoft.com/v1.0/sites/{host}:{path}?$select=id,webUrl,displayName"
        data = graph_get(url, token)
        sid = data["id"]
        st.session_state["site_id"] = sid
        return sid
    except Exception:
        pass  

    # 2) Búsqueda por nombre
    #   toma el último segmento del path como "nombre" 
    name = path.split("/")[-1]
    try:
        search_url = f"https://graph.microsoft.com/v1.0/sites/{host}/sites?search={name}&$select=id,webUrl,displayName"
        res = graph_get(search_url, token)
        candidates = res.get("value", [])
        # Elegimos el que termine con el path (si hay) o el primero
        chosen = None
        for c in candidates:
            web = (c.get("webUrl") or "").lower().replace("%20", "-")
            if web.endswith(path.lower().replace("%20", "-").replace(" ", "-")):
                chosen = c; break
        if not chosen and candidates:
            chosen = candidates[0]

        if not chosen:
            raise RuntimeError(f"No pude encontrar el sitio '{path}' en {host}. Revisa secrets hostname/site_path.")

        sid = chosen["id"]
        st.session_state["site_id"] = sid
        return sid
    except Exception as e:
        # Mensaje útil para depurar
        st.error(f"No se pudo resolver el site_id. host='{host}', path='{path}'. Detalle: {e}")
        raise


def site_base(settings):
    """
    Base robusta: usa sites/{site_id} en lugar de sites/{hostname}:{path}.
    """
    token = st.session_state.get("token")
    if not token:
        raise RuntimeError("site_base: falta token en session_state (llama después del login).")
    sid = _resolve_site_id(settings, token)
    return f"https://graph.microsoft.com/v1.0/sites/{sid}"

def items_to_df(items: List[dict]) -> pd.DataFrame:
    rows = []
    for it in items:
        f = it.get("fields", {})
        # inyecta SharePoint itemId si hace falta
        f["_sp_item_id"] = it.get("id")
        rows.append(f)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).fillna("")

def upload_signature_png(settings, token, filename: str, content: bytes) -> str:
    """
    Sube la firma al drive del sitio (carpeta configurada) y devuelve el webUrl.
    """
    base = site_base(settings)  # -> https://graph.microsoft.com/v1.0/sites/{site-id}
    path = (settings.get("firmas_library_path", "/Shared Documents/Firmas") or "").rstrip("/")
    put_url = f"{base}/drive/root:{path}/{filename}:/content"  # <-- OJO: /drive/root: (sin ':' antes de /drive)

    h = {"Authorization": f"Bearer {token['access_token']}"}
    r = requests.put(put_url, headers=h, data=content, timeout=60)

    if 200 <= r.status_code < 300:
        return r.json().get("webUrl", "")

    # Deja el warning pero no revienta el flujo de captura.
    st.warning(f"Error subiendo firma: {r.status_code} {r.text[:300]}")
    return ""


def next_id_from_df(df: pd.DataFrame, id_col: str) -> int:
    if df.empty or id_col not in df.columns or df[id_col].dropna().empty:
        return 1
    try:
        return int(pd.to_numeric(df[id_col], errors="coerce").max()) + 1
    except Exception:
        return 1

def user_context(token: dict, settings: dict) -> dict:
    me = graph_get("https://graph.microsoft.com/v1.0/me", token)
    upn = me.get("userPrincipalName", "")
    groups = graph_get("https://graph.microsoft.com/v1.0/me/memberOf?$select=displayName", token)
    names = [g.get("displayName", "") for g in groups.get("value", [])]
    rol = "admin" if settings["admin_group"] in names else ("operador" if settings["operator_group"] in names else "operador")

    # ruta asignada (UsuariosExtras)
    id_ruta = ""
    try:
        ue = list_items_all(settings, token, "UsuariosExtras")
        ue_df = items_to_df(ue)
        if not ue_df.empty and "user_upn" in ue_df.columns and "id_ruta" in ue_df.columns:
            fila = ue_df.loc[ue_df["user_upn"].str.lower() == upn.lower()]
            if not fila.empty:
                val = fila["id_ruta"].iloc[0]
                # 1, "1", 1.0, "1.0"  ->  "1"
                id_ruta = str(int(float(val))) if str(val) != "" else ""
    except Exception:
        id_ruta = ""

    return {"upn": upn, "rol": rol, "id_ruta": id_ruta, "groups": names}


# =========================
# Carga inicial de listas
# =========================

@st.cache_data(show_spinner=False, ttl=15)  # se refresca solo cada 15s
def load_all_lists(settings, token) -> Dict[str, pd.DataFrame]:
    dfs: Dict[str, pd.DataFrame] = {}
    for name in ["Rutas", "Clientes", "Maquinas", "Capturas", "Bitacora", "UsuariosExtras"]:
        try:
            items = list_items_all(settings, token, name)
            dfs[name.lower()] = items_to_df(items)
        except Exception as e:
            st.error(f"Error cargando lista '{name}': {e}")
            dfs[name.lower()] = pd.DataFrame()
    return dfs

def log_action_remote(settings, token, modulo: str, accion: str, detalle: str, user_upn: str):
    try:
        list_add(settings, token, "Bitacora", {
            "fecha_hora": datetime.now().isoformat(), 
            "user_upn": user_upn,
            "modulo": modulo,
            "accion": accion,
            "detalle": (detalle or "")[:255],
            "Title": f"{modulo}:{accion}", 
        })
    except Exception as e:
        st.warning(f"No se pudo registrar en bitácora: {e}")

# =========================
# UI comunes
# =========================

def show_menu_for(user):
    rol = (user.get("rol") or "").lower()
    opciones = []
    if rol in ("admin", "operador"):
        opciones.append("Capturar Datos")
    if rol == "admin":
        opciones += ["Reportes", "Clientes", "Maquinas", "Rutas", "Usuarios", "Migrar desde Excel"]
    opciones.append("Salir")
    choice = st.selectbox("Menú", opciones, index=0)
    return choice

# =========================
# Páginas
# =========================

def page_capturas(dfs, settings, user, token):
    st.header("Capturar Datos")

    rutas_df    = dfs.get("rutas", pd.DataFrame()).copy()
    clientes_df = dfs.get("clientes", pd.DataFrame()).copy()
    maquinas_df = dfs.get("maquinas", pd.DataFrame()).copy()

    # Normaliza IDs a texto entero
    for df, cols in ((rutas_df, ["id_ruta"]),
                     (clientes_df, ["id_cliente", "id_ruta"]),
                     (maquinas_df, ["id_maquina", "id_cliente", "id_ruta"])):
        for c in cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int).astype(str)

    # --- Operador: filtra por ruta asignada ---
    assigned_route_raw = str(user.get("id_ruta") or "").strip()
    assigned_route = str(int(float(assigned_route_raw))) if assigned_route_raw else ""

    if (user.get("rol") == "operador") and assigned_route:
        rutas_df = rutas_df.loc[pd.to_numeric(rutas_df["id_ruta"], errors="coerce").fillna(0).astype(int).astype(str) == assigned_route]
        # Mostrar nombre, no ID
        st.caption(f"Ruta asignada: {rutas_df['nombre'].iloc[0] if not rutas_df.empty else assigned_route}")

    # --- Selector por NOMBRE ---
    route_names, name2id, id2name = _rutas_maps(rutas_df)
    if route_names:
        default_name = id2name.get(assigned_route, route_names[0])
        ruta_name_sel = st.selectbox("Ruta", route_names, index=route_names.index(default_name) if default_name in route_names else 0)
        ruta_sel = name2id.get(ruta_name_sel, "")
    else:
        ruta_name_sel = st.selectbox("Ruta", [])
        ruta_sel = ""

    # --- Clientes por ruta ---
    if ruta_sel and not clientes_df.empty:
        mask_cli = (clientes_df["id_ruta"].astype(str) == str(ruta_sel)) & (
            _scol(clientes_df, "estatus", "activo").astype(str).str.lower() != "inactivo"
        )
        clientes_sub = clientes_df.loc[mask_cli].copy()
    else:
        clientes_sub = pd.DataFrame()

    cliente_sel = None
    if not clientes_sub.empty:
        cliente_opts = (clientes_sub["id_cliente"].astype(str) + " - " + clientes_sub["nombre"].astype(str)).tolist()
        cliente_sel = st.selectbox("Cliente", cliente_opts)

    # --- Máquinas del cliente ---
    maquinas_sub = pd.DataFrame()
    id_cliente = None
    if cliente_sel:
        id_cliente = cliente_sel.split(" - ")[0]
        for c in ("id_cliente", "id_maquina", "numero_maquina", "estatus"):
            if c not in maquinas_df.columns:
                maquinas_df[c] = ""
        mask_maq = (maquinas_df["id_cliente"].astype(str) == str(id_cliente)) & (
            maquinas_df["estatus"].astype(str).str.lower() != "inactiva"
        )
        maquinas_sub = maquinas_df.loc[mask_maq, ["id_maquina", "numero_maquina"]].copy().sort_values("numero_maquina")

    if maquinas_sub.empty and cliente_sel:
        st.info("Este cliente no tiene máquinas asignadas activas.")
        return

    # --- Inputs por máquina ---
    entradas = []
    if not maquinas_sub.empty:
        st.subheader("Detalle por máquina")
        for _, m in maquinas_sub.iterrows():
            mid = str(m["id_maquina"])
            cols = st.columns([1, 2, 2, 3])
            with cols[0]:
                st.markdown(f"**Maq {m['numero_maquina']}** (ID {mid})")
            with cols[1]:
                score = st.number_input("Score", min_value=0, step=1, value=0, key=f"sc_{mid}")
            with cols[2]:
                valor = st.number_input("Valor real ($)", min_value=0.0, step=1.0, value=0.0, format="%.2f", key=f"vr_{mid}")
            with cols[3]:
                obs = st.text_input("Observaciones", "", key=f"obs_{mid}")
            entradas.append({"id_maquina": mid, "numero": m["numero_maquina"], "score": score, "valor_real": valor, "obs": obs})

    # Comisión del cliente
    comision = 40.0
    if cliente_sel and not clientes_df.empty:
        fila_c = clientes_df.loc[clientes_df["id_cliente"].astype(str) == id_cliente]
        if not fila_c.empty and "comision" in fila_c.columns:
            try:
                comision = float(fila_c["comision"].iloc[0])
            except Exception:
                pass

    # Totales estimados (sobre lo capturado hasta el momento)
    seleccionadas = [e for e in entradas if (e["score"] or e["valor_real"])]
    tot_valor   = sum(e["valor_real"] for e in seleccionadas)
    tot_cli     = round(tot_valor * (comision / 100.0), 2)
    tot_owner   = round(tot_valor - tot_cli, 2)

    st.subheader("Resumen")
    st.write(f"Máquinas con captura: **{len(seleccionadas)}**")
    st.write(f"Comisión cliente: **{comision:.1f}%**")
    st.write(f"Ganancia total: **{tot_valor:,.2f}**")
    st.write(f"Ganancia cliente: **${tot_cli:,.2f}**")
    #st.write(f"Ganancia propietario: **${tot_owner:,.2f}**")

    st.markdown("#### Firma del cliente (una sola para el corte)")
    canvas = st_canvas(
        fill_color="rgba(0,0,0,0)",
        stroke_width=2,
        stroke_color="#000000",
        background_color="#FFFFFF",
        height=180,
        width=400,
        drawing_mode="freedraw",
        key="firma_multi",
    )

    if st.button("Confirmar y Guardar", use_container_width=True):
        if not ruta_sel or not cliente_sel:
            st.error("Selecciona Ruta y Cliente.")
            return
        if not seleccionadas:
            st.error("Captura al menos una máquina con Score o Valor real.")
            return
        if not canvas or canvas.image_data is None:
            st.error("Captura la firma del cliente.")
            return

        # Bytes de firma
        from PIL import Image
        img = Image.fromarray((canvas.image_data[:, :, :3]).astype("uint8"))
        bio = io.BytesIO(); img.save(bio, format="PNG")
        firma_bytes = bio.getvalue()

        # id_captura secuencial
        caps_df = dfs.get("capturas", pd.DataFrame())
        next_id = next_id_from_df(caps_df, "id_captura")

        # Campos por máquina
        items_fields = []
        for e in seleccionadas:
            valor_real = float(e["valor_real"])
            gan_cli = round(valor_real * (comision / 100.0), 2)
            gan_own = round(valor_real - gan_cli, 2)
            items_fields.append({
                "id_captura": next_id,
                "fecha": datetime.now().isoformat(),
                "user_upn": user["upn"],
                "id_ruta":     to_int_or_none(ruta_sel),
                "id_cliente":  to_int_or_none(id_cliente),
                "id_maquina":  to_int_or_none(e["id_maquina"]),
                "score": int(e["score"]),
                "valor_real": valor_real,
                "comision_cliente": float(comision),
                "ganancia_cliente": gan_cli,
                "ganancia_owner":   gan_own,
                "observaciones": e["obs"],
                # firma_url se rellena si sube en línea
            })
            next_id += 1

        # ¿Modo offline?
        if st.session_state.get("offline_mode", False):
            sig_path = _save_signature_local(firma_bytes)
            queue_add({"type": "capture_batch", "signature_file": sig_path,
                       "items": [{"fields": f} for f in items_fields]})
            log_action_remote(settings, token, "Capturas", "cola-offline", f"cliente={id_cliente}, n={len(items_fields)}", user["upn"])
            st.success(f"Lote en cola local ({len(items_fields)}). Se subirá al sincronizar.")
            refresh_data()
            return

        # Online: intentar subir firma y luego items
        try:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            fname = f"firma_{id_cliente}_{ts}.png"
            firma_url = upload_signature_png(settings, token, fname, firma_bytes)

            if not firma_url:
                # Fallback a cola si no obtuvimos URL
                raise RuntimeError("No se pudo obtener URL de la firma.")

            creadas = 0
            for f in items_fields:
                f2 = dict(f); f2["firma_url"] = firma_url
                list_add(settings, token, "Capturas", f2)
                creadas += 1

            log_action_remote(settings, token, "Capturas", "crear-multiple", f"cliente={id_cliente}, n={creadas}", user["upn"])
            st.success(f"¡{creadas} captura(s) guardada(s) correctamente!")
            refresh_data()

        except Exception:
            # Cola de respaldo si algo falla en línea
            sig_path = _save_signature_local(firma_bytes)
            queue_add({"type": "capture_batch", "signature_file": sig_path,
                       "items": [{"fields": f} for f in items_fields]})
            log_action_remote(settings, token, "Capturas", "cola-offline-fallback", f"cliente={id_cliente}, n={len(items_fields)}", user["upn"])
            st.info("Sin conexión o error al subir. El lote se guardó en cola local.")
            refresh_data()


def page_reportes(dfs, user):
    st.header("Reportes")
    df = dfs.get("capturas", pd.DataFrame())
    if df.empty:
        st.info("Sin capturas todavía.")
        return

    # Filtros
    col1, col2, col3 = st.columns(3)
    with col1:
        cliente = st.text_input("Cliente (id o nombre contiene)", "")
    with col2:
        f_ini = st.date_input("Desde", value=date.today().replace(day=1))
    with col3:
        f_fin = st.date_input("Hasta", value=date.today())

    work = df.copy()
    if "fecha" in work.columns:
        try:
            work["fecha_dt"] = pd.to_datetime(work["fecha"])
            work = work[(work["fecha_dt"].dt.date >= f_ini) & (work["fecha_dt"].dt.date <= f_fin)]
        except Exception:
            pass

    if cliente:
        mask = pd.Series([False]*len(work))
        if "id_cliente" in work.columns:
            mask = mask | work["id_cliente"].astype(str).str.contains(cliente, case=False, na=False)
        if "nombre" in work.columns:
            mask = mask | work["nombre"].astype(str).str.contains(cliente, case=False, na=False)
        work = work[mask]

    tot_owner = work.get("ganancia_owner", pd.Series([], dtype=float)).astype(float).sum()
    tot_cli = work.get("ganancia_cliente", pd.Series([], dtype=float)).astype(float).sum()

    st.metric("Ganancia Propietario", f"${tot_owner:,.2f}")
    st.metric("Ganancia Clientes", f"${tot_cli:,.2f}")
    st.dataframe(work, use_container_width=True)

def page_clientes(dfs, settings, user, token):
    st.header("Clientes")

    clientes_df = dfs.get("clientes", pd.DataFrame()).copy()
    rutas_df    = dfs.get("rutas", pd.DataFrame()).copy()

    # Mapeos nombre<->id
    route_names, name2id, id2name = _rutas_maps(rutas_df)

    tab1, tab2 = st.tabs(["➕ Agregar", "✏️ Editar / Activar-Inactivar"])

    # -------- Agregar --------
    with tab1:
        with st.form("frm_add_cliente"):
            nombre    = st.text_input("Nombre")
            apellido  = st.text_input("Apellido")
            direccion = st.text_input("Dirección Postal")
            telefono  = st.text_input("Teléfono")
            correo    = st.text_input("Correo electrónico")
            comision  = st.number_input("Comisión (%)", value=40.0, step=1.0)

            ruta_name = st.selectbox("Ruta", route_names if route_names else [])
            ruta_id   = to_int_or_none(name2id.get(ruta_name))

            if st.form_submit_button("Guardar", use_container_width=True):
                try:
                    new_id = next_id_from_df(clientes_df, "id_cliente")
                    list_add(settings, token, "Clientes", {
                        "id_cliente": new_id,
                        "nombre": nombre, "apellido": apellido,
                        "direccion": direccion, "telefono": telefono, "correo": correo,
                        "comision": float(comision), "id_ruta": ruta_id, "estatus": "activo",
                    })
                    log_action_remote(settings, token, "Clientes", "crear", nombre, user["upn"])
                    st.success("Cliente creado.")
                    refresh_data()
                except Exception as e:
                    st.error(f"Error al crear cliente: {e}")

    # -------- Editar --------
    with tab2:
        if clientes_df.empty:
            st.info("No hay clientes.")
            return

        sel = st.selectbox("Cliente", clientes_df["id_cliente"].astype(str) + " - " + clientes_df["nombre"].astype(str))
        if not sel:
            return
        idc = sel.split(" - ")[0]
        row = clientes_df.loc[clientes_df["id_cliente"].astype(str) == idc].iloc[0]

        with st.form("frm_edit_cliente"):
            nombre    = st.text_input("Nombre", row.get("nombre", ""))
            apellido  = st.text_input("Apellido", row.get("apellido", ""))
            direccion = st.text_input("Dirección Postal", row.get("direccion", ""))
            telefono  = st.text_input("Teléfono", row.get("telefono", ""))
            correo    = st.text_input("Correo electrónico", row.get("correo", ""))
            comision  = st.number_input("Comisión (%)", value=float(row.get("comision", 40)), step=1.0)

            curr_ruta_id = str(to_int_or_none(row.get("id_ruta","")) or "")
            curr_name    = id2name.get(curr_ruta_id, route_names[0] if route_names else "")
            ruta_name    = st.selectbox("Ruta", route_names if route_names else [],
                                        index=route_names.index(curr_name) if curr_name in route_names else 0)
            ruta_id      = to_int_or_none(name2id.get(ruta_name))

            estatus = st.selectbox("Estatus", ["activo","inactivo"],
                                   index=(0 if str(row.get("estatus","activo"))=="activo" else 1))

            if st.form_submit_button("Guardar cambios", use_container_width=True):
                try:
                    sp_id = str(row["_sp_item_id"])
                    list_update(settings, token, "Clientes", sp_id, {
                        "nombre": nombre, "apellido": apellido, "direccion": direccion,
                        "telefono": telefono, "correo": correo, "comision": float(comision),
                        "id_ruta": ruta_id, "estatus": estatus
                    })
                    log_action_remote(settings, token, "Clientes", "editar", idc, user["upn"])
                    st.success("Cliente actualizado.")
                    refresh_data()
                except Exception as e:
                    st.error(f"Error actualizando: {e}")


def page_maquinas(dfs, settings, user, token):
    st.header("Máquinas")

    maq_df      = dfs.get("maquinas", pd.DataFrame()).copy()
    rutas_df    = dfs.get("rutas", pd.DataFrame()).copy()
    clientes_df = dfs.get("clientes", pd.DataFrame()).copy()

    # Mapeos rutas
    route_names, name2id, id2name = _rutas_maps(rutas_df)

    # Opciones de cliente
    if "id_cliente" in clientes_df.columns:
        clientes_df["id_cliente_norm"] = pd.to_numeric(clientes_df["id_cliente"], errors="coerce").fillna(0).astype(int).astype(str)
    else:
        clientes_df["id_cliente_norm"] = pd.Series([], dtype=str)
    cliente_opts = [""] + clientes_df["id_cliente_norm"].tolist()

    tab1, tab2 = st.tabs(["➕ Agregar", "✏️ Editar/Asignar/Activar"])

    # -------- Agregar --------
    with tab1:
        with st.form("frm_add_maquina"):
            numero_maquina = st.text_input("Número de máquina", "")
            numero_permiso = st.text_input("Número de permiso", "")
            tipo_maquina   = st.text_input("Tipo de máquina", "")
            fondo          = st.number_input("Fondo", min_value=0.0, step=1.0, value=0.0)

            ruta_name = st.selectbox("Ruta asignada", route_names if route_names else [])
            ruta_id   = to_int_or_none(name2id.get(ruta_name))

            cliente   = st.selectbox("Cliente (opcional)", cliente_opts)
            estatus   = st.selectbox("Estatus", ["libre", "asignada", "inactiva"], index=0)
            activa    = st.selectbox("Activa", ["activa", "inactiva"], index=0)

            if st.form_submit_button("Guardar", use_container_width=True):
                try:
                    new_id = next_id_from_df(maq_df, "id_maquina")
                    list_add(settings, token, "Maquinas", {
                        "id_maquina": new_id,
                        "numero_maquina": numero_maquina,
                        "numero_permiso": numero_permiso,
                        "tipo_maquina": tipo_maquina,
                        "fondo": float(fondo),
                        "id_ruta":   ruta_id,
                        "id_cliente": to_int_or_none(cliente),
                        "estatus": estatus,
                        "activa": activa,
                    })
                    log_action_remote(settings, token, "Maquinas", "crear", numero_maquina, user["upn"])
                    st.success("Máquina creada.")
                    refresh_data()
                except Exception as e:
                    st.error(f"Error al crear máquina: {e}")

    # -------- Editar --------
    with tab2:
        if maq_df.empty:
            st.info("No hay máquinas.")
            return

        sel = st.selectbox("Máquina", maq_df["id_maquina"].astype(str) + " - " + _scol(maq_df, "numero_maquina", "").astype(str))
        if not sel:
            return
        idm = sel.split(" - ")[0]
        row = maq_df.loc[maq_df["id_maquina"].astype(str) == idm].iloc[0]

        with st.form("frm_edit_maquina"):
            numero_maquina = st.text_input("Número de máquina", row.get("numero_maquina", ""))
            numero_permiso = st.text_input("Número de permiso", row.get("numero_permiso", ""))
            tipo_maquina   = st.text_input("Tipo de máquina", row.get("tipo_maquina", ""))
            fondo          = st.number_input("Fondo", min_value=0.0, step=1.0, value=float(row.get("fondo", 0)))

            curr_ruta_id = str(to_int_or_none(row.get("id_ruta","")) or "")
            curr_name    = id2name.get(curr_ruta_id, route_names[0] if route_names else "")
            ruta_name    = st.selectbox("Ruta asignada", route_names if route_names else [],
                                        index=route_names.index(curr_name) if curr_name in route_names else 0)
            ruta_id      = to_int_or_none(name2id.get(ruta_name))

            curr_cli  = str(to_int_or_none(row.get("id_cliente","")) or "")
            idx_cli   = ([""] + clientes_df["id_cliente_norm"].tolist()).index(curr_cli) if curr_cli in ([""] + clientes_df["id_cliente_norm"].tolist()) else 0
            cliente   = st.selectbox("Cliente", [""] + clientes_df["id_cliente_norm"].tolist(), index=idx_cli)

            estatus = st.selectbox("Estatus", ["libre", "asignada", "inactiva"],
                                   index=(["libre","asignada","inactiva"].index(str(row.get("estatus","libre"))) if str(row.get("estatus","libre")) in ["libre","asignada","inactiva"] else 0))
            activa  = st.selectbox("Activa", ["activa", "inactiva"],
                                   index=(["activa","inactiva"].index(str(row.get("activa","activa"))) if str(row.get("activa","activa")) in ["activa","inactiva"] else 0))

            if st.form_submit_button("Guardar cambios", use_container_width=True):
                try:
                    sp_id = str(row["_sp_item_id"])
                    list_update(settings, token, "Maquinas", sp_id, {
                        "numero_maquina": numero_maquina,
                        "numero_permiso": numero_permiso,
                        "tipo_maquina": tipo_maquina,
                        "fondo": float(fondo),
                        "id_ruta":   ruta_id,
                        "id_cliente": to_int_or_none(cliente),
                        "estatus": estatus,
                        "activa": activa,
                    })
                    log_action_remote(settings, token, "Maquinas", "editar", idm, user["upn"])
                    st.success("Máquina actualizada.")
                    refresh_data()
                except Exception as e:
                    st.error(f"Error actualizando máquina: {e}")


def page_rutas(dfs, settings, user, token):
    st.header("Rutas")
    rutas_df = dfs.get("rutas", pd.DataFrame()).copy()

    tab1, tab2 = st.tabs(["➕ Agregar", "✏️ Editar/Activar/Desactivar"])
    with tab1:
        with st.form("frm_add_ruta"):
            nombre = st.text_input("Nombre de ruta", "")
            if st.form_submit_button("Guardar", use_container_width=True):
                try:
                    new_id = next_id_from_df(rutas_df, "id_ruta")
                    list_add(settings, token, "Rutas", {"id_ruta": new_id, "nombre": nombre, "estatus": "activo"})
                    log_action_remote(settings, token, "Rutas", "crear", nombre, user["upn"])
                    st.success("Ruta creada.")
                    refresh_data()
                except Exception as e:
                    st.error(f"Error al crear ruta: {e}")

    with tab2:
        if rutas_df.empty:
            st.info("No hay rutas.")
            return
        sel = st.selectbox("Ruta", rutas_df["id_ruta"].astype(str) + " - " + rutas_df["nombre"].astype(str))
        if sel:
            rid = sel.split(" - ")[0]
            row = rutas_df.loc[rutas_df["id_ruta"].astype(str) == rid].iloc[0]
            with st.form("frm_edit_ruta"):
                nombre = st.text_input("Nombre de ruta", row.get("nombre",""))
                estatus = st.selectbox("Estatus", ["activo","inactivo"], index=(0 if row.get("estatus","activo")=="activo" else 1))
                if st.form_submit_button("Guardar cambios", use_container_width=True):
                    try:
                        sp_id = str(row["_sp_item_id"])
                        list_update(settings, token, "Rutas", sp_id, {"nombre": nombre, "estatus": estatus})
                        log_action_remote(settings, token, "Rutas", "editar", rid, user["upn"])
                        st.success("Ruta actualizada.")
                        refresh_data()
                    except Exception as e:
                        st.error(f"Error actualizando ruta: {e}")

def page_usuarios(dfs, settings, user, token):
    st.header("Usuarios (asignación de rutas)")

    # Asegurar lista
    try:
        ensure_list_with_columns(settings, token, "UsuariosExtras", [
            {"name":"user_upn","text":{}},
            {"name":"id_ruta","number":{}},
        ])
    except Exception as e:
        st.warning(f"No pude asegurar la lista UsuariosExtras: {e}")

    ue_df    = dfs.get("usuariosextras", pd.DataFrame()).copy()
    rutas_df = dfs.get("rutas", pd.DataFrame()).copy()

    route_names, name2id, id2name = _rutas_maps(rutas_df)

    # Asegura columnas para evitar KeyError
    for c, dtype in (("user_upn", str), ("id_ruta", str), ("_sp_item_id", str)):
        if c not in ue_df.columns:
            ue_df[c] = pd.Series([], dtype=dtype)

    with st.form("frm_add_userextra"):
        upn       = st.text_input("UPN (correo corporativo)")
        ruta_name = st.selectbox("Ruta asignada", route_names if route_names else [])
        ruta_id   = to_int_or_none(name2id.get(ruta_name))
        if st.form_submit_button("Asignar/Actualizar", use_container_width=True):
            try:
                upn_key = (upn or "").strip().lower()
                if not upn_key:
                    st.error("Captura un UPN válido.")
                    st.stop()

                mask = ue_df["user_upn"].astype(str).str.lower() == upn_key
                if not mask.any():
                    list_add(settings, token, "UsuariosExtras", {"user_upn": upn, "id_ruta": ruta_id, "Title": upn})
                    log_action_remote(settings, token, "Usuarios", "crear", upn, user["upn"])
                    st.success("Asignación creada.")
                else:
                    sp_id = str(ue_df.loc[mask, "_sp_item_id"].iloc[0])
                    list_update(settings, token, "UsuariosExtras", sp_id, {"user_upn": upn, "id_ruta": ruta_id, "Title": upn})
                    log_action_remote(settings, token, "Usuarios", "editar", upn, user["upn"])
                    st.success("Asignación actualizada.")
                refresh_data()
            except Exception as e:
                st.error(f"Error guardando asignación: {e}")

    st.subheader("Asignaciones actuales")
    if not ue_df.empty and "id_ruta" in ue_df.columns:
        try:
            ue_df["id_ruta"] = pd.to_numeric(ue_df["id_ruta"], errors="coerce").fillna(0).astype(int).astype(str)
            ue_df["ruta_nombre"] = ue_df["id_ruta"].map(lambda rid: id2name.get(rid, rid))
        except Exception:
            pass
    st.dataframe(ue_df, use_container_width=True)



# =========================
# MAIN
# =========================

def main():
    settings = _get_settings()

    # Paso 1: Autenticación SSO
    token = login_sso(settings)
    user = user_context(token, settings)
    st.sidebar.success(f"Bienvenido: {user['upn']} ({user['rol']})")

    # Toggle de modo offline en la barra lateral
    offline = st.sidebar.toggle(
        "Modo offline (cola local)",
        value=st.session_state.get("offline_mode", False),
        help="Si está activo, guarda en disco y sube después."
    )
    st.session_state["offline_mode"] = offline

    # Botón para sincronizar pendientes
    pend = queue_count()
    if pend > 0:
        if st.sidebar.button(f"Sincronizar pendientes ({pend})"):
            ok, remain = sync_queue(settings, token)
            st.sidebar.success(f"Sincronizados: {ok}. Quedan: {remain}.")

    # Paso 2: Cargar listas
    dfs = load_all_lists(settings, token)

    # Paso 3: Menú
    choice = show_menu_for(user)

    if choice == "Capturar Datos":
        page_capturas(dfs, settings, user, token)
    elif choice == "Reportes":
        page_reportes(dfs, user)
    elif choice == "Clientes" and user["rol"] == "admin":
        page_clientes(dfs, settings, user, token)
    elif choice == "Maquinas" and user["rol"] == "admin":
        page_maquinas(dfs, settings, user, token)
    elif choice == "Rutas" and user["rol"] == "admin":
        page_rutas(dfs, settings, user, token)
    elif choice == "Usuarios" and user["rol"] == "admin":
        page_usuarios(dfs, settings, user, token)
    elif choice == "Migrar desde Excel" and user["rol"] == "admin":
        page_migrar_excel(settings, token)
    elif choice == "Salir":
        st.session_state.clear()
        st.success("Sesión cerrada.")
        refresh_data()

if __name__ == "__main__":
    main()


