# -*- coding: utf-8 -*-
import io
import os
import time
import json
import uuid
import math
from copy import deepcopy
from datetime import datetime, date
from typing import Dict, List

import requests
import pandas as pd
import streamlit as st
from streamlit_drawable_canvas import st_canvas

# =========================
# Setup (Azure-friendly)
# =========================

st.set_page_config(page_title="Dashboard Maquinas by Nexus Solutions", page_icon="🎰", layout="wide")
PRIMARY_COLOR = "#161F35"

# Data dir for offline queue & local signatures
DEFAULT_DATA_DIR = os.environ.get("DATA_DIR") or "/home/site/wwwroot/data"
if not os.path.exists(DEFAULT_DATA_DIR):
    # Fallback when running locally
    DEFAULT_DATA_DIR = os.path.join(os.getcwd(), "data")
os.makedirs(DEFAULT_DATA_DIR, exist_ok=True)

QUEUE_FILE = os.path.join(DEFAULT_DATA_DIR, "queue.json")

# --- Almacenamiento offline persistente ---
AZURE_PERSIST_DIR = os.environ.get("AZURE_PERSIST_DIR", "/home")
DATA_DIR   = os.environ.get("DATA_DIR", os.path.join(AZURE_PERSIST_DIR, "site", "wwwroot", "offline_queue"))
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
    fn = os.path.join(DEFAULT_DATA_DIR, f"firma_{uuid.uuid4().hex}.png")
    with open(fn, "wb") as f:
        f.write(content)
    return fn

def refresh_data():
    st.cache_data.clear()
    st.rerun()

# =========================
# Settings
# =========================

def _from_env(name: str, default: str = "") -> str:
    # allow both MY__KEY and MY_KEY styles
    if name in os.environ:
        return os.environ.get(name, default)
    # dotted style GRAPH__TENANT_ID, APP__REDIRECT_URI
    val = os.environ.get(name.replace(".", "__").upper(), "")
    return val or default

def _get_settings():
    # 1) Lee primero variables de entorno (recomendado en Azure)
    env_graph = {
        "tenant_id":    os.getenv("GRAPH_TENANT_ID", "").strip(),
        "client_id":    os.getenv("GRAPH_CLIENT_ID", "").strip(),
        "client_secret":os.getenv("GRAPH_CLIENT_SECRET", "").strip(),
        "hostname":     os.getenv("GRAPH_HOSTNAME", "").strip(),
        "site_path":    os.getenv("GRAPH_SITE_PATH", "").strip(),
        "firmas_library_path": os.getenv("GRAPH_FIRMAS_LIBRARY_PATH", "/Shared Documents/Firmas").strip(),
        "authority":    os.getenv("GRAPH_AUTHORITY", "").strip(),
        "tenant_name":  os.getenv("GRAPH_TENANT_NAME", "").strip(),
    }
    env_app = {
        "redirect_uri": os.getenv("APP_REDIRECT_URI", "https://localhost:8501/redirect"),
        "admin_group":  os.getenv("APP_ADMIN_GROUP", "MES-Admin"),
        "operator_group": os.getenv("APP_OPERATOR_GROUP", "MES-Operador"),
    }

    # 2) Si hay st.secrets, sirve como fallback/override local
    graph = {}
    app = {}
    try:
        graph = dict(st.secrets.get("graph", {}))
        app   = dict(st.secrets.get("app", {}))
    except Exception:
        graph = {}
        app = {}

    # 3) Mezcla: env > secrets > defaults
    def pick(env_val, sec_val, default=""):
        return (env_val or sec_val or default).strip() if isinstance(default, str) else (env_val or sec_val or default)

    return {
        "tenant_id":          pick(env_graph["tenant_id"], graph.get("tenant_id","")),
        "client_id":          pick(env_graph["client_id"], graph.get("client_id","")),
        "client_secret":      pick(env_graph["client_secret"], graph.get("client_secret","")),
        "hostname":           pick(env_graph["hostname"], graph.get("hostname","")),
        "site_path":          pick(env_graph["site_path"], graph.get("site_path","")),
        "firmas_library_path":pick(env_graph["firmas_library_path"], graph.get("firmas_library_path","/Shared Documents/Firmas")),
        "authority":          pick(env_graph["authority"], graph.get("authority","")),
        "tenant_name":        pick(env_graph["tenant_name"], graph.get("tenant_name","")),
        "redirect_uri":       pick(env_app["redirect_uri"], app.get("redirect_uri","https://localhost:8501/redirect")),
        "admin_group":        pick(env_app["admin_group"], app.get("admin_group","MES-Admin")),
        "operator_group":     pick(env_app["operator_group"], app.get("operator_group","MES-Operador")),
    }

# =========================
# Auth (MSAL SSO)
# =========================

AUTH_SCOPES = [
    "User.Read",
    "GroupMember.Read.All",
    "Sites.ReadWrite.All",
    "Files.ReadWrite.All",
]

def msal_app(settings):
    import msal

    auth = (settings.get("authority") or "").strip()
    if not auth:
        tenant_name = (settings.get("tenant_name") or "").strip()
        if tenant_name:
            auth = f"https://login.microsoftonline.com/{tenant_name}"
        else:
            tid = (settings.get("tenant_id") or "").strip()
            auth = f"https://login.microsoftonline.com/{tid}"

    # Validate authority discovery (nicer error)
    oidc = auth.rstrip("/") + "/v2.0/.well-known/openid-configuration"
    try:
        r = requests.get(oidc, timeout=10)
        r.raise_for_status()
    except Exception as e:
        st.error(f"Authority inválida o no accesible: {oidc}\n{e}")
        st.code(f"authority = {auth!r}", language="text")
        raise

    return msal.ConfidentialClientApplication(
        client_id=settings["client_id"],
        authority=auth,
        client_credential=settings["client_secret"],
    )

def login_sso(settings) -> dict:
    if "token" in st.session_state:
        tok = st.session_state["token"]
        if tok and tok.get("expires_at", 0) > time.time() + 60:
            return tok

    app = msal_app(settings)
    qp = st.query_params
    if "code" not in qp:
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

    st.query_params.clear()
    refresh_data()

# =========================
# Microsoft Graph helpers
# =========================

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

def _resolve_site_id(settings, token) -> str:
    if "site_id" in st.session_state and st.session_state["site_id"]:
        return st.session_state["site_id"]

    host = settings["hostname"].strip()
    path = settings["site_path"].strip()
    try:
        url = f"https://graph.microsoft.com/v1.0/sites/{host}:{path}?$select=id,webUrl,displayName"
        data = graph_get(url, token)
        sid = data["id"]
        st.session_state["site_id"] = sid
        return sid
    except Exception:
        pass

    # fallback search
    name = path.split("/")[-1]
    search_url = f"https://graph.microsoft.com/v1.0/sites/{host}/sites?search={name}&$select=id,webUrl,displayName"
    res = graph_get(search_url, token)
    candidates = res.get("value", [])
    chosen = None
    for c in candidates:
        web = (c.get("webUrl") or "").lower().replace("%20", "-")
        if web.endswith(path.lower().replace("%20", "-").replace(" ", "-")):
            chosen = c; break
    if not chosen and candidates:
        chosen = candidates[0]
    if not chosen:
        raise RuntimeError(f"No pude encontrar el sitio '{path}' en {host}. Revisa hostname/site_path.")

    sid = chosen["id"]
    st.session_state["site_id"] = sid
    return sid

def site_base(settings):
    token = st.session_state.get("token")
    if not token:
        raise RuntimeError("site_base: falta token.")
    sid = _resolve_site_id(settings, token)
    return f"https://graph.microsoft.com/v1.0/sites/{sid}"

# Lists helpers
def _list_id_for(settings, token, list_title: str) -> str:
    cache = st.session_state.setdefault("list_id_cache", {})
    if list_title in cache:
        return cache[list_title]
    base = site_base(settings)
    title = list_title.replace("'", "''")
    url = f"{base}/lists?$filter=displayName eq '{title}'&$select=id,displayName"
    data = graph_get(url, token)
    vals = data.get("value", [])
    if not vals:
        raise RuntimeError(f"La lista '{list_title}' no existe en el sitio.")
    lid = vals[0]["id"]
    cache[list_title] = lid
    return lid

def list_items_all(settings, token, list_title: str, select="*", expand_fields=True) -> list:
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
    lid = _list_id_for(settings, token, list_title)
    url = f"{base}/lists/{lid}/items"
    return graph_post_json(url, token, {"fields": fields})

def list_update(settings, token, list_title: str, item_id: str, fields: dict) -> dict:
    base = site_base(settings)
    lid = _list_id_for(settings, token, list_title)
    url = f"{base}/lists/{lid}/items/{item_id}/fields"
    return graph_patch_json(url, token, fields)

def ensure_list_with_columns(settings, token, list_title: str, columns: list[dict]):
    base = site_base(settings)
    # create if missing
    def get_list_id_by_name(list_title: str) -> str | None:
        url = f"{base}/lists?$filter=displayName eq '{list_title}'&$select=id,displayName"
        data = graph_get(url, token)
        vals = data.get("value", [])
        return vals[0]["id"] if vals else None

    list_id = get_list_id_by_name(list_title)
    if not list_id:
        created = graph_post_json(f"{base}/lists", token, {"displayName": list_title, "list": {"template": "genericList"}})
        list_id = created["id"]

    try:
        existing = graph_get(f"{base}/lists/{list_id}/columns?$select=id,name,required", token).get("value", [])
    except Exception:
        existing = graph_get(f"{base}/lists/{list_id}/columns", token).get("value", [])
    existing_names = {c.get("name") for c in existing if c.get("name")}

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

    for col in columns:
        name = col.get("name")
        if not name or name in existing_names:
            continue
        graph_post_json(f"{base}/lists/{list_id}/columns", token, _sanitize(col))
    return list_id

def upload_signature_png(settings, token, filename: str, content: bytes) -> str:
    """Upload signature to site drive and return webUrl."""
    base = site_base(settings)
    path = (settings.get("firmas_library_path", "/Shared Documents/Firmas") or "").rstrip("/")
    # IMPORTANT: pattern is /drive/root:{path}/{file}:/content
    put_url = f"{base}/drive/root:{path}/{filename}:/content"
    h = {"Authorization": f"Bearer {token['access_token']}"}
    r = requests.put(put_url, headers=h, data=content, timeout=60)
    if 200 <= r.status_code < 300:
        return r.json().get("webUrl", "")
    st.warning(f"Error subiendo firma: {r.status_code} {r.text[:300]}")
    return ""

# =========================
# Data utils
# =========================

def items_to_df(items: List[dict]) -> pd.DataFrame:
    rows = []
    for it in items:
        f = it.get("fields", {})
        f["_sp_item_id"] = it.get("id")
        rows.append(f)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).fillna("")

def next_id_from_df(df: pd.DataFrame, id_col: str) -> int:
    if df.empty or id_col not in df.columns or df[id_col].dropna().empty:
        return 1
    try:
        return int(pd.to_numeric(df[id_col], errors="coerce").max()) + 1
    except Exception:
        return 1

def to_int_or_none(v):
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    try:
        return int(float(s))
    except Exception:
        return None

def _scol(df: pd.DataFrame, name: str, default=""):
    if name in df.columns:
        return df[name]
    return pd.Series([default]*len(df), index=df.index)

def _rutas_maps(rutas_df: pd.DataFrame):
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
    route_names.sort()
    return route_names, name2id, id2name

# =========================
# Context & caching
# =========================

def user_context(token: dict, settings: dict) -> dict:
    me = graph_get("https://graph.microsoft.com/v1.0/me", token)
    upn = me.get("userPrincipalName", "")
    groups = graph_get("https://graph.microsoft.com/v1.0/me/memberOf?$select=displayName", token)
    names = [g.get("displayName", "") for g in groups.get("value", [])]
    rol = "admin" if settings["admin_group"] in names else ("operador" if settings["operator_group"] in names else "operador")

    id_ruta = ""
    try:
        ue = list_items_all(settings, token, "UsuariosExtras")
        ue_df = items_to_df(ue)
        if not ue_df.empty and "user_upn" in ue_df.columns and "id_ruta" in ue_df.columns:
            fila = ue_df.loc[ue_df["user_upn"].str.lower() == upn.lower()]
            if not fila.empty:
                val = fila["id_ruta"].iloc[0]
                id_ruta = str(int(float(val))) if str(val) != "" else ""
    except Exception:
        id_ruta = ""

    return {"upn": upn, "rol": rol, "id_ruta": id_ruta, "groups": names}

@st.cache_data(show_spinner=False, ttl=15)
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
# Offline queue sync
# =========================

def sync_queue(settings, token):
    q = _queue_load()
    remaining = []
    uploaded = 0

    for entry in q:
        try:
            et = entry.get("type")
            if et == "list_add":
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
                # 1) upload signature
                with open(entry["signature_file"], "rb") as f:
                    sig = f.read()
                fname = os.path.basename(entry["signature_file"])
                firma_url = upload_signature_png(settings, token, fname, sig)
                # 2) push items
                for it in entry["items"]:
                    fields = deepcopy(it["fields"])
                    fields["firma_url"] = firma_url
                    base = site_base(settings)
                    lid  = _list_id_for(settings, token, "Capturas")
                    url  = f"{base}/lists/{lid}/items"
                    graph_post_json(url, token, {"fields": fields})
                uploaded += 1

            else:
                remaining.append(entry)
        except Exception:
            remaining.append(entry)

    _queue_save(remaining)
    return uploaded, len(remaining)

# =========================
# UI bits
# =========================

def show_menu_for(user):
    rol = (user.get("rol") or "").lower()
    opciones = []
    if rol in ("admin", "operador"):
        opciones.append("Capturar Datos")
    if rol == "admin":
        opciones += ["Reportes", "Clientes", "Maquinas", "Rutas", "Usuarios", "Migrar desde Excel"]
    opciones.append("Salir")
    return st.selectbox("Menú", opciones, index=0)

# =========================
# Pages
# =========================

def page_capturas(dfs, settings, user, token):
    st.header("Capturar Datos")

    rutas_df    = dfs.get("rutas", pd.DataFrame()).copy()
    clientes_df = dfs.get("clientes", pd.DataFrame()).copy()
    maquinas_df = dfs.get("maquinas", pd.DataFrame()).copy()

    # Normalize numeric columns to str-int
    for df, cols in ((rutas_df, ["id_ruta"]),
                     (clientes_df, ["id_cliente", "id_ruta"]),
                     (maquinas_df, ["id_maquina", "id_cliente", "id_ruta"])):
        for c in cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int).astype(str)

    # Filter by operator route
    assigned_route = str(user.get("id_ruta") or "").strip()
    if (user.get("rol") == "operador") and assigned_route:
        rutas_df = rutas_df.loc[rutas_df["id_ruta"].astype(str) == assigned_route]
        if not rutas_df.empty:
            st.caption(f"Ruta asignada: {rutas_df['nombre'].iloc[0]}")

    # Route selection by name
    route_names, name2id, id2name = _rutas_maps(rutas_df)
    if route_names:
        default_name = id2name.get(assigned_route, route_names[0])
        ruta_name_sel = st.selectbox("Ruta", route_names, index=route_names.index(default_name) if default_name in route_names else 0)
        ruta_sel = name2id.get(ruta_name_sel, "")
    else:
        ruta_name_sel = st.selectbox("Ruta", [])
        ruta_sel = ""

    # Clients by route
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

    # Machines by client
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

    # Per-machine inputs
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

    # Commission
    comision = 40.0
    if cliente_sel and not clientes_df.empty:
        fila_c = clientes_df.loc[clientes_df["id_cliente"].astype(str) == id_cliente]
        if not fila_c.empty and "comision" in fila_c.columns:
            try:
                comision = float(fila_c["comision"].iloc[0])
            except Exception:
                pass

    # Totals
    seleccionadas = [e for e in entradas if (e["score"] or e["valor_real"])]
    tot_valor   = sum(e["valor_real"] for e in seleccionadas)
    tot_cli     = round(tot_valor * (comision / 100.0), 2)
    tot_owner   = round(tot_valor - tot_cli, 2)

    st.subheader("Resumen")
    st.write(f"Máquinas con captura: **{len(seleccionadas)}**")
    st.write(f"Comisión cliente: **{comision:.1f}%**")
    st.write(f"Ganancia cliente: **${tot_cli:,.2f}**")
    st.write(f"Ganancia propietario: **${tot_owner:,.2f}**")

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

        # Render signature
        from PIL import Image
        img = Image.fromarray((canvas.image_data[:, :, :3]).astype("uint8"))
        bio = io.BytesIO(); img.save(bio, format="PNG")
        firma_bytes = bio.getvalue()
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = f"firma_{id_cliente}_{ts}.png"

        offline_mode = st.session_state.get("offline_mode", False)

        if offline_mode:
            # Save locally and queue the batch
            local_path = _save_signature_local(firma_bytes)
            caps_df = dfs.get("capturas", pd.DataFrame())
            next_id = next_id_from_df(caps_df, "id_captura")
            items = []
            for e in seleccionadas:
                valor_real = float(e["valor_real"])
                gan_cli  = round(valor_real * (comision / 100.0), 2)
                gan_own  = round(valor_real - gan_cli, 2)
                items.append({
                    "fields": {
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
                        # firma_url se añade al sincronizar
                    }
                })
                next_id += 1
            queue_add({"type": "capture_batch", "signature_file": local_path, "items": items})
            st.success(f"Capturas en cola offline: {len(items)}. Se subirán al sincronizar.")
            return

        # Online: upload signature then push items
        firma_url = upload_signature_png(settings, token, fname, firma_bytes)
        caps_df = dfs.get("capturas", pd.DataFrame())
        next_id = next_id_from_df(caps_df, "id_captura")

        guardadas = 0
        errores   = 0
        for e in seleccionadas:
            try:
                valor_real = float(e["valor_real"])
                gan_cli  = round(valor_real * (comision / 100.0), 2)
                gan_own  = round(valor_real - gan_cli, 2)

                list_add(settings, token, "Capturas", {
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
                    "firma_url": firma_url,
                })
                next_id += 1
                guardadas += 1
            except Exception as ex:
                errores += 1
                st.warning(f"No se pudo guardar Maq {e['numero']} (ID {e['id_maquina']}): {ex}")

        log_action_remote(settings, token, "Capturas", "crear-multiple",
                          f"cliente={id_cliente}, maquinas={guardadas}, errores={errores}", user["upn"])

        if guardadas:
            st.success(f"¡{guardadas} capturas guardadas correctamente!")
            refresh_data()
        else:
            st.error("No se guardó ninguna captura.")

def page_reportes(dfs, user):
    st.header("Reportes")
    df = dfs.get("capturas", pd.DataFrame())
    if df.empty:
        st.info("Sin capturas todavía.")
        return

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
    rutas_df = dfs.get("rutas", pd.DataFrame()).copy()

    if "id_ruta" in rutas_df.columns:
        rutas_df["id_ruta_norm"] = pd.to_numeric(rutas_df["id_ruta"], errors="coerce").fillna(0).astype(int).astype(str)
    else:
        rutas_df["id_ruta_norm"] = pd.Series([], dtype=str)
    ruta_opts = rutas_df["id_ruta_norm"].tolist()

    tab1, tab2 = st.tabs(["➕ Agregar", "✏️ Editar / Activar-Inactivar"])

    with tab1:
        with st.form("frm_add_cliente"):
            nombre = st.text_input("Nombre")
            apellido = st.text_input("Apellido")
            direccion = st.text_input("Dirección Postal")
            telefono = st.text_input("Teléfono")
            correo = st.text_input("Correo electrónico")
            comision = st.number_input("Comisión (%)", value=40.0, step=1.0)
            ruta = st.selectbox("Ruta", ruta_opts if ruta_opts else [])
            if st.form_submit_button("Guardar", use_container_width=True):
                try:
                    new_id = next_id_from_df(clientes_df, "id_cliente")
                    list_add(settings, token, "Clientes", {
                        "id_cliente": new_id,
                        "nombre": nombre, "apellido": apellido,
                        "direccion": direccion, "telefono": telefono, "correo": correo,
                        "comision": float(comision), "id_ruta": to_int_or_none(ruta), "estatus": "activo",
                    })
                    log_action_remote(settings, token, "Clientes", "crear", nombre, user["upn"])
                    st.success("Cliente creado.")
                    refresh_data()
                except Exception as e:
                    st.error(f"Error al crear cliente: {e}")

    with tab2:
        if clientes_df.empty:
            st.info("No hay clientes.")
        else:
            sel = st.selectbox("Cliente", clientes_df["nombre"].astype(str))
            if sel:
                idc = sel
                row = clientes_df.loc[clientes_df["nombre"].astype(str) == idc].iloc[0]
                with st.form("frm_edit_cliente"):
                    nombre = st.text_input("Nombre", row.get("nombre", ""))
                    apellido = st.text_input("Apellido", row.get("apellido", ""))
                    direccion = st.text_input("Dirección Postal", row.get("direccion", ""))
                    telefono = st.text_input("Teléfono", row.get("telefono", ""))
                    correo = st.text_input("Correo electrónico", row.get("correo", ""))
                    comision = st.number_input("Comisión (%)", value=float(row.get("comision", 40)), step=1.0)
                    curr_ruta = str(to_int_or_none(row.get("id_ruta", "")) or "")
                    idx_ruta = ruta_opts.index(curr_ruta) if curr_ruta in ruta_opts else 0
                    ruta = st.selectbox("Ruta", ruta_opts if ruta_opts else [], index=idx_ruta)
                    estatus = st.selectbox("Estatus", ["activo","inactivo"], index=0 if str(row.get("estatus","activo"))=="activo" else 1)
                    if st.form_submit_button("Guardar cambios", use_container_width=True):
                        try:
                            sp_id = str(row["_sp_item_id"])
                            list_update(settings, token, "Clientes", sp_id, {
                                "nombre": nombre, "apellido": apellido, "direccion": direccion,
                                "telefono": telefono, "correo": correo, "comision": float(comision),
                                "id_ruta": to_int_or_none(ruta), "estatus": estatus
                            })
                            log_action_remote(settings, token, "Clientes", "editar", idc, user["upn"])
                            st.success("Cliente actualizado.")
                            refresh_data()
                        except Exception as e:
                            st.error(f"Error actualizando: {e}")

def page_maquinas(dfs, settings, user, token):
    st.header("Máquinas")

    maq_df     = dfs.get("maquinas", pd.DataFrame()).copy()
    rutas_df   = dfs.get("rutas", pd.DataFrame()).copy()
    clientes_df= dfs.get("clientes", pd.DataFrame()).copy()

    if "id_ruta" in rutas_df.columns:
        rutas_df["id_ruta_norm"] = pd.to_numeric(rutas_df["id_ruta"], errors="coerce").fillna(0).astype(int).astype(str)
    else:
        rutas_df["id_ruta_norm"] = pd.Series([], dtype=str)
    ruta_opts = rutas_df["id_ruta_norm"].tolist()

    if "id_cliente" in clientes_df.columns:
        clientes_df["id_cliente_norm"] = pd.to_numeric(clientes_df["id_cliente"], errors="coerce").fillna(0).astype(int).astype(str)
    else:
        clientes_df["id_cliente_norm"] = pd.Series([], dtype=str)
    cliente_opts = [""] + clientes_df["id_cliente_norm"].tolist()

    tab1, tab2 = st.tabs(["➕ Agregar", "✏️ Editar/Asignar/Activar"])

    with tab1:
        with st.form("frm_add_maquina"):
            numero_maquina = st.text_input("Número de máquina", "")
            numero_permiso = st.text_input("Número de permiso", "")
            tipo_maquina   = st.text_input("Tipo de máquina", "")
            fondo          = st.number_input("Fondo", min_value=0.0, step=1.0, value=0.0)
            ruta           = st.selectbox("Ruta asignada", ruta_opts if ruta_opts else [])
            cliente        = st.selectbox("Cliente (opcional)", cliente_opts)
            estatus        = st.selectbox("Estatus", ["libre", "asignada", "inactiva"], index=0)
            activa         = st.selectbox("Activa", ["activa", "inactiva"], index=0)

            if st.form_submit_button("Guardar", use_container_width=True):
                try:
                    new_id = next_id_from_df(maq_df, "id_maquina")
                    list_add(settings, token, "Maquinas", {
                        "id_maquina": new_id,
                        "numero_maquina": numero_maquina,
                        "numero_permiso": numero_permiso,
                        "tipo_maquina": tipo_maquina,
                        "fondo": float(fondo),
                        "id_ruta":   to_int_or_none(ruta),
                        "id_cliente": to_int_or_none(cliente),
                        "estatus": estatus,
                        "activa": activa,
                    })
                    log_action_remote(settings, token, "Maquinas", "crear", numero_maquina, user["upn"])
                    st.success("Máquina creada.")
                    refresh_data()
                except Exception as e:
                    st.error(f"Error al crear máquina: {e}")

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

            curr_ruta = str(to_int_or_none(row.get("id_ruta","")) or "")
            idx_ruta  = ruta_opts.index(curr_ruta) if curr_ruta in ruta_opts else 0

            curr_cli  = str(to_int_or_none(row.get("id_cliente","")) or "")
            idx_cli   = cliente_opts.index(curr_cli) if curr_cli in cliente_opts else 0

            ruta    = st.selectbox("Ruta asignada", ruta_opts, index=idx_ruta)
            cliente = st.selectbox("Cliente", cliente_opts, index=idx_cli)

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
                        "id_ruta":   to_int_or_none(ruta),
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
    try:
        ensure_list_with_columns(settings, token, "UsuariosExtras", [
            {"name":"user_upn","text":{}},
            {"name":"id_ruta","number":{}},
        ])
    except Exception as e:
        st.warning(f"No pude asegurar la lista UsuariosExtras: {e}")

    ue_df     = dfs.get("usuariosextras", pd.DataFrame()).copy()
    rutas_df  = dfs.get("rutas", pd.DataFrame()).copy()

    if "id_ruta" in rutas_df.columns:
        rutas_df["id_ruta_norm"] = pd.to_numeric(rutas_df["id_ruta"], errors="coerce").fillna(0).astype(int).astype(str)
    else:
        rutas_df["id_ruta_norm"] = pd.Series([], dtype=str)
    ruta_opts = rutas_df["id_ruta_norm"].tolist()

    for c, dtype in (("user_upn", str), ("id_ruta", str), ("_sp_item_id", str)):
        if c not in ue_df.columns:
            ue_df[c] = pd.Series([], dtype=dtype)

    with st.form("frm_add_userextra"):
        upn  = st.text_input("UPN (correo corporativo)")
        ruta = st.selectbox("Ruta asignada", ruta_opts if ruta_opts else [])
        if st.form_submit_button("Asignar/Actualizar", use_container_width=True):
            try:
                upn_key = (upn or "").strip().lower()
                if not upn_key:
                    st.error("Captura un UPN válido.")
                    st.stop()

                mask = ue_df["user_upn"].astype(str).str.lower() == upn_key
                if not mask.any():
                    list_add(settings, token, "UsuariosExtras", {
                        "user_upn": upn,
                        "id_ruta": to_int_or_none(ruta),
                        "Title": upn,
                    })
                    log_action_remote(settings, token, "Usuarios", "crear", upn, user["upn"])
                    st.success("Asignación creada.")
                else:
                    sp_id = str(ue_df.loc[mask, "_sp_item_id"].iloc[0])
                    list_update(settings, token, "UsuariosExtras", sp_id, {
                        "user_upn": upn,
                        "id_ruta": to_int_or_none(ruta),
                        "Title": upn,
                    })
                    log_action_remote(settings, token, "Usuarios", "editar", upn, user["upn"])
                    st.success("Asignación actualizada.")

                refresh_data()
            except Exception as e:
                st.error(f"Error guardando asignación: {e}")

    st.subheader("Asignaciones actuales")
    if not ue_df.empty and "id_ruta" in ue_df.columns:
        try:
            ue_df["id_ruta"] = pd.to_numeric(ue_df["id_ruta"], errors="coerce").fillna(0).astype(int).astype(str)
        except Exception:
            pass
    st.dataframe(ue_df, use_container_width=True)

# =========================
# Main
# =========================

def main():
    settings = _get_settings()

    # --- Sidebar: offline & sync ---
    offline = st.sidebar.toggle(
        "Modo offline (cola local)",
        value=st.session_state.get("offline_mode", False),
        help="Si está activo, se guarda localmente y se sube después."
    )
    st.session_state["offline_mode"] = offline
    if queue_count() > 0:
        if st.sidebar.button(f"Sincronizar pendientes ({queue_count()})"):
            try:
                tok = st.session_state.get("token")
                ok, remain = sync_queue(settings, tok)
                st.sidebar.success(f"Sincronizados: {ok}. Quedan: {remain}.")
            except Exception as e:
                st.sidebar.error(f"No se pudo sincronizar: {e}")

    # Auth
    token = login_sso(settings)
    user = user_context(token, settings)
    st.sidebar.success(f"Bienvenido: {user['upn']} ({user['rol']})")

    # Load data
    dfs = load_all_lists(settings, token)

    # Menu
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

# Optional: Excel migration page (kept minimal)
def page_migrar_excel(settings, token):
    st.header("Migrar datos desde Excel a Listas de SharePoint")
    up = st.file_uploader("Excel .xlsx", type=["xlsx"])
    if not up:
        return
    xls = pd.ExcelFile(up)
    def sheet_or_empty(name):
        return pd.read_excel(xls, sheet_name=name).fillna("") if name in xls.sheet_names else pd.DataFrame()

    def migrate(list_title, df, cols, key_field):
        if df.empty:
            st.warning(f"No encontré hoja para {list_title}.")
            return 0
        ensure_list_with_columns(settings, token, list_title, cols)
        ok = 0
        for _, row in df.iterrows():
            try:
                list_add(settings, token, list_title, row.to_dict())
                ok += 1
            except Exception as e:
                st.warning(f"{list_title}: error con fila {row.to_dict()}: {e}")
        st.success(f"{list_title}: {ok} filas migradas.")
        return ok

    total = 0
    total += migrate("Rutas", sheet_or_empty("rutas"), [
        {"name":"id_ruta","number":{}}, {"name":"nombre","text":{}}, {"name":"estatus","choice":{"choices":["activo","inactivo"],"allowTextEntry":True}}], "id_ruta")
    total += migrate("Clientes", sheet_or_empty("clientes"), [
        {"name":"id_cliente","number":{}}, {"name":"nombre","text":{}}, {"name":"apellido","text":{}},
        {"name":"direccion","text":{}}, {"name":"telefono","text":{}}, {"name":"correo","text":{}},
        {"name":"comision","number":{}}, {"name":"id_ruta","number":{}}, {"name":"estatus","choice":{"choices":["activo","inactivo"],"allowTextEntry":True}}], "id_cliente")
    total += migrate("Maquinas", sheet_or_empty("maquinas"), [
        {"name":"id_maquina","number":{}}, {"name":"numero_maquina","text":{}}, {"name":"numero_permiso","text":{}},
        {"name":"tipo_maquina","text":{}}, {"name":"fondo","number":{}}, {"name":"id_ruta","number":{}}, {"name":"id_cliente","number":{}},
        {"name":"estatus","choice":{"choices":["libre","asignada","inactiva"],"allowTextEntry":True}},
        {"name":"activa","choice":{"choices":["activa","inactiva"],"allowTextEntry":True}}], "id_maquina")
    st.success(f"¡Migración terminada! Total migrado: {total} filas.")

if __name__ == "__main__":
    main()
