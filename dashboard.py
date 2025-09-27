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

# -------------------------------------------------------------
# Setup (Azure-friendly)
# -------------------------------------------------------------

st.set_page_config(page_title="Dashboard Maquinas by Nexus Solutions", page_icon="🎰", layout="wide")
PRIMARY_COLOR = "#161F35"

# Data dir for offline queue & local signatures
DEFAULT_DATA_DIR = os.environ.get("DATA_DIR") or "/home/site/wwwroot/data"
if not os.path.exists(DEFAULT_DATA_DIR):
    # Fallback when running locally
    DEFAULT_DATA_DIR = os.path.join(os.getcwd(), "data")
os.makedirs(DEFAULT_DATA_DIR, exist_ok=True)

# Local queue for offline mode
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

# -------------------------------------------------------------
# Excel storage configuration
# -------------------------------------------------------------

# We store all data in a single Excel file with multiple sheets.  The
# file location can be overridden via the DATA_FILE_PATH environment
# variable.  By default it lives in the default data directory.
DATA_FILE_PATH = os.environ.get("DATA_FILE_PATH") or os.path.join(DEFAULT_DATA_DIR, "data.xlsx")

# Sheet names for the logical tables used by the app.
SHEETS = [
    "rutas",
    "clientes",
    "maquinas",
    "capturas",
    "bitacora",
    "usuariosextras",
]

def load_excel_data() -> Dict[str, pd.DataFrame]:
    """Load all sheets from the Excel file into a dict of DataFrames."""
    dfs: Dict[str, pd.DataFrame] = {}
    if os.path.exists(DATA_FILE_PATH):
        try:
            xls = pd.ExcelFile(DATA_FILE_PATH)
            for s in SHEETS:
                if s in xls.sheet_names:
                    df = pd.read_excel(xls, sheet_name=s, dtype=str).fillna("")
                    dfs[s] = df
                else:
                    dfs[s] = pd.DataFrame()
        except Exception:
            dfs = {s: pd.DataFrame() for s in SHEETS}
    else:
        dfs = {s: pd.DataFrame() for s in SHEETS}
    return dfs

def save_excel_data(dfs: Dict[str, pd.DataFrame]) -> None:
    """Persist the provided DataFrames to the Excel file."""
    os.makedirs(os.path.dirname(DATA_FILE_PATH), exist_ok=True)
    with pd.ExcelWriter(DATA_FILE_PATH, engine="openpyxl", mode="w") as writer:
        for s in SHEETS:
            df = dfs.get(s, pd.DataFrame())
            if df is None:
                df = pd.DataFrame()
            df = df.copy().fillna("")
            df.to_excel(writer, sheet_name=s, index=False)

def append_to_sheet(dfs: Dict[str, pd.DataFrame], sheet_name: str, row: Dict) -> None:
    """Append a row to the given sheet and persist."""
    sheet = sheet_name.lower()
    if sheet not in dfs:
        dfs[sheet] = pd.DataFrame()
    df = dfs[sheet]
    # Cast None values to empty strings
    row_obj = {k: ("" if v is None else v) for k, v in row.items()}
    new_df = pd.DataFrame([row_obj])
    df = pd.concat([df, new_df], ignore_index=True)
    dfs[sheet] = df
    save_excel_data(dfs)

def update_sheet_row(dfs: Dict[str, pd.DataFrame], sheet_name: str, key_col: str, key_value, fields_update: Dict) -> bool:
    """Update a row where key_col == key_value."""
    sheet = sheet_name.lower()
    df = dfs.get(sheet, pd.DataFrame()).copy()
    if key_col not in df.columns:
        return False
    mask = df[key_col].astype(str) == str(key_value)
    if not mask.any():
        return False
    for k, v in fields_update.items():
        df.loc[mask, k] = v
    dfs[sheet] = df
    save_excel_data(dfs)
    return True

# -------------------------------------------------------------
# Settings
# -------------------------------------------------------------

def _get_settings():
    # Read environment variables for Graph/MSAL.  These remain for login only.
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
    # Mix environment values with defaults
    return {
        "tenant_id":          env_graph["tenant_id"],
        "client_id":          env_graph["client_id"],
        "client_secret":      env_graph["client_secret"],
        "hostname":           env_graph["hostname"],
        "site_path":          env_graph["site_path"],
        "firmas_library_path":env_graph["firmas_library_path"],
        "authority":          env_graph["authority"],
        "tenant_name":        env_graph["tenant_name"],
        "redirect_uri":       env_app["redirect_uri"],
        "admin_group":        env_app["admin_group"],
        "operator_group":     env_app["operator_group"],
    }

# -------------------------------------------------------------
# Auth (MSAL SSO)
# -------------------------------------------------------------

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
    # Validate discovery
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

# -------------------------------------------------------------
# Graph helpers (unused for Excel data)
# -------------------------------------------------------------

def graph_get(url: str, token: dict):
    headers = {"Authorization": f"Bearer {token['access_token']}"}
    if "$filter=" in url:
        headers["Prefer"] = "HonorNonIndexedQueriesWarningMayFailRandomly"
    r = requests.get(url, headers=headers, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"GET {url} -> {r.status_code}: {r.text[:300]}")
    return r.json()

def log_action_remote(settings, token, modulo: str, accion: str, detalle: str, user_upn: str):
    """Persist an action to the bitacora sheet locally."""
    try:
        dfs = load_excel_data()
        row = {
            "fecha_hora": datetime.now().isoformat(),
            "user_upn": user_upn,
            "modulo": modulo,
            "accion": accion,
            "detalle": (detalle or "")[:255],
        }
        append_to_sheet(dfs, "bitacora", row)
    except Exception:
        st.warning("No se pudo registrar en bitácora local.")

# -------------------------------------------------------------
# Data utils
# -------------------------------------------------------------

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
    id2name: Dict[str,str] = {}
    for _, r in df.iterrows():
        rid = r.get("id_ruta", "")
        nm  = (r.get("nombre", "") or "").strip() or f"Ruta {rid}"
        id2name[rid] = nm
    name2id = {v:k for k,v in id2name.items()}
    route_names = list(name2id.keys())
    route_names.sort()
    return route_names, name2id, id2name

# -------------------------------------------------------------
# Context & caching
# -------------------------------------------------------------

def user_context(token: dict, settings: dict) -> dict:
    try:
        me = graph_get("https://graph.microsoft.com/v1.0/me", token)
        upn = me.get("userPrincipalName", "")
        groups = graph_get("https://graph.microsoft.com/v1.0/me/memberOf?$select=displayName", token)
        names = [g.get("displayName", "") for g in groups.get("value", [])]
        rol = "admin" if settings["admin_group"] in names else ("operador" if settings["operator_group"] in names else "operador")
    except Exception:
        upn = ""
        names = []
        rol = "operador"
    return {"upn": upn, "rol": rol, "id_ruta": "", "groups": names}

@st.cache_data(show_spinner=False, ttl=5)
def load_all_lists(settings, token) -> Dict[str, pd.DataFrame]:
    return load_excel_data()

# -------------------------------------------------------------
# Offline queue sync (Excel version)
# -------------------------------------------------------------

def sync_queue(settings, token):
    q = _queue_load()
    remaining = []
    uploaded = 0
    dfs = load_excel_data()
    for entry in q:
        try:
            et = entry.get("type")
            if et == "list_add":
                list_name = entry["list_title"].lower()
                row = entry.get("fields", {})
                append_to_sheet(dfs, list_name, row)
                uploaded += 1
            elif et == "capture_batch":
                sig_path = entry.get("signature_file")
                for it in entry.get("items", []):
                    fields = deepcopy(it.get("fields", {}))
                    fields["firma_url"] = sig_path
                    append_to_sheet(dfs, "capturas", fields)
                    uploaded += 1
            else:
                remaining.append(entry)
        except Exception:
            remaining.append(entry)
    save_excel_data(dfs)
    _queue_save(remaining)
    return uploaded, len(remaining)

# -------------------------------------------------------------
# UI bits
# -------------------------------------------------------------

def show_menu_for(user):
    rol = (user.get("rol") or "").lower()
    opciones: List[str] = []
    if rol in ("admin", "operador"):
        opciones.append("Capturar Datos")
    if rol == "admin":
        opciones += ["Reportes", "Clientes", "Maquinas", "Rutas", "Usuarios", "Migrar desde Excel"]
    opciones.append("Salir")
    return st.selectbox("Menú", opciones, index=0)

# -------------------------------------------------------------
# Pages
# -------------------------------------------------------------

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
    entradas: List[Dict] = []
    if not maquinas_sub.empty:
        st.subheader("Detalle por máquina")
        for _, m in maquinas_sub.iterrows():
            mid = str(m["id_maquina"])
            cols_ui = st.columns([1, 2, 2, 3])
            with cols_ui[0]:
                st.markdown(f"**Maq {m['numero_maquina']}** (ID {mid})")
            with cols_ui[1]:
                score = st.number_input("Score", min_value=0, step=1, value=0, key=f"sc_{mid}")
            with cols_ui[2]:
                valor = st.number_input("Valor real ($)", min_value=0.0, step=1.0, value=0.0, format="%.2f", key=f"vr_{mid}")
            with cols_ui[3]:
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
        # Render signature to PNG
        from PIL import Image
        img = Image.fromarray((canvas.image_data[:, :, :3]).astype("uint8"))
        bio = io.BytesIO(); img.save(bio, format="PNG")
        firma_bytes = bio.getvalue()
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        offline_mode = st.session_state.get("offline_mode", False)
        if offline_mode:
            local_path = _save_signature_local(firma_bytes)
            caps_df = dfs.get("capturas", pd.DataFrame())
            next_id = next_id_from_df(caps_df, "id_captura")
            items = []
            for e in seleccionadas:
                valor_real = float(e["valor_real"])
                gan_cli  = round(valor_real * (comision / 100.0), 2)
                gan_own  = round(valor_real - gan_cli, 2)
                items.append({"fields": {
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
                }})
                next_id += 1
            queue_add({"type": "capture_batch", "signature_file": local_path, "items": items})
            st.success(f"Capturas en cola offline: {len(items)}. Se subirán al sincronizar.")
            return
        # Online/local: save immediately
        local_path = _save_signature_local(firma_bytes)
        caps_df = dfs.get("capturas", pd.DataFrame())
        next_id = next_id_from_df(caps_df, "id_captura")
        count_saved = 0
        for e in seleccionadas:
            valor_real = float(e["valor_real"])
            gan_cli  = round(valor_real * (comision / 100.0), 2)
            gan_own  = round(valor_real - gan_cli, 2)
            row = {
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
                "firma_url": local_path,
            }
            append_to_sheet(dfs, "capturas", row)
            next_id += 1
            count_saved += 1
        log_action_remote(settings, token, "Capturas", "crear-multiple", f"cliente={id_cliente}, maquinas={count_saved}", user["upn"])
        st.success(f"¡{count_saved} capturas guardadas correctamente!")
        refresh_data()

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
            comision_cli = st.number_input("Comisión (%)", value=40.0, step=1.0)
            ruta = st.selectbox("Ruta", ruta_opts if ruta_opts else [])
            if st.form_submit_button("Guardar", use_container_width=True):
                try:
                    new_id = next_id_from_df(clientes_df, "id_cliente")
                    row = {
                        "id_cliente": new_id,
                        "nombre": nombre, "apellido": apellido,
                        "direccion": direccion, "telefono": telefono, "correo": correo,
                        "comision": float(comision_cli), "id_ruta": to_int_or_none(ruta), "estatus": "activo",
                    }
                    append_to_sheet(dfs, "clientes", row)
                    log_action_remote(settings, token, "Clientes", "crear", nombre, user["upn"])
                    st.success("Cliente creado.")
                    refresh_data()
                except Exception as e:
                    st.error(f"Error al crear cliente: {e}")
    with tab2:
        if clientes_df.empty:
            st.info("No hay clientes.")
        else:
            sel = st.selectbox("Cliente", clientes_df["id_cliente"].astype(str) + " - " + clientes_df["nombre"].astype(str))
            if sel:
                idc = sel.split(" - ")[0]
                row = clientes_df.loc[clientes_df["id_cliente"].astype(str) == idc].iloc[0]
                with st.form("frm_edit_cliente"):
                    nombre_e = st.text_input("Nombre", row.get("nombre", ""))
                    apellido_e = st.text_input("Apellido", row.get("apellido", ""))
                    direccion_e = st.text_input("Dirección Postal", row.get("direccion", ""))
                    telefono_e = st.text_input("Teléfono", row.get("telefono", ""))
                    correo_e = st.text_input("Correo electrónico", row.get("correo", ""))
                    comision_e = st.number_input("Comisión (%)", value=float(row.get("comision", 40)), step=1.0)
                    curr_ruta = str(to_int_or_none(row.get("id_ruta", "")) or "")
                    idx_ruta = ruta_opts.index(curr_ruta) if curr_ruta in ruta_opts else 0
                    ruta_e = st.selectbox("Ruta", ruta_opts if ruta_opts else [], index=idx_ruta)
                    estatus_e = st.selectbox("Estatus", ["activo","inactivo"], index=0 if str(row.get("estatus","activo"))=="activo" else 1)
                    if st.form_submit_button("Guardar cambios", use_container_width=True):
                        try:
                            fields_update = {
                                "nombre": nombre_e, "apellido": apellido_e, "direccion": direccion_e,
                                "telefono": telefono_e, "correo": correo_e, "comision": float(comision_e),
                                "id_ruta": to_int_or_none(ruta_e), "estatus": estatus_e,
                            }
                            ok = update_sheet_row(dfs, "clientes", "id_cliente", idc, fields_update)
                            if ok:
                                log_action_remote(settings, token, "Clientes", "editar", idc, user["upn"])
                                st.success("Cliente actualizado.")
                                refresh_data()
                            else:
                                st.error("No se encontró el cliente para actualizar.")
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
                    row = {
                        "id_maquina": new_id,
                        "numero_maquina": numero_maquina,
                        "numero_permiso": numero_permiso,
                        "tipo_maquina": tipo_maquina,
                        "fondo": float(fondo),
                        "id_ruta":   to_int_or_none(ruta),
                        "id_cliente": to_int_or_none(cliente),
                        "estatus": estatus,
                        "activa": activa,
                    }
                    append_to_sheet(dfs, "maquinas", row)
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
            numero_maquina_e = st.text_input("Número de máquina", row.get("numero_maquina", ""))
            numero_permiso_e = st.text_input("Número de permiso", row.get("numero_permiso", ""))
            tipo_maquina_e   = st.text_input("Tipo de máquina", row.get("tipo_maquina", ""))
            fondo_e          = st.number_input("Fondo", min_value=0.0, step=1.0, value=float(row.get("fondo", 0)))
            curr_ruta = str(to_int_or_none(row.get("id_ruta","")) or "")
            idx_ruta  = ruta_opts.index(curr_ruta) if curr_ruta in ruta_opts else 0
            curr_cli  = str(to_int_or_none(row.get("id_cliente","")) or "")
            idx_cli   = cliente_opts.index(curr_cli) if curr_cli in cliente_opts else 0
            ruta_e    = st.selectbox("Ruta asignada", ruta_opts, index=idx_ruta)
            cliente_e = st.selectbox("Cliente", cliente_opts, index=idx_cli)
            estatus_e = st.selectbox("Estatus", ["libre", "asignada", "inactiva"], index=( ["libre","asignada","inactiva"].index(str(row.get("estatus","libre"))) if str(row.get("estatus","libre")) in ["libre","asignada","inactiva"] else 0 ))
            activa_e  = st.selectbox("Activa", ["activa", "inactiva"], index=( ["activa","inactiva"].index(str(row.get("activa","activa"))) if str(row.get("activa","activa")) in ["activa","inactiva"] else 0 ))
            if st.form_submit_button("Guardar cambios", use_container_width=True):
                try:
                    fields_update = {
                        "numero_maquina": numero_maquina_e,
                        "numero_permiso": numero_permiso_e,
                        "tipo_maquina": tipo_maquina_e,
                        "fondo": float(fondo_e),
                        "id_ruta":   to_int_or_none(ruta_e),
                        "id_cliente": to_int_or_none(cliente_e),
                        "estatus": estatus_e,
                        "activa": activa_e,
                    }
                    ok = update_sheet_row(dfs, "maquinas", "id_maquina", idm, fields_update)
                    if ok:
                        log_action_remote(settings, token, "Maquinas", "editar", idm, user["upn"])
                        st.success("Máquina actualizada.")
                        refresh_data()
                    else:
                        st.error("No se encontró la máquina para actualizar.")
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
                    row = {"id_ruta": new_id, "nombre": nombre, "estatus": "activo"}
                    append_to_sheet(dfs, "rutas", row)
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
                nombre_e = st.text_input("Nombre de ruta", row.get("nombre",""))
                estatus_e = st.selectbox("Estatus", ["activo","inactivo"], index=(0 if row.get("estatus","activo")=="activo" else 1))
                if st.form_submit_button("Guardar cambios", use_container_width=True):
                    try:
                        ok = update_sheet_row(dfs, "rutas", "id_ruta", rid, {"nombre": nombre_e, "estatus": estatus_e})
                        if ok:
                            log_action_remote(settings, token, "Rutas", "editar", rid, user["upn"])
                            st.success("Ruta actualizada.")
                            refresh_data()
                        else:
                            st.error("No se encontró la ruta para actualizar.")
                    except Exception as e:
                        st.error(f"Error actualizando ruta: {e}")

def page_usuarios(dfs, settings, user, token):
    st.header("Usuarios (asignación de rutas)")
    ue_df     = dfs.get("usuariosextras", pd.DataFrame()).copy()
    rutas_df  = dfs.get("rutas", pd.DataFrame()).copy()
    if "id_ruta" in rutas_df.columns:
        rutas_df["id_ruta_norm"] = pd.to_numeric(rutas_df["id_ruta"], errors="coerce").fillna(0).astype(int).astype(str)
    else:
        rutas_df["id_ruta_norm"] = pd.Series([], dtype=str)
    ruta_opts = rutas_df["id_ruta_norm"].tolist()
    for c in ("user_upn", "id_ruta"):
        if c not in ue_df.columns:
            ue_df[c] = pd.Series([], dtype=str)
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
                    row = {"user_upn": upn, "id_ruta": to_int_or_none(ruta)}
                    append_to_sheet(dfs, "usuariosextras", row)
                    log_action_remote(settings, token, "Usuarios", "crear", upn, user["upn"])
                    st.success("Asignación creada.")
                else:
                    ok = update_sheet_row(dfs, "usuariosextras", "user_upn", upn, {"id_ruta": to_int_or_none(ruta)})
                    if ok:
                        log_action_remote(settings, token, "Usuarios", "editar", upn, user["upn"])
                        st.success("Asignación actualizada.")
                    else:
                        st.error("No se encontró el usuario para actualizar.")
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

# -------------------------------------------------------------
# Main
# -------------------------------------------------------------

def main():
    settings = _get_settings()
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
    token = login_sso(settings)
    user = user_context(token, settings)
    st.sidebar.success(f"Bienvenido: {user['upn']} ({user['rol']})")
    dfs = load_all_lists(settings, token)
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

def page_migrar_excel(settings, token):
    st.header("Migrar datos desde Excel a Listas de SharePoint")
    up = st.file_uploader("Excel .xlsx", type=["xlsx"])
    if not up:
        return
    xls = pd.ExcelFile(up)
    def sheet_or_empty(name):
        return pd.read_excel(xls, sheet_name=name).fillna("") if name in xls.sheet_names else pd.DataFrame()
    def migrate_local(sheet_name, df):
        if df.empty:
            st.warning(f"No encontré hoja para {sheet_name}.")
            return 0
        ok = 0
        dfs = load_excel_data()
        for _, row in df.iterrows():
            try:
                append_to_sheet(dfs, sheet_name.lower(), row.to_dict())
                ok += 1
            except Exception as e:
                st.warning(f"{sheet_name}: error con fila {row.to_dict()}: {e}")
        st.success(f"{sheet_name}: {ok} filas migradas.")
        return ok
    total = 0
    total += migrate_local("rutas", sheet_or_empty("rutas"))
    total += migrate_local("clientes", sheet_or_empty("clientes"))
    total += migrate_local("maquinas", sheet_or_empty("maquinas"))
    total += migrate_local("capturas", sheet_or_empty("capturas"))
    total += migrate_local("bitacora", sheet_or_empty("bitacora"))
    total += migrate_local("usuariosextras", sheet_or_empty("usuariosextras"))
    st.success(f"¡Migración terminada! Total migrado: {total} filas.")

if __name__ == "__main__":
    main()
