"""
dashboard_pivot.py — Softys Chile · Compras Estratégicas
=========================================================
Sube el Pivot (Ariba) + Consolidado (Drive) y el sistema detecta
automáticamente qué datos están desactualizados y quién debe actualizarlos.

Instalar:  pip install streamlit pandas plotly openpyxl
Ejecutar:  streamlit run dashboard_pivot.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from io import BytesIO
import hashlib
import openpyxl
import warnings
warnings.filterwarnings("ignore")

# ──────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="Contratos · Softys", page_icon="📋",
                   layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&display=swap');
html,body,[class*="css"]{font-family:'DM Sans',sans-serif;}

section[data-testid="stSidebar"]{background:#0d1f3c;}
section[data-testid="stSidebar"] *{color:#e8eef7 !important;}
section[data-testid="stSidebar"] .stSelectbox>div>div,
section[data-testid="stSidebar"] .stMultiSelect>div>div{
  background:#1a3358 !important;border-color:#2e5490 !important;}
section[data-testid="stSidebar"] hr{border-color:#2e5490;}

/* ── Botón toggle de la sidebar siempre visible ── */
button[kind="headerNoPadding"],
[data-testid="collapsedControl"] {
    display: flex !important;
    visibility: visible !important;
    opacity: 1 !important;
    background: #1a3358 !important;
    border-radius: 0 8px 8px 0 !important;
    border: 1px solid #2e5490 !important;
    border-left: none !important;
    padding: 6px 4px !important;
    z-index: 999999 !important;
}
[data-testid="collapsedControl"] svg {
    fill: #e8eef7 !important;
    color: #e8eef7 !important;
    visibility: visible !important;
    opacity: 1 !important;
}
[data-testid="collapsedControl"] {
    position: fixed !important;
    top: 50% !important;
    left: 0 !important;
    transform: translateY(-50%) !important;
}

.kpi-row{display:grid;grid-template-columns:repeat(5,1fr);gap:11px;margin-bottom:14px;}
.kpi{background:#fff;border-radius:10px;padding:15px 13px 11px;
     border-left:4px solid #1a56db;box-shadow:0 1px 5px rgba(0,0,0,.07);}
.kpi.g{border-left-color:#059669;} .kpi.r{border-left-color:#dc2626;}
.kpi.y{border-left-color:#d97706;} .kpi.gr{border-left-color:#6b7280;}
.kpi.b{border-left-color:#2563eb;} .kpi.o{border-left-color:#ea580c;}
.kpi-lbl{font-size:.67rem;text-transform:uppercase;letter-spacing:.07em;color:#6b7280;font-weight:600;}
.kpi-val{font-size:1.85rem;font-weight:700;color:#0d1f3c;line-height:1.05;}
.kpi-sub{font-size:.69rem;color:#9ca3af;margin-top:2px;}

.alert-card{border-radius:9px;padding:13px 15px;margin-bottom:9px;font-size:.84rem;line-height:1.55;}
.alert-card.red   {background:#fef2f2;border-left:4px solid #dc2626;}
.alert-card.yellow{background:#fffbeb;border-left:4px solid #d97706;}
.alert-card.blue  {background:#eff6ff;border-left:4px solid #2563eb;}
.alert-card.green {background:#f0fdf4;border-left:4px solid #059669;}
.sec{font-size:.93rem;font-weight:700;color:#0d1f3c;border-bottom:2px solid #e5e7eb;
     padding-bottom:5px;margin:20px 0 11px;}
#MainMenu,footer,header{visibility:hidden;}
.block-container{padding-top:1.3rem;}
</style>
""", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────
# LISTAS OFICIALES DE COMPRADORES (según imágenes)
# ──────────────────────────────────────────────────────────────

# Estratégicos (imágenes 1 y 2)
ESTRATEGICOS = {
    "Bárbara García",
    "BPO",
    "Claudio Berrios",
    "Denisse Andrea Gonzalez Terrile",
    "Jorge Alfonso Urrutia Carillo",
    "Jorge Urrutia",
    "Joseph España",
    "Juan Daniel Figueroa",
    "Juan Figueroa",
    "Magdalena Farias",
    "Martina Fuentes",
    "Patricio Espinoza",
    "Viviana Grandón",
}

# Tácticos (imagen 3)
TACTICOS = {
    "BPO",
    "Dayana Dávila",
    "Joseph España",
    "Leonardo Nacarate",
    "Patricio Espinoza",
}

TODOS_COMPRADORES = ESTRATEGICOS | TACTICOS

# Mapa: nombre en Pivot → nombre canónico oficial
PIVOT_A_CANON = {
    "jorge alfonso urrutia carillo":       "Jorge Urrutia",
    "juan daniel figueroa":                "Juan Figueroa",
    "joseph eduardo españa escalona":      "Joseph España",
    "dayana dávila":                       "Dayana Dávila",
    "magdalena farias":                    "Magdalena Farias",
    "denisse andrea gonzalez terrile":     "Denisse Andrea Gonzalez Terrile",
    "bárbara garcía":                      "Bárbara García",
    "claudio berrios":                     "Claudio Berrios",
    "martina fuentes":                     "Martina Fuentes",
    "viviana grandón":                     "Viviana Grandón",
    "patricio espinoza":                   "Patricio Espinoza",
    "leandro medina":                      "Leonardo Nacarate",
    "leonardo nacarate":                   "Leonardo Nacarate",
    "dayana davila":                       "Dayana Dávila",
    # Los que aparecen en Pivot pero NO son compradores oficiales → se dejan como están
}

# ──────────────────────────────────────────────────────────────
# MAPEO DE COLUMNAS
# ──────────────────────────────────────────────────────────────
PIVOT_COL_MAP = {
    "ID de contrato":                          "id",
    "Proyecto - Nombre del proyecto":          "nombre_proyecto",
    "Fecha de inicio":                         "fecha_inicio_raw",
    "Nombre del propietario":                  "propietario_raw",
    "Código acreedor SAP":                     "cod_sap",
    "Es Indefinido":                           "indefinido_raw",
    "Región - Región (L2)":                    "region",
    "Rut empresa proveedor":                   "rut",
    "Partes afectadas - Proveedor común":      "proveedor",
    "Estado del contrato":                     "estado_ariba",
    "Fecha de expiración - Fecha":             "fecha_termino_raw",
    "Descripción":                             "descripcion",
    "Aplica Garantía":                         "garantia_ariba",
    "sum(Importe Monto total Contrato)":       "monto_total",
}

CONS_COL_MAP = {
    "Contrato Ariba":                          "id",
    "Comprador Estratégico":                   "comprador_estrat",
    "Comprador Táctico":                       "comprador_tact",
    "Estado Contrato Ariba":                   "estado_cons_ariba",
    "Estado    Contrato":                      "estado_cons_manual",
    "Fecha Término Contrato":                  "fecha_termino_cons",
    "Proveedor":                               "proveedor_cons",
    "Área":                                    "area",
    "Gerencia":                                "gerencia",
    "Aplica Boleta de Garantía (Ariba)":       "garantia_cons",
    "Estado Garantía":                         "estado_garantia",
    "Vencimiento Garantía":                    "venc_garantia",
    "Monto Garantía":                          "monto_garantia",
    "Contratos Indefinidos":                   "indefinido_cons",
    "Observación Interna":                     "obs_interna",
}

# ──────────────────────────────────────────────────────────────
# UTILIDADES
# ──────────────────────────────────────────────────────────────

def norm(v) -> str:
    return str(v).strip().lower() if pd.notna(v) and str(v).strip() not in ("", "nan") else ""

def canon(raw: str) -> str:
    """Convierte nombre del Pivot al nombre canónico oficial."""
    k = norm(raw)
    return PIVOT_A_CANON.get(k, str(raw).strip() if k else "Sin asignar")

def es_comprador_oficial(nombre: str) -> bool:
    return nombre in TODOS_COMPRADORES

def tipo_comprador(nombre: str) -> str:
    es_e = nombre in ESTRATEGICOS
    es_t = nombre in TACTICOS
    if es_e and es_t: return "Estratégico + Táctico"
    if es_e: return "Estratégico"
    if es_t: return "Táctico"
    return "No registrado"

def parse_fecha(v) -> pd.Timestamp:
    if pd.isna(v): return pd.NaT
    s = str(v).strip()
    if s in ("", "99.99.9999", "31/12/2999", "2999", "nan"): return pd.NaT
    if isinstance(v, pd.Timestamp):
        return v if v.year < 2900 else pd.NaT
    if isinstance(v, (int, float)):
        try:
            ts = pd.Timestamp("1899-12-30") + pd.Timedelta(days=int(v))
            return ts if ts.year < 2900 else pd.NaT
        except: return pd.NaT
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%m/%d/%Y", "%d.%m.%Y"):
        try:
            ts = pd.to_datetime(s, format=fmt)
            return ts if ts.year < 2900 else pd.NaT
        except: continue
    try:
        ts = pd.to_datetime(s, dayfirst=True, errors="coerce")
        return ts if pd.notna(ts) and ts.year < 2900 else pd.NaT
    except: return pd.NaT

def calcular_riesgo(estado: str, dias, es_indef: bool) -> str:
    if es_indef: return "BAJO 🟢"
    if dias is None or pd.isna(dias): return "REVISAR ⚪"
    d = int(dias)
    if estado in ("Vencido", "Cancelado", "Terminado") or d < 0: return "ALTO 🔴"
    if estado in ("Modificación del borrador", "Próximo a vencer") or d <= 60: return "MEDIO 🟡"
    if estado in ("Borrador", "En espera"): return "REVISAR ⚪"
    if d > 60: return "BAJO 🟢"
    return "REVISAR ⚪"

def fmt_m(v: float) -> str:
    if v >= 1_000_000_000: return f"${v/1_000_000_000:.1f}B"
    if v >= 1_000_000:     return f"${v/1_000_000:.1f}M"
    if v >= 1_000:         return f"${v/1_000:.0f}K"
    return f"${v:.0f}"

COL_RIESGO = {"BAJO 🟢":"#059669","MEDIO 🟡":"#d97706","ALTO 🔴":"#dc2626","REVISAR ⚪":"#6b7280"}
COL_SYNC   = {"OK":"#059669","DESACTUALIZADO":"#dc2626","NUEVO":"#2563eb","REVISAR":"#d97706"}
BG_SYNC    = {"OK":"#f0fdf4","DESACTUALIZADO":"#fef2f2","NUEVO":"#eff6ff","REVISAR":"#fffbeb"}
FG_SYNC    = {"OK":"#065f46","DESACTUALIZADO":"#991b1b","NUEVO":"#1e40af","REVISAR":"#92400e"}

# ──────────────────────────────────────────────────────────────
# CARGA Y TRANSFORMACIÓN
# ──────────────────────────────────────────────────────────────

def detectar_header(content: bytes) -> int:
    df_scan = pd.read_excel(BytesIO(content), sheet_name="Data",
                             header=None, nrows=25, engine="openpyxl")
    for i, row in df_scan.iterrows():
        if "ID de contrato" in row.values:
            return i
    raise ValueError("No se encontró 'ID de contrato' en la hoja Data.")

@st.cache_data(show_spinner=False)
def cargar_pivot(fhash: str, content: bytes) -> pd.DataFrame:
    hi = detectar_header(content)
    df = pd.read_excel(BytesIO(content), sheet_name="Data", header=hi, engine="openpyxl")
    df = df.dropna(how="all").dropna(axis=1, how="all").reset_index(drop=True)
    df = df[~df["Estado del contrato"].astype(str).str.lower().str.strip().isin(["cerrado","cerrados"])]
    df = df.rename(columns={k: v for k, v in PIVOT_COL_MAP.items() if k in df.columns})
    df["id"] = df["id"].astype(str).str.strip()

    for rw, pr in [("fecha_inicio_raw","fecha_inicio"),("fecha_termino_raw","fecha_termino")]:
        if rw in df.columns:
            df[pr] = df[rw].apply(parse_fecha)

    # Nombre canónico del propietario
    df["comprador_canon"] = df["propietario_raw"].apply(canon) if "propietario_raw" in df.columns else "Sin asignar"
    df["es_oficial"]      = df["comprador_canon"].apply(es_comprador_oficial)
    df["tipo_comprador"]  = df["comprador_canon"].apply(tipo_comprador)

    hoy = pd.Timestamp.today().normalize()
    df["dias_venc"] = (df["fecha_termino"] - hoy).dt.days if "fecha_termino" in df.columns else None

    def _indef(row):
        if norm(row.get("indefinido_raw","")) in ("sí","si","yes","1","true","indefinido"): return True
        ft = row.get("fecha_termino")
        return pd.notna(ft) and isinstance(ft, pd.Timestamp) and ft.year > 2100

    df["es_indefinido"] = df.apply(_indef, axis=1)
    df["riesgo"]        = df.apply(lambda r: calcular_riesgo(
        str(r.get("estado_ariba","")), r.get("dias_venc"), r.get("es_indefinido",False)), axis=1)
    df["tiene_garantia"] = df["garantia_ariba"].apply(
        lambda v: norm(v) in ("sí","si","yes")) if "garantia_ariba" in df.columns else False

    if "monto_total" in df.columns:
        df["monto_total"] = pd.to_numeric(df["monto_total"], errors="coerce").fillna(0)
    if "fecha_inicio" in df.columns:
        df["anio_inicio"] = df["fecha_inicio"].dt.year

    return df[df["id"].notna() & (df["id"] != "nan") & (df["id"] != "")].reset_index(drop=True)


@st.cache_data(show_spinner=False)
def cargar_consolidado(fhash: str, content: bytes) -> pd.DataFrame:
    wb = openpyxl.load_workbook(BytesIO(content), data_only=True)
    if "Consolidado de Contratos" not in wb.sheetnames:
        raise ValueError("No se encontró la hoja 'Consolidado de Contratos'.")
    ws  = wb["Consolidado de Contratos"]
    rows = list(ws.iter_rows(values_only=True))
    df  = pd.DataFrame(rows[1:], columns=rows[0]).dropna(how="all").reset_index(drop=True)
    df  = df.rename(columns={k: v for k, v in CONS_COL_MAP.items() if k in df.columns})
    df["id"] = df["id"].astype(str).str.strip() if "id" in df.columns else ""
    for col in ("fecha_termino_cons","venc_garantia"):
        if col in df.columns:
            df[col] = df[col].apply(parse_fecha)
    return df[df["id"].notna() & (df["id"] != "") & (df["id"] != "nan")].reset_index(drop=True)


# ──────────────────────────────────────────────────────────────
# MOTOR DE COMPARACIÓN
# ──────────────────────────────────────────────────────────────

def comparar(df_p: pd.DataFrame, df_c: pd.DataFrame) -> pd.DataFrame:
    ids_cons = set(df_c["id"].astype(str))
    merged = df_p.merge(df_c, on="id", how="left", suffixes=("","_c"))

    merged["es_nuevo"] = ~merged["id"].isin(ids_cons)

    merged["dif_estado"] = merged.apply(
        lambda r: not r["es_nuevo"] and
                  norm(r.get("estado_ariba","")) != norm(r.get("estado_cons_ariba","")), axis=1)

    def _dif_fecha(r):
        if r["es_nuevo"]: return False
        fp, fc = r.get("fecha_termino"), r.get("fecha_termino_cons")
        if pd.isna(fp) or pd.isna(fc): return False
        if not (isinstance(fp, pd.Timestamp) and isinstance(fc, pd.Timestamp)): return False
        return abs((fp - fc).days) > 1
    merged["dif_fecha"] = merged.apply(_dif_fecha, axis=1)

    merged["dif_proveedor"] = merged.apply(
        lambda r: not r["es_nuevo"] and
                  norm(r.get("proveedor","")) != norm(r.get("proveedor_cons","")), axis=1)

    merged["dif_garantia"] = merged.apply(
        lambda r: not r["es_nuevo"] and
                  norm(r.get("garantia_ariba","")) != norm(r.get("garantia_cons","")), axis=1)

    def _status(r):
        if r["es_nuevo"]:                        return "NUEVO"
        if r["dif_estado"] or r["dif_fecha"]:    return "DESACTUALIZADO"
        if r["dif_proveedor"] or r["dif_garantia"]: return "REVISAR"
        return "OK"
    merged["sync_status"] = merged.apply(_status, axis=1)

    def _cambios(r):
        if r["es_nuevo"]:
            return "🆕 Contrato nuevo en Ariba — no existe en el Consolidado"
        msgs = []
        if r["dif_estado"]:
            ea = r.get("estado_ariba","—")
            ec = r.get("estado_cons_ariba","—") or "vacío"
            msgs.append(f"📄 Estado: Ariba=«{ea}» / Consolidado=«{ec}»")
        if r["dif_fecha"]:
            fp = r.get("fecha_termino")
            fc = r.get("fecha_termino_cons")
            fps = fp.strftime("%d/%m/%Y") if pd.notna(fp) else "—"
            fcs = fc.strftime("%d/%m/%Y") if pd.notna(fc) else "—"
            msgs.append(f"📅 Fecha término: Ariba={fps} / Consolidado={fcs}")
        if r["dif_proveedor"]:
            msgs.append(f"🏢 Proveedor: «{r.get('proveedor','—')}» ≠ «{r.get('proveedor_cons','—')}»")
        if r["dif_garantia"]:
            msgs.append(f"🔒 Garantía: Ariba=«{r.get('garantia_ariba','—')}» / Consolidado=«{r.get('garantia_cons','—')}»")
        return " | ".join(msgs) if msgs else "✅ Sincronizado"
    merged["cambios"] = merged.apply(_cambios, axis=1)

    return merged

# ──────────────────────────────────────────────────────────────
# SIDEBAR
# ──────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("""
    <div style="padding:8px 0 16px;border-bottom:1px solid #2e5490;">
      <div style="font-size:1.1rem;font-weight:700;">📋 Contratos · Softys</div>
      <div style="font-size:.68rem;opacity:.6;margin-top:2px;">Compras Estratégicas Chile</div>
    </div>
    <div style="height:12px"></div>
    """, unsafe_allow_html=True)

    st.markdown("**📁 Pivot Ariba** *(obligatorio)*")
    up_pivot = st.file_uploader("Pivot", type=["xlsx","xls"], key="piv", label_visibility="collapsed")
    st.caption("Descarga directa de SAP Ariba Analysis")

    st.markdown("**📄 Consolidado Drive** *(para sincronización)*")
    up_cons = st.file_uploader("Consolidado", type=["xlsx","xls"], key="con", label_visibility="collapsed")
    st.caption("Archivo del SharePoint · opcional")

    st.markdown("---")
    filtros_ph = st.empty()

# ──────────────────────────────────────────────────────────────
# PANTALLA BIENVENIDA
# ──────────────────────────────────────────────────────────────

if not up_pivot:
    st.markdown("""
    <div style="display:flex;flex-direction:column;align-items:center;
         justify-content:center;padding:60px 30px;text-align:center;">
      <div style="font-size:2.8rem;margin-bottom:12px;">📋</div>
      <h1 style="font-size:1.6rem;font-weight:700;color:#0d1f3c;margin-bottom:8px;">
        Dashboard de Gestión de Contratos
      </h1>
      <p style="color:#6b7280;max-width:480px;line-height:1.6;margin-bottom:26px;">
        Sube el <strong>Pivot de Ariba</strong> para ver indicadores.<br>
        Agrega el <strong>Consolidado del Drive</strong> para detectar qué está desactualizado
        y quién debe actualizarlo.
      </p>
      <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;max-width:580px;">
        <div style="background:#f0fdf4;border-radius:10px;padding:13px;border:1px solid #d1fae5;">
          <div style="font-size:1.3rem">📊</div>
          <div style="font-weight:600;font-size:.8rem;margin-top:5px;">10 KPIs automáticos</div>
        </div>
        <div style="background:#eff6ff;border-radius:10px;padding:13px;border:1px solid #dbeafe;">
          <div style="font-size:1.3rem">🔄</div>
          <div style="font-weight:600;font-size:.8rem;margin-top:5px;">Sincronización campo a campo</div>
        </div>
        <div style="background:#fefce8;border-radius:10px;padding:13px;border:1px solid #fde68a;">
          <div style="font-size:1.3rem">👤</div>
          <div style="font-weight:600;font-size:.8rem;margin-top:5px;">Alertas por comprador</div>
        </div>
      </div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

# ──────────────────────────────────────────────────────────────
# CARGA DE DATOS
# ──────────────────────────────────────────────────────────────

up_pivot.seek(0); piv_bytes = up_pivot.read()
with st.spinner("🔄 Procesando Pivot de Ariba..."):
    try:
        df_piv = cargar_pivot(hashlib.md5(piv_bytes).hexdigest(), piv_bytes)
    except Exception as e:
        st.error(f"❌ Error al leer el Pivot: {e}"); st.stop()

df_cons_raw = None
if up_cons:
    up_cons.seek(0); cons_bytes = up_cons.read()
    with st.spinner("🔄 Cargando Consolidado..."):
        try:
            df_cons_raw = cargar_consolidado(hashlib.md5(cons_bytes).hexdigest(), cons_bytes)
        except Exception as e:
            st.warning(f"⚠️ No se pudo leer el Consolidado: {e}")

# ──────────────────────────────────────────────────────────────
# FILTROS
# ──────────────────────────────────────────────────────────────

with st.sidebar:
    with filtros_ph.container():
        st.markdown("**🎛️ Filtros**")

        mostrar_solo_oficiales = st.checkbox("Solo compradores oficiales", value=True,
            help="Filtra contratos cuyo propietario en Ariba es un comprador registrado")

        f_riesgo  = st.selectbox("🚦 Riesgo", ["Todos"] + sorted(df_piv["riesgo"].dropna().unique()))
        f_tipo    = st.selectbox("👥 Tipo comprador",
                                  ["Todos","Estratégico","Táctico","Estratégico + Táctico","No registrado"])

        compradores_lista = sorted(df_piv["comprador_canon"].dropna().unique().tolist())
        f_comp    = st.selectbox("👤 Comprador", ["Todos"] + compradores_lista)
        f_estado  = st.selectbox("📄 Estado Ariba",
                                  ["Todos"] + sorted(df_piv["estado_ariba"].dropna().unique()))
        f_gar     = st.selectbox("🔒 Garantía", ["Todas","Con garantía","Sin garantía"])
        f_indef   = st.selectbox("♾️ Indefinidos", ["Todos","Solo indefinidos","Solo con fecha"])

        st.markdown("---")
        st.caption(f"📁 {up_pivot.name}\n{len(df_piv):,} contratos activos")
        if df_cons_raw is not None:
            st.caption(f"📄 {up_cons.name}\n{len(df_cons_raw):,} filas en Consolidado")

# Aplicar filtros
df = df_piv.copy()
if mostrar_solo_oficiales:    df = df[df["es_oficial"]]
if f_riesgo != "Todos":       df = df[df["riesgo"] == f_riesgo]
if f_tipo   != "Todos":       df = df[df["tipo_comprador"] == f_tipo]
if f_comp   != "Todos":       df = df[df["comprador_canon"] == f_comp]
if f_estado != "Todos":       df = df[df["estado_ariba"] == f_estado]
if f_gar == "Con garantía":   df = df[df["tiene_garantia"]]
elif f_gar == "Sin garantía": df = df[~df["tiene_garantia"]]
if f_indef == "Solo indefinidos": df = df[df["es_indefinido"]]
elif f_indef == "Solo con fecha": df = df[~df["es_indefinido"]]

if df.empty:
    st.warning("⚠️ Ningún contrato coincide con los filtros."); st.stop()

# ──────────────────────────────────────────────────────────────
# ENCABEZADO
# ──────────────────────────────────────────────────────────────

sync_pill = ""
if df_cons_raw is not None:
    sync_pill = '<span style="background:#dbeafe;color:#1e40af;border-radius:99px;padding:2px 10px;font-size:.7rem;font-weight:700;margin-left:10px;">🔄 Sincronización activa</span>'

st.markdown(f"""
<div style="display:flex;justify-content:space-between;align-items:flex-end;margin-bottom:5px;">
  <div>
    <h1 style="font-size:1.4rem;font-weight:700;color:#0d1f3c;margin:0;line-height:1.2;">
      Dashboard de Gestión de Contratos{sync_pill}
    </h1>
    <div style="color:#6b7280;font-size:.77rem;margin-top:2px;">
      Softys Chile · Fuente: SAP Ariba · {df["id"].nunique():,} contratos activos en vista
    </div>
  </div>
  <div style="font-size:.7rem;color:#9ca3af;">{datetime.now().strftime('%d/%m/%Y %H:%M')}</div>
</div>
""", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────
# TABS
# ──────────────────────────────────────────────────────────────

if df_cons_raw is not None:
    tab_kpi, tab_sync, tab_comp, tab_prov, tab_gar_tab, tab_exp = st.tabs([
        "📊 Resumen & KPIs", "🔄 Sincronización", "📌 Por Comprador",
        "🏢 Proveedores", "🔒 Garantías", "🔍 Explorador"])
else:
    tab_kpi, tab_comp, tab_prov, tab_gar_tab, tab_exp = st.tabs([
        "📊 Resumen & KPIs", "📌 Por Comprador",
        "🏢 Proveedores", "🔒 Garantías", "🔍 Explorador"])
    tab_sync = None

# ══════════════════════════════════════════════
# TAB: KPIs & RESUMEN
# ══════════════════════════════════════════════
with tab_kpi:
    total   = len(df)
    bajo    = (df["riesgo"] == "BAJO 🟢").sum()
    medio   = (df["riesgo"] == "MEDIO 🟡").sum()
    alto    = (df["riesgo"] == "ALTO 🔴").sum()
    revisar = (df["riesgo"] == "REVISAR ⚪").sum()
    indef   = df["es_indefinido"].sum()
    gar     = df["tiene_garantia"].sum()
    monto   = df["monto_total"].sum() if "monto_total" in df.columns else 0
    pct_v   = f"{bajo/total*100:.0f}%" if total else "—"

    st.markdown(f"""
    <div class="kpi-row">
      <div class="kpi"><div class="kpi-lbl">📋 Total contratos</div>
        <div class="kpi-val">{total:,}</div><div class="kpi-sub">activos (sin cerrados)</div></div>
      <div class="kpi g"><div class="kpi-lbl">✅ Vigentes</div>
        <div class="kpi-val">{bajo:,}</div><div class="kpi-sub">{pct_v} del total</div></div>
      <div class="kpi y"><div class="kpi-lbl">⚠️ Riesgo medio</div>
        <div class="kpi-val">{medio:,}</div><div class="kpi-sub">vencen ≤ 60 días</div></div>
      <div class="kpi r"><div class="kpi-lbl">🚨 Riesgo alto</div>
        <div class="kpi-val">{alto:,}</div><div class="kpi-sub">vencidos / cancelados</div></div>
      <div class="kpi gr"><div class="kpi-lbl">🔍 Por revisar</div>
        <div class="kpi-val">{revisar:,}</div><div class="kpi-sub">borrador / sin fecha</div></div>
    </div>
    <div class="kpi-row">
      <div class="kpi"><div class="kpi-lbl">♾️ Indefinidos</div>
        <div class="kpi-val">{indef:,}</div><div class="kpi-sub">sin fecha de término</div></div>
      <div class="kpi g"><div class="kpi-lbl">🔒 Con garantía</div>
        <div class="kpi-val">{gar:,}</div><div class="kpi-sub">aplica boleta Ariba</div></div>
      <div class="kpi"><div class="kpi-lbl">💰 Monto total</div>
        <div class="kpi-val">{fmt_m(monto)}</div><div class="kpi-sub">CLP contratos filtrados</div></div>
      <div class="kpi"><div class="kpi-lbl">👤 Compradores</div>
        <div class="kpi-val">{df["comprador_canon"].nunique():,}</div><div class="kpi-sub">propietarios únicos</div></div>
      <div class="kpi"><div class="kpi-lbl">🏢 Proveedores</div>
        <div class="kpi-val">{df["proveedor"].nunique() if "proveedor" in df.columns else 0:,}</div>
        <div class="kpi-sub">proveedores únicos</div></div>
    </div>
    """, unsafe_allow_html=True)

    c1, c2, c3 = st.columns([1, 1.4, 1.2])
    with c1:
        d = df["riesgo"].value_counts().reset_index(); d.columns = ["r","n"]
        fig = go.Figure(go.Pie(labels=d["r"], values=d["n"], hole=0.55,
            marker_colors=[COL_RIESGO.get(r,"#999") for r in d["r"]],
            textinfo="percent+value", textfont=dict(size=11,family="DM Sans"),
            hovertemplate="<b>%{label}</b><br>%{value} (%{percent})<extra></extra>"))
        fig.update_layout(title=dict(text="Distribución de riesgo",font=dict(size=13,color="#0d1f3c"),x=0.02),
            legend=dict(font=dict(size=9),orientation="h",y=-0.15),
            paper_bgcolor="white",margin=dict(t=38,b=40,l=8,r=8),height=275)
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        if "fecha_termino" in df.columns:
            hoy = pd.Timestamp.today().normalize()
            df_tl = df[df["fecha_termino"].notna() & df["dias_venc"].between(-30,180)].copy()
            if not df_tl.empty:
                df_tl["mes"] = df_tl["fecha_termino"].dt.to_period("M").astype(str)
                agr = df_tl.groupby(["mes","riesgo"]).size().reset_index(name="n")
                fig2 = px.bar(agr, x="mes", y="n", color="riesgo",
                    color_discrete_map=COL_RIESGO, barmode="stack",
                    title="Vencimientos próximos 6 meses",
                    labels={"mes":"Mes","n":"Contratos","riesgo":"Riesgo"})
                fig2.update_layout(paper_bgcolor="white",plot_bgcolor="white",
                    font=dict(family="DM Sans",size=10),
                    title=dict(font=dict(size=13,color="#0d1f3c")),
                    xaxis=dict(tickangle=-30,gridcolor="#f3f4f6"),
                    yaxis=dict(gridcolor="#f3f4f6"),
                    legend=dict(orientation="h",y=-0.28,font=dict(size=9)),
                    margin=dict(t=38,b=65,l=8,r=8),height=275)
                st.plotly_chart(fig2, use_container_width=True)

    with c3:
        d3 = df["estado_ariba"].value_counts().reset_index(); d3.columns = ["e","n"]
        fig3 = px.bar(d3, x="n", y="e", orientation="h", title="Por estado Ariba",
            color="n", color_continuous_scale="Blues", labels={"n":"","e":""})
        fig3.update_layout(paper_bgcolor="white",plot_bgcolor="white",
            font=dict(family="DM Sans",size=10),
            title=dict(font=dict(size=13,color="#0d1f3c")),
            yaxis=dict(categoryorder="total ascending",tickfont=dict(size=9)),
            coloraxis_showscale=False,margin=dict(t=38,b=10,l=8,r=8),height=275)
        st.plotly_chart(fig3, use_container_width=True)

    st.markdown('<div class="sec">🚨 Contratos que requieren acción inmediata</div>', unsafe_allow_html=True)
    df_alt = df[df["riesgo"].isin(["ALTO 🔴","MEDIO 🟡"])].copy()
    if not df_alt.empty:
        cols_a = [c for c in ["id","proveedor","comprador_canon","estado_ariba","dias_venc","riesgo","tiene_garantia"] if c in df_alt]
        ren    = {"id":"Contrato","proveedor":"Proveedor","comprador_canon":"Comprador",
                  "estado_ariba":"Estado Ariba","dias_venc":"Días","riesgo":"Riesgo","tiene_garantia":"Garantía"}
        tbl = df_alt[cols_a].rename(columns=ren).sort_values("Días")
        def hl(v):
            if "ALTO"  in str(v): return "background:#fef2f2;color:#991b1b;font-weight:600"
            if "MEDIO" in str(v): return "background:#fffbeb;color:#92400e;font-weight:600"
            return ""
        st.dataframe(tbl.style.map(hl, subset=["Riesgo"] if "Riesgo" in tbl.columns else [])
                     .format({"Días":"{:.0f}"}, na_rep="—"),
                     use_container_width=True, height=240)
        a1,a2 = st.columns(2)
        a1.error(f"🚨 **{(df['riesgo']=='ALTO 🔴').sum()}** contratos en riesgo ALTO — acción urgente")
        a2.warning(f"⚠️ **{(df['riesgo']=='MEDIO 🟡').sum()}** contratos próximos a vencer")
    else:
        st.success("✅ Sin contratos críticos con los filtros actuales.")


# ══════════════════════════════════════════════
# TAB: SINCRONIZACIÓN
# ══════════════════════════════════════════════
if tab_sync is not None:
    with tab_sync:
        st.markdown("""
        <div class="alert-card blue">
          <strong>¿Cómo funciona?</strong><br>
          Compara el <strong>Pivot de Ariba</strong> (fuente oficial, siempre al día) contra el
          <strong>Consolidado del Drive</strong> (editado manualmente por los compradores).
          Detecta campo a campo qué contratos tienen datos distintos y le dice a cada comprador
          exactamente qué debe actualizar en el Drive.
        </div>
        """, unsafe_allow_html=True)

        with st.spinner("🔄 Comparando archivos..."):
            df_cmp = comparar(df_piv, df_cons_raw)

        n_ok    = (df_cmp["sync_status"] == "OK").sum()
        n_desact= (df_cmp["sync_status"] == "DESACTUALIZADO").sum()
        n_nuevo = (df_cmp["sync_status"] == "NUEVO").sum()
        n_rev   = (df_cmp["sync_status"] == "REVISAR").sum()
        total_c = len(df_cmp)
        pct_ok  = f"{n_ok/total_c*100:.0f}%" if total_c else "—"

        st.markdown(f"""
        <div class="kpi-row">
          <div class="kpi g"><div class="kpi-lbl">✅ Sincronizados</div>
            <div class="kpi-val">{n_ok:,}</div><div class="kpi-sub">{pct_ok} del Pivot</div></div>
          <div class="kpi r"><div class="kpi-lbl">⚠️ Desactualizados</div>
            <div class="kpi-val">{n_desact:,}</div><div class="kpi-sub">estado o fecha difieren</div></div>
          <div class="kpi b"><div class="kpi-lbl">🆕 Nuevos en Ariba</div>
            <div class="kpi-val">{n_nuevo:,}</div><div class="kpi-sub">no están en el Drive</div></div>
          <div class="kpi y"><div class="kpi-lbl">🔍 Revisar</div>
            <div class="kpi-val">{n_rev:,}</div><div class="kpi-sub">proveedor / garantía</div></div>
          <div class="kpi"><div class="kpi-lbl">📊 Total comparados</div>
            <div class="kpi-val">{total_c:,}</div><div class="kpi-sub">contratos en Pivot activo</div></div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown('<div class="sec">🔔 Alertas por comprador — qué debe actualizar cada uno</div>',
                    unsafe_allow_html=True)

        problemas = df_cmp[df_cmp["sync_status"] != "OK"].copy()
        problemas["comprador_canon"] = problemas["propietario_raw"].apply(canon) if "propietario_raw" in problemas.columns else "Sin asignar"
        compradores_con_prob = sorted(problemas["comprador_canon"].dropna().unique())

        if not compradores_con_prob:
            st.success("🎉 ¡El Consolidado está completamente sincronizado con el Pivot de Ariba!")
        else:
            f_sync_comp = st.selectbox("Ver alertas de:", ["Todos los compradores"] + compradores_con_prob,
                                        key="f_sync_comp")
            df_prob_view = problemas if f_sync_comp == "Todos los compradores" else problemas[problemas["comprador_canon"] == f_sync_comp]

            for comp in (compradores_con_prob if f_sync_comp == "Todos los compradores" else [f_sync_comp]):
                grp = df_prob_view[df_prob_view["comprador_canon"] == comp]
                if grp.empty: continue

                n_grp = len(grp)
                n_d   = (grp["sync_status"] == "DESACTUALIZADO").sum()
                n_n   = (grp["sync_status"] == "NUEVO").sum()
                n_r   = (grp["sync_status"] == "REVISAR").sum()
                tipo  = tipo_comprador(comp)
                es_of = es_comprador_oficial(comp)
                badge_tipo = f"<span style='background:#e0f2fe;color:#0369a1;border-radius:6px;padding:1px 7px;font-size:.68rem;margin-left:6px;'>{tipo}</span>" if es_of else "<span style='background:#fef9c3;color:#854d0e;border-radius:6px;padding:1px 7px;font-size:.68rem;margin-left:6px;'>No registrado</span>"

                severity = "red" if n_d > 0 or n_n > 0 else "yellow"
                partes = []
                if n_d: partes.append(f"<strong>{n_d}</strong> desactualizados (estado/fecha diferente)")
                if n_n: partes.append(f"<strong>{n_n}</strong> contratos nuevos en Ariba sin registrar")
                if n_r: partes.append(f"<strong>{n_r}</strong> por revisar (proveedor/garantía)")
                resumen_html = " · ".join(partes)

                with st.expander(f"👤 {comp}{badge_tipo}  —  {n_grp} contrato(s) con diferencias", expanded=(n_d+n_n > 0)):
                    st.markdown(f"""
                    <div class="alert-card {severity}">
                      <strong>Acción requerida para {comp}:</strong><br>{resumen_html}
                    </div>
                    """, unsafe_allow_html=True)

                    cols_det = [c for c in ["id","proveedor","estado_ariba","estado_cons_ariba",
                                             "fecha_termino","fecha_termino_cons",
                                             "sync_status","cambios"] if c in grp.columns]
                    ren_det  = {"id":"Contrato","proveedor":"Proveedor",
                                "estado_ariba":"Estado Ariba (oficial)",
                                "estado_cons_ariba":"Estado en Drive",
                                "fecha_termino":"Fecha Ariba",
                                "fecha_termino_cons":"Fecha en Drive",
                                "sync_status":"Estado Sync","cambios":"Qué actualizar"}
                    tbl_det = grp[cols_det].rename(columns=ren_det)

                    def hl_sync_row(val):
                        bg = BG_SYNC.get(str(val),"")
                        fg = FG_SYNC.get(str(val),"")
                        return f"background:{bg};color:{fg};font-weight:600" if bg else ""

                    styled_det = tbl_det.style.map(hl_sync_row,
                        subset=["Estado Sync"] if "Estado Sync" in tbl_det.columns else [])
                    st.dataframe(styled_det, use_container_width=True, height=min(300, 60 + len(grp)*38))

        st.markdown('<div class="sec">📊 Estado de sincronización por comprador</div>', unsafe_allow_html=True)
        df_cmp["comprador_canon"] = df_cmp["propietario_raw"].apply(canon) if "propietario_raw" in df_cmp.columns else "Sin asignar"
        sc = df_cmp.groupby(["comprador_canon","sync_status"]).size().reset_index(name="n")
        fig_sc = px.bar(sc, y="comprador_canon", x="n", color="sync_status",
            color_discrete_map={"OK":"#059669","DESACTUALIZADO":"#dc2626","NUEVO":"#2563eb","REVISAR":"#d97706"},
            barmode="stack", orientation="h",
            labels={"comprador_canon":"","n":"Contratos","sync_status":"Estado"})
        fig_sc.update_layout(paper_bgcolor="white", plot_bgcolor="white",
            font=dict(family="DM Sans",size=10),
            xaxis=dict(gridcolor="#f3f4f6"),
            yaxis=dict(categoryorder="total ascending",tickfont=dict(size=9)),
            legend=dict(orientation="h",y=-0.12,font=dict(size=9)),
            height=max(280, df_cmp["comprador_canon"].nunique()*24),
            margin=dict(t=10,b=60,l=10,r=10))
        st.plotly_chart(fig_sc, use_container_width=True)

        st.markdown('<div class="sec">📥 Exportar reporte de sincronización</div>', unsafe_allow_html=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        cols_exp = [c for c in ["id","proveedor","comprador_canon","estado_ariba","estado_cons_ariba",
                                  "fecha_termino","fecha_termino_cons","sync_status","cambios"] if c in df_cmp.columns]
        ren_exp  = {"id":"Contrato","proveedor":"Proveedor","comprador_canon":"Comprador",
                    "estado_ariba":"Estado Ariba","estado_cons_ariba":"Estado Drive",
                    "fecha_termino":"Fecha Ariba","fecha_termino_cons":"Fecha Drive",
                    "sync_status":"Estado Sync","cambios":"Qué actualizar"}

        e1,e2,e3 = st.columns(3)
        with e1:
            pend = df_cmp[df_cmp["sync_status"] != "OK"][cols_exp].rename(columns=ren_exp)
            st.download_button("⚠️ Todos los pendientes", pend.to_csv(index=False).encode("utf-8-sig"),
                               f"sync_pendientes_{ts}.csv", mime="text/csv")
        with e2:
            nuev = df_cmp[df_cmp["sync_status"]=="NUEVO"][cols_exp].rename(columns=ren_exp)
            st.download_button("🆕 Solo contratos nuevos", nuev.to_csv(index=False).encode("utf-8-sig"),
                               f"sync_nuevos_{ts}.csv", mime="text/csv")
        with e3:
            desact = df_cmp[df_cmp["sync_status"]=="DESACTUALIZADO"][cols_exp].rename(columns=ren_exp)
            st.download_button("🔄 Estado/fecha diferente", desact.to_csv(index=False).encode("utf-8-sig"),
                               f"sync_desact_{ts}.csv", mime="text/csv")


# ══════════════════════════════════════════════
# TAB: POR COMPRADOR
# ══════════════════════════════════════════════
with tab_comp:
    dc = df.groupby(["comprador_canon","riesgo"]).size().reset_index(name="n")
    orden = df["comprador_canon"].value_counts().index.tolist()
    dc["comprador_canon"] = pd.Categorical(dc["comprador_canon"], categories=orden[::-1], ordered=True)
    dc = dc.sort_values("comprador_canon")
    fig_c = px.bar(dc, y="comprador_canon", x="n", color="riesgo", color_discrete_map=COL_RIESGO,
        barmode="stack", orientation="h",
        title="Contratos por comprador", labels={"comprador_canon":"","n":"Contratos","riesgo":"Riesgo"})
    fig_c.update_layout(paper_bgcolor="white", plot_bgcolor="white",
        font=dict(family="DM Sans",size=10), title=dict(font=dict(size=13,color="#0d1f3c")),
        xaxis=dict(gridcolor="#f3f4f6"),
        yaxis=dict(tickfont=dict(size=9)),
        legend=dict(orientation="h",y=-0.12,font=dict(size=9)),
        height=max(260, df["comprador_canon"].nunique()*26),
        margin=dict(t=38,b=60,l=10,r=10))
    st.plotly_chart(fig_c, use_container_width=True)

    resumen = df.groupby(["comprador_canon","tipo_comprador"]).agg(
        Contratos    =("id","count"),
        Riesgo_Alto  =("riesgo", lambda x: (x=="ALTO 🔴").sum()),
        Riesgo_Medio =("riesgo", lambda x: (x=="MEDIO 🟡").sum()),
        Vigentes     =("riesgo", lambda x: (x=="BAJO 🟢").sum()),
        Indefinidos  =("es_indefinido","sum"),
        Con_Garantia =("tiene_garantia","sum"),
        Monto        =("monto_total","sum") if "monto_total" in df.columns else ("id","count"),
    ).reset_index().sort_values("Contratos", ascending=False)
    resumen.rename(columns={"comprador_canon":"Comprador","tipo_comprador":"Tipo"}, inplace=True)
    if "Monto" in resumen.columns:
        resumen["Monto"] = resumen["Monto"].apply(fmt_m)
    st.dataframe(resumen, use_container_width=True, height=320)


# ══════════════════════════════════════════════
# TAB: PROVEEDORES
# ══════════════════════════════════════════════
with tab_prov:
    c1,c2 = st.columns(2)
    with c1:
        if "proveedor" in df.columns:
            top = df["proveedor"].value_counts().head(15).reset_index()
            top.columns = ["Proveedor","n"]; top["Proveedor"] = top["Proveedor"].str[:45]
            fig_p = px.bar(top, y="Proveedor", x="n", orientation="h",
                title="Top 15 proveedores", color="n", color_continuous_scale="Teal",
                labels={"n":"","Proveedor":""})
            fig_p.update_layout(paper_bgcolor="white", plot_bgcolor="white",
                font=dict(family="DM Sans",size=10), title=dict(font=dict(size=13,color="#0d1f3c")),
                yaxis=dict(categoryorder="total ascending",tickfont=dict(size=9)),
                coloraxis_showscale=False, height=380, margin=dict(t=38,b=10))
            st.plotly_chart(fig_p, use_container_width=True)
    with c2:
        if "proveedor" in df.columns:
            pv = df[df["riesgo"]=="ALTO 🔴"].groupby("proveedor").size().reset_index(name="n")
            pv = pv.sort_values("n", ascending=False).head(15)
            pv["proveedor"] = pv["proveedor"].str[:45]
            if not pv.empty:
                fig_pv = px.bar(pv, y="proveedor", x="n", orientation="h",
                    title="Proveedores con más contratos vencidos",
                    color="n", color_continuous_scale="Reds", labels={"proveedor":"","n":""})
                fig_pv.update_layout(paper_bgcolor="white", plot_bgcolor="white",
                    font=dict(family="DM Sans",size=10), title=dict(font=dict(size=13,color="#0d1f3c")),
                    yaxis=dict(categoryorder="total ascending",tickfont=dict(size=9)),
                    coloraxis_showscale=False, height=380, margin=dict(t=38,b=10))
                st.plotly_chart(fig_pv, use_container_width=True)
            else:
                st.success("✅ Ningún proveedor tiene contratos en riesgo ALTO.")


# ══════════════════════════════════════════════
# TAB: GARANTÍAS
# ══════════════════════════════════════════════
with tab_gar_tab:
    c1,c2 = st.columns(2)
    with c1:
        gc = df["tiene_garantia"].map({True:"Con garantía ✅",False:"Sin garantía ❌"}).value_counts()
        fig_g = go.Figure(go.Pie(labels=gc.index, values=gc.values, hole=0.5,
            marker_colors=["#059669","#e5e7eb"], textinfo="percent+value"))
        fig_g.update_layout(title="Aplicación de garantías", paper_bgcolor="white",
            font=dict(family="DM Sans",size=10), height=240, margin=dict(t=38,b=10))
        st.plotly_chart(fig_g, use_container_width=True)
    with c2:
        gc2 = df[df["tiene_garantia"]].groupby("comprador_canon").size().reset_index(name="n")
        if not gc2.empty:
            fig_gc2 = px.bar(gc2.sort_values("n"), y="comprador_canon", x="n",
                orientation="h", title="Garantías por comprador",
                color="n", color_continuous_scale="Greens", labels={"comprador_canon":"","n":""})
            fig_gc2.update_layout(paper_bgcolor="white", plot_bgcolor="white",
                font=dict(family="DM Sans",size=10), title=dict(font=dict(size=13,color="#0d1f3c")),
                yaxis=dict(categoryorder="total ascending",tickfont=dict(size=9)),
                coloraxis_showscale=False, height=260, margin=dict(t=38,b=10))
            st.plotly_chart(fig_gc2, use_container_width=True)

    df_grisk = df[df["tiene_garantia"] & df["riesgo"].isin(["ALTO 🔴","MEDIO 🟡"])].copy()
    if not df_grisk.empty:
        st.markdown("**⚠️ Contratos con garantía en riesgo:**")
        cols_gr = [c for c in ["id","proveedor","comprador_canon","estado_ariba","dias_venc","riesgo"] if c in df_grisk]
        st.dataframe(df_grisk[cols_gr].sort_values("dias_venc"), use_container_width=True, height=200)


# ══════════════════════════════════════════════
# TAB: EXPLORADOR
# ══════════════════════════════════════════════
with tab_exp:
    cb,cn = st.columns([3,1])
    with cb:
        busq = st.text_input("🔎 Buscar proveedor, ID o descripción", placeholder="Ej: LOGISTICA, CW2284016...")
    with cn:
        top_n = st.selectbox("Mostrar", [50,100,200,500,"Todos"], index=1)

    cols_def = [c for c in ["id","proveedor","comprador_canon","tipo_comprador","estado_ariba",
                              "fecha_inicio","fecha_termino","dias_venc","riesgo","tiene_garantia",
                              "monto_total","rut"] if c in df.columns]
    cols_sel = st.multiselect("Columnas", df.columns.tolist(), default=cols_def)

    df_exp = df.copy()
    if busq.strip():
        mask = pd.Series(False, index=df_exp.index)
        for col in ["proveedor","descripcion","id","nombre_proyecto"]:
            if col in df_exp.columns:
                mask |= df_exp[col].astype(str).str.contains(busq.strip(), case=False, na=False)
        df_exp = df_exp[mask]

    if "dias_venc" in df_exp.columns:
        df_exp = df_exp.sort_values("dias_venc")
    if top_n != "Todos":
        df_exp = df_exp.head(int(top_n))
    if cols_sel:
        df_exp = df_exp[cols_sel]

    st.dataframe(df_exp, use_container_width=True, height=500)
    st.caption(f"Mostrando {len(df_exp):,} de {len(df):,} contratos · {len(df_piv):,} total en Pivot")

# ──────────────────────────────────────────────────────────────
# EXPORTACIÓN GENERAL
# ──────────────────────────────────────────────────────────────
st.markdown('<div class="sec">📥 Exportar datos del Pivot</div>', unsafe_allow_html=True)
ts = datetime.now().strftime("%Y%m%d_%H%M")
ex1,ex2,ex3,ex4 = st.columns(4)
with ex1:
    st.download_button("💾 Vista actual · CSV", df.to_csv(index=False).encode("utf-8-sig"),
                       f"contratos_{ts}.csv", mime="text/csv")
with ex2:
    crit = df[df["riesgo"].isin(["ALTO 🔴","MEDIO 🟡"])]
    st.download_button("🔴 Solo en riesgo · CSV", crit.to_csv(index=False).encode("utf-8-sig"),
                       f"contratos_riesgo_{ts}.csv", mime="text/csv")
with ex3:
    urg = df[df["dias_venc"].between(0,60)] if "dias_venc" in df.columns else pd.DataFrame()
    st.download_button("⚠️ Vencen en 60 días · CSV", urg.to_csv(index=False).encode("utf-8-sig"),
                       f"contratos_urgentes_{ts}.csv", mime="text/csv")
with ex4:
    gdf = df[df["tiene_garantia"]]
    st.download_button("🔒 Con garantía · CSV", gdf.to_csv(index=False).encode("utf-8-sig"),
                       f"contratos_garantia_{ts}.csv", mime="text/csv")

# ──────────────────────────────────────────────────────────────
# DIAGNÓSTICO
# ──────────────────────────────────────────────────────────────
with st.expander("🔧 Diagnóstico técnico"):
    d1,d2 = st.columns(2)
    with d1:
        st.json({"Total Pivot (activos)":len(df_piv),
                 "Con garantía":int(df_piv["tiene_garantia"].sum()),
                 "Indefinidos":int(df_piv["es_indefinido"].sum()),
                 "Compradores oficiales":int(df_piv["es_oficial"].sum()),
                 "Compradores no registrados":int((~df_piv["es_oficial"]).sum())})
    with d2:
        st.json({"Contratos en vista":len(df),
                 "Riesgo ALTO":int((df["riesgo"]=="ALTO 🔴").sum()),
                 "Riesgo MEDIO":int((df["riesgo"]=="MEDIO 🟡").sum()),
                 "Consolidado cargado":df_cons_raw is not None,
                 "Actualizado":datetime.now().strftime("%d/%m/%Y %H:%M")})

st.markdown(f"""
<div style="text-align:center;color:#9ca3af;font-size:.69rem;margin-top:24px;
     padding-top:12px;border-top:1px solid #f3f4f6;">
  Softys Chile · Compras Estratégicas · {datetime.now().strftime('%d/%m/%Y %H:%M')} · Fuente: SAP Ariba Analysis
</div>
""", unsafe_allow_html=True)
