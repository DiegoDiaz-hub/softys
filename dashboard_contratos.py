"""
dashboard_pivot.py
==================
Dashboard de Gestión de Contratos — Softys Chile
Fuente de datos: Pivot crudo descargado desde SAP Ariba Analysis
Ejecutar: streamlit run dashboard_pivot.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from io import BytesIO
import hashlib
import warnings
warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Contratos · Softys",
    page_icon="📋",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap');

  html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }

  /* Sidebar */
  section[data-testid="stSidebar"] {
    background: #0d1f3c;
    color: #e8eef7;
  }
  section[data-testid="stSidebar"] * { color: #e8eef7 !important; }
  section[data-testid="stSidebar"] .stSelectbox > div > div,
  section[data-testid="stSidebar"] .stMultiSelect > div > div {
    background: #1a3358 !important;
    border-color: #2e5490 !important;
    color: #e8eef7 !important;
  }
  section[data-testid="stSidebar"] hr { border-color: #2e5490; }

  /* KPI Cards */
  .kpi-grid { display: grid; grid-template-columns: repeat(5, 1fr); gap: 14px; margin-bottom: 24px; }
  .kpi-card {
    background: #ffffff;
    border-radius: 12px;
    padding: 18px 16px 14px;
    border-left: 4px solid #1a56db;
    box-shadow: 0 1px 8px rgba(0,0,0,0.07);
    display: flex; flex-direction: column; gap: 4px;
  }
  .kpi-card.green  { border-left-color: #059669; }
  .kpi-card.yellow { border-left-color: #d97706; }
  .kpi-card.red    { border-left-color: #dc2626; }
  .kpi-card.gray   { border-left-color: #6b7280; }
  .kpi-label { font-size: 0.7rem; text-transform: uppercase; letter-spacing: .08em; color: #6b7280; font-weight: 600; }
  .kpi-value { font-size: 2rem; font-weight: 700; color: #0d1f3c; line-height: 1; }
  .kpi-sub   { font-size: 0.72rem; color: #9ca3af; margin-top: 2px; }

  /* Alert rows */
  .alert-row {
    display: flex; align-items: center; gap: 12px;
    padding: 10px 14px; border-radius: 8px;
    background: #fff7ed; border-left: 4px solid #f59e0b;
    margin-bottom: 6px; font-size: 0.85rem;
  }
  .alert-row.red-alert { background: #fef2f2; border-left-color: #ef4444; }
  .alert-tag {
    font-size: 0.7rem; font-weight: 700; padding: 2px 8px;
    border-radius: 99px; background: #fde68a; color: #92400e;
    white-space: nowrap;
  }
  .alert-tag.red-tag { background: #fecaca; color: #991b1b; }

  /* Section headers */
  .section-title {
    font-size: 1rem; font-weight: 700; color: #0d1f3c;
    border-bottom: 2px solid #e5e7eb; padding-bottom: 6px;
    margin: 24px 0 14px;
  }

  /* Hide Streamlit default decoration */
  #MainMenu, footer, header { visibility: hidden; }
  .block-container { padding-top: 1.5rem; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────
# CONSTANTES — MAPEO PIVOT
# ─────────────────────────────────────────────────────────────
# Columnas exactas del Pivot Ariba (hoja Data, fila header 14)
PIVOT_COL_MAP = {
    "ID de contrato":                                    "contrato_ariba",
    "Proyecto - Nombre del proyecto":                    "nombre_proyecto",
    "Fecha de inicio":                                   "fecha_inicio_raw",
    "Nombre del propietario":                            "comprador_raw",
    "Código acreedor SAP":                               "cod_sap",
    "Es Indefinido":                                     "es_indefinido_raw",
    "Región - Región (L2)":                              "region",
    "Rut empresa proveedor":                             "rut",
    "Partes afectadas - Proveedor común":                "proveedor",
    "Contrato - Contrato":                               "contrato_nombre",
    "Fecha de entrada en vigor - Fecha":                 "fecha_vigor_raw",
    "Fecha de finalización - Año":                       "anio_fin",
    "Estado del contrato":                               "estado_contrato",
    "Fecha de expiración - Fecha":                       "fecha_termino_raw",
    "Es un proyecto de prueba":                          "es_prueba",
    "Descripción":                                       "descripcion",
    "Aplica Garantía":                                   "aplica_garantia",
    "Fecha de presentación Garantía N°1 - Fecha":        "fecha_presentacion_garantia",
    "N° de Tipos de Garantías":                          "n_tipos_garantia",
    "Fecha de termino de notificaciones de garantía - Año": "anio_termino_garantia",
    "sum(Importe del contrato)":                         "monto_base",
    "sum(Importe Monto Total Contrato Original)":        "monto_original",
    "sum(Importe Monto total Contrato)":                 "monto_total",
}

# Compradores estratégicos conocidos (lista maestra)
COMPRADORES_ESTRATEGICOS = {
    "Jorge Alfonso Urrutia Carillo": "Jorge Urrutia",
    "Juan Daniel Figueroa": "Juan Figueroa",
    "Joseph Eduardo España Escalona": "Joseph España",
    "Dayana Dávila": "Dayana Dávila",
    "Magdalena Farias": "Magdalena Farias",
    "Denisse Andrea Gonzalez Terrile": "Denisse González",
    "Laura Mendoza": "Laura Mendoza",
    "Bárbara García": "Bárbara García",
    "Claudio Berrios": "Claudio Berrios",
    "Michelle Esperanza": "Michelle Palma",
    "Lina Diaz": "Lina Díaz",
    "Victor Camilla": "Victor Camilla",
    "Leandro Medina": "Leandro Medina",
    "Diego Escalona": "Diego Escalona",
    "Judith Rivas": "Judith Rivas",
    "Sofia Delgado": "Sofia Delgado",
    "Daniela Escobar": "Daniela Escobar",
    "Valeria Silva": "Valeria Silva",
    "Priscilla Gre Guerra": "Priscilla Guerra",
    "MARTINA FUENTES": "Martina Fuentes",
}

COLORES_RIESGO = {
    "BAJO 🟢":    "#059669",
    "MEDIO 🟡":   "#d97706",
    "ALTO 🔴":    "#dc2626",
    "REVISAR ⚪": "#6b7280",
}

MAX_FILE_MB = 50

# ─────────────────────────────────────────────────────────────
# FUNCIONES UTILITARIAS
# ─────────────────────────────────────────────────────────────

def parse_fecha(val) -> pd.Timestamp:
    if pd.isna(val): return pd.NaT
    s = str(val).strip()
    if s in ("", "99.99.9999", "31/12/2999", "2999", "nan"): return pd.NaT
    if isinstance(val, pd.Timestamp):
        return val if val.year < 2900 else pd.NaT
    if isinstance(val, (int, float)):
        try:
            ts = pd.Timestamp("1899-12-30") + pd.Timedelta(days=int(val))
            return ts if ts.year < 2900 else pd.NaT
        except Exception:
            return pd.NaT
    # Intentar múltiples formatos
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%m/%d/%Y", "%d.%m.%Y"):
        try:
            ts = pd.to_datetime(s, format=fmt)
            return ts if ts.year < 2900 else pd.NaT
        except Exception:
            continue
    try:
        ts = pd.to_datetime(s, dayfirst=True, errors="coerce")
        return ts if pd.notna(ts) and ts.year < 2900 else pd.NaT
    except Exception:
        return pd.NaT


def normalizar_nombre(nombre: str) -> str:
    return str(nombre).strip().lower()


def normalizar_comprador(raw: str) -> str:
    """Devuelve nombre canónico del comprador o el nombre limpio si no está en la lista."""
    raw_norm = normalizar_nombre(raw)
    for clave, canonico in COMPRADORES_ESTRATEGICOS.items():
        if normalizar_nombre(clave) == raw_norm:
            return canonico
    # Capitalizar y devolver si no está en lista
    return str(raw).strip().title() if raw and str(raw).strip() else "Sin asignar"


def calcular_riesgo(estado: str, dias: float | None, es_indefinido: bool) -> str:
    if es_indefinido:
        return "BAJO 🟢"
    if dias is None or pd.isna(dias):
        return "REVISAR ⚪"
    d = int(dias)
    estado_l = str(estado).strip()
    if estado_l in ("Vencido", "Cancelado", "Terminado") or d < 0:
        return "ALTO 🔴"
    if estado_l in ("Modificación del borrador", "Próximo a vencer") or d <= 60:
        return "MEDIO 🟡"
    if estado_l in ("Borrador", "En espera"):
        return "REVISAR ⚪"
    if estado_l in ("Publicado", "En revisión", "Aprobado") or d > 60:
        return "BAJO 🟢"
    return "REVISAR ⚪"


def fmt_millones(v: float) -> str:
    if v >= 1_000_000_000:
        return f"${v/1_000_000_000:.1f}B"
    if v >= 1_000_000:
        return f"${v/1_000_000:.1f}M"
    if v >= 1_000:
        return f"${v/1_000:.0f}K"
    return f"${v:.0f}"


# ─────────────────────────────────────────────────────────────
# CARGA Y TRANSFORMACIÓN DEL PIVOT
# ─────────────────────────────────────────────────────────────

def detectar_fila_header(content: bytes) -> int:
    """Detecta la fila 0-indexed donde está 'ID de contrato' en hoja Data."""
    df_scan = pd.read_excel(BytesIO(content), sheet_name="Data", header=None, nrows=25, engine="openpyxl")
    for i, row in df_scan.iterrows():
        if "ID de contrato" in row.values:
            return i
    raise ValueError("No se encontró 'ID de contrato' en las primeras 25 filas de la hoja Data.")


@st.cache_data(show_spinner=False)
def cargar_pivot(file_hash: str, content: bytes) -> pd.DataFrame:
    header_idx = detectar_fila_header(content)
    df_raw = pd.read_excel(BytesIO(content), sheet_name="Data", header=header_idx, engine="openpyxl")
    df_raw = df_raw.dropna(how="all").dropna(axis=1, how="all").reset_index(drop=True)

    # Renombrar columnas conocidas
    df = df_raw.rename(columns={k: v for k, v in PIVOT_COL_MAP.items() if k in df_raw.columns})

    # ── Eliminar contratos cerrados
    if "estado_contrato" in df.columns:
        df = df[~df["estado_contrato"].astype(str).str.lower().str.strip().isin(["cerrado", "cerrados"])]

    # ── Fechas
    for col_raw, col_parsed in [("fecha_inicio_raw", "fecha_inicio"), ("fecha_termino_raw", "fecha_termino"), ("fecha_vigor_raw", "fecha_vigor")]:
        if col_raw in df.columns:
            df[col_parsed] = df[col_raw].apply(parse_fecha)

    # ── Comprador normalizado
    if "comprador_raw" in df.columns:
        df["comprador"] = df["comprador_raw"].apply(normalizar_comprador)
    else:
        df["comprador"] = "Sin asignar"

    # ── Días para vencimiento
    hoy = pd.Timestamp.today().normalize()
    if "fecha_termino" in df.columns:
        df["dias_vencimiento"] = (df["fecha_termino"] - hoy).dt.days
    else:
        df["dias_vencimiento"] = None

    # ── Es indefinido
    def _es_indefinido(row):
        raw = str(row.get("es_indefinido_raw", "")).strip().lower()
        if raw in ("sí", "si", "yes", "1", "true", "indefinido"):
            return True
        ft = row.get("fecha_termino")
        if pd.notna(ft) and isinstance(ft, pd.Timestamp) and ft.year > 2100:
            return True
        return False

    df["es_indefinido"] = df.apply(_es_indefinido, axis=1)

    # ── Riesgo spot
    df["riesgo"] = df.apply(
        lambda r: calcular_riesgo(r.get("estado_contrato", ""), r.get("dias_vencimiento"), r.get("es_indefinido", False)),
        axis=1,
    )

    # ── Garantía flag
    if "aplica_garantia" in df.columns:
        df["tiene_garantia"] = df["aplica_garantia"].astype(str).str.lower().str.strip().isin(["sí", "si", "yes"])
    else:
        df["tiene_garantia"] = False

    # ── Montos limpios
    for col in ("monto_base", "monto_original", "monto_total"):
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # ── Año inicio
    if "fecha_inicio" in df.columns:
        df["anio_inicio"] = df["fecha_inicio"].dt.year

    # ── Mes término (para timeline)
    if "fecha_termino" in df.columns:
        df["mes_termino"] = df["fecha_termino"].dt.to_period("M").astype(str)

    # ── Filtrar filas sin ID de contrato
    df = df[df["contrato_ariba"].notna() & (df["contrato_ariba"].astype(str).str.strip() != "")]

    return df.reset_index(drop=True)


# ─────────────────────────────────────────────────────────────
# COMPONENTES DE VISUALIZACIÓN
# ─────────────────────────────────────────────────────────────

def kpi_cards(df: pd.DataFrame) -> None:
    total      = len(df)
    vigentes   = (df["riesgo"] == "BAJO 🟢").sum()
    medio      = (df["riesgo"] == "MEDIO 🟡").sum()
    alto       = (df["riesgo"] == "ALTO 🔴").sum()
    revisar    = (df["riesgo"] == "REVISAR ⚪").sum()
    indefinidos = df["es_indefinido"].sum() if "es_indefinido" in df.columns else 0
    con_garantia = df["tiene_garantia"].sum() if "tiene_garantia" in df.columns else 0
    monto_tot  = df["monto_total"].sum() if "monto_total" in df.columns else 0
    pct_vig    = f"{vigentes/total*100:.0f}% vigentes" if total else "—"

    st.markdown(f"""
    <div class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-label">Total contratos</div>
        <div class="kpi-value">{total:,}</div>
        <div class="kpi-sub">en la selección actual</div>
      </div>
      <div class="kpi-card green">
        <div class="kpi-label">✅ Vigentes</div>
        <div class="kpi-value">{vigentes:,}</div>
        <div class="kpi-sub">{pct_vig}</div>
      </div>
      <div class="kpi-card yellow">
        <div class="kpi-label">⚠️ Riesgo medio</div>
        <div class="kpi-value">{medio:,}</div>
        <div class="kpi-sub">Próximos a vencer ≤60 d</div>
      </div>
      <div class="kpi-card red">
        <div class="kpi-label">🚨 Riesgo alto</div>
        <div class="kpi-value">{alto:,}</div>
        <div class="kpi-sub">Vencidos o cancelados</div>
      </div>
      <div class="kpi-card gray">
        <div class="kpi-label">🔍 Por revisar</div>
        <div class="kpi-value">{revisar:,}</div>
        <div class="kpi-sub">Borrador / sin fecha</div>
      </div>
    </div>
    <div class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-label">♾️ Indefinidos</div>
        <div class="kpi-value">{indefinidos:,}</div>
        <div class="kpi-sub">sin fecha de término</div>
      </div>
      <div class="kpi-card green">
        <div class="kpi-label">🔒 Con garantía</div>
        <div class="kpi-value">{con_garantia:,}</div>
        <div class="kpi-sub">aplica boleta</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">💰 Monto total</div>
        <div class="kpi-value">{fmt_millones(monto_tot)}</div>
        <div class="kpi-sub">contratos filtrados</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">👤 Compradores</div>
        <div class="kpi-value">{df["comprador"].nunique():,}</div>
        <div class="kpi-sub">propietarios únicos</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">🏢 Proveedores</div>
        <div class="kpi-value">{df["proveedor"].nunique() if "proveedor" in df.columns else 0:,}</div>
        <div class="kpi-sub">proveedores únicos</div>
      </div>
    </div>
    """, unsafe_allow_html=True)


def grafico_donut_riesgo(df: pd.DataFrame) -> go.Figure:
    datos = df["riesgo"].value_counts().reset_index()
    datos.columns = ["Riesgo", "Cantidad"]
    colores = [COLORES_RIESGO.get(r, "#999") for r in datos["Riesgo"]]
    fig = go.Figure(go.Pie(
        labels=datos["Riesgo"], values=datos["Cantidad"],
        hole=0.55, marker_colors=colores,
        textinfo="percent+value",
        textfont=dict(size=11, family="DM Sans"),
        hovertemplate="<b>%{label}</b><br>%{value} contratos (%{percent})<extra></extra>",
    ))
    fig.update_layout(
        title=dict(text="Distribución de Riesgo", font=dict(size=13, family="DM Sans", color="#0d1f3c"), x=0.02),
        showlegend=True,
        legend=dict(font=dict(family="DM Sans", size=10), orientation="h", y=-0.12),
        paper_bgcolor="white", plot_bgcolor="white",
        margin=dict(t=40, b=30, l=10, r=10),
        height=280,
    )
    return fig


def grafico_timeline_vencimientos(df: pd.DataFrame) -> go.Figure | None:
    if "fecha_termino" not in df.columns:
        return None
    hoy = pd.Timestamp.today().normalize()
    df_f = df[df["fecha_termino"].notna() & (df["dias_vencimiento"].between(-30, 180))].copy()
    if df_f.empty:
        return None
    df_f["mes"] = df_f["fecha_termino"].dt.to_period("M").astype(str)
    agrup = df_f.groupby(["mes", "riesgo"]).size().reset_index(name="n")
    fig = px.bar(
        agrup, x="mes", y="n", color="riesgo",
        color_discrete_map=COLORES_RIESGO,
        title="Vencimientos próximos 6 meses",
        labels={"mes": "Mes", "n": "Contratos", "riesgo": "Riesgo"},
        barmode="stack",
    )
    fig.update_layout(
        paper_bgcolor="white", plot_bgcolor="white",
        font=dict(family="DM Sans", size=10, color="#374151"),
        title=dict(font=dict(size=13, color="#0d1f3c")),
        xaxis=dict(tickangle=-30, gridcolor="#f3f4f6"),
        yaxis=dict(gridcolor="#f3f4f6"),
        legend=dict(orientation="h", y=-0.25, font=dict(size=10)),
        margin=dict(t=40, b=60, l=10, r=10),
        height=280,
    )
    return fig


def grafico_compradores(df: pd.DataFrame) -> go.Figure:
    dc = df.groupby(["comprador", "riesgo"]).size().reset_index(name="n")
    # Ordenar por total
    orden = df["comprador"].value_counts().index.tolist()
    dc["comprador"] = pd.Categorical(dc["comprador"], categories=orden[::-1], ordered=True)
    dc = dc.sort_values("comprador")
    fig = px.bar(
        dc, y="comprador", x="n", color="riesgo",
        color_discrete_map=COLORES_RIESGO, barmode="stack", orientation="h",
        title="Contratos por comprador (propietario Ariba)",
        labels={"comprador": "", "n": "Contratos", "riesgo": "Riesgo"},
    )
    fig.update_layout(
        paper_bgcolor="white", plot_bgcolor="white",
        font=dict(family="DM Sans", size=10, color="#374151"),
        title=dict(font=dict(size=13, color="#0d1f3c")),
        xaxis=dict(gridcolor="#f3f4f6"),
        yaxis=dict(tickfont=dict(size=9)),
        legend=dict(orientation="h", y=-0.1, font=dict(size=10)),
        margin=dict(t=40, b=60, l=10, r=10),
        height=max(280, len(df["comprador"].unique()) * 22),
    )
    return fig


def grafico_estados(df: pd.DataFrame) -> go.Figure:
    datos = df["estado_contrato"].value_counts().reset_index()
    datos.columns = ["Estado", "Cantidad"]
    fig = px.bar(
        datos, x="Cantidad", y="Estado", orientation="h",
        title="Contratos por estado Ariba",
        color="Cantidad", color_continuous_scale="Blues",
        labels={"Cantidad": "Contratos", "Estado": ""},
    )
    fig.update_layout(
        paper_bgcolor="white", plot_bgcolor="white",
        font=dict(family="DM Sans", size=10, color="#374151"),
        title=dict(font=dict(size=13, color="#0d1f3c")),
        xaxis=dict(gridcolor="#f3f4f6"),
        yaxis=dict(categoryorder="total ascending", tickfont=dict(size=9)),
        coloraxis_showscale=False,
        margin=dict(t=40, b=20, l=10, r=10),
        height=280,
    )
    return fig


def grafico_evolucion_anual(df: pd.DataFrame) -> go.Figure | None:
    if "anio_inicio" not in df.columns:
        return None
    anios = df["anio_inicio"].dropna().astype(int)
    anios = anios[(anios >= 2015) & (anios <= datetime.now().year + 1)]
    if anios.empty:
        return None
    conteo = anios.value_counts().sort_index().reset_index()
    conteo.columns = ["Año", "Contratos"]
    fig = px.area(
        conteo, x="Año", y="Contratos",
        title="Contratos iniciados por año",
        color_discrete_sequence=["#1a56db"],
    )
    fig.update_traces(fill="tozeroy", line_color="#1a56db", fillcolor="rgba(26,86,219,0.12)")
    fig.update_layout(
        paper_bgcolor="white", plot_bgcolor="white",
        font=dict(family="DM Sans", size=10, color="#374151"),
        title=dict(font=dict(size=13, color="#0d1f3c")),
        xaxis=dict(gridcolor="#f3f4f6", dtick=1),
        yaxis=dict(gridcolor="#f3f4f6"),
        margin=dict(t=40, b=20, l=10, r=10),
        height=250,
    )
    return fig


def grafico_top_proveedores(df: pd.DataFrame, n: int = 15) -> go.Figure:
    if "proveedor" not in df.columns:
        return go.Figure()
    top = df["proveedor"].value_counts().head(n).reset_index()
    top.columns = ["Proveedor", "Contratos"]
    # Nombre corto para eje
    top["Proveedor"] = top["Proveedor"].str[:45]
    fig = px.bar(
        top, y="Proveedor", x="Contratos", orientation="h",
        title=f"Top {n} proveedores por nº de contratos",
        color="Contratos", color_continuous_scale="Teal",
    )
    fig.update_layout(
        paper_bgcolor="white", plot_bgcolor="white",
        font=dict(family="DM Sans", size=10, color="#374151"),
        title=dict(font=dict(size=13, color="#0d1f3c")),
        xaxis=dict(gridcolor="#f3f4f6"),
        yaxis=dict(categoryorder="total ascending", tickfont=dict(size=9)),
        coloraxis_showscale=False,
        margin=dict(t=40, b=20, l=10, r=10),
        height=max(280, n * 22),
    )
    return fig


def grafico_monto_comprador(df: pd.DataFrame) -> go.Figure | None:
    if "monto_total" not in df.columns:
        return None
    dm = df.groupby("comprador")["monto_total"].sum().reset_index()
    dm = dm[dm["monto_total"] > 0].sort_values("monto_total", ascending=True).tail(15)
    if dm.empty:
        return None
    fig = px.bar(
        dm, y="comprador", x="monto_total", orientation="h",
        title="Monto total de contratos por comprador (CLP)",
        color="monto_total", color_continuous_scale="Purp",
        labels={"comprador": "", "monto_total": "Monto CLP"},
    )
    fig.update_traces(hovertemplate="<b>%{y}</b><br>$%{x:,.0f}<extra></extra>")
    fig.update_layout(
        paper_bgcolor="white", plot_bgcolor="white",
        font=dict(family="DM Sans", size=10, color="#374151"),
        title=dict(font=dict(size=13, color="#0d1f3c")),
        xaxis=dict(gridcolor="#f3f4f6"),
        yaxis=dict(tickfont=dict(size=9)),
        coloraxis_showscale=False,
        margin=dict(t=40, b=20, l=10, r=10),
        height=300,
    )
    return fig


def tabla_alertas(df: pd.DataFrame) -> pd.DataFrame:
    criticos = df[df["riesgo"].isin(["ALTO 🔴", "MEDIO 🟡"])].copy()
    cols = [c for c in ["contrato_ariba", "proveedor", "comprador", "estado_contrato",
                         "dias_vencimiento", "riesgo", "tiene_garantia", "monto_total"] if c in criticos.columns]
    alertas = criticos[cols].copy()
    renombres = {
        "contrato_ariba": "Contrato",
        "proveedor": "Proveedor",
        "comprador": "Comprador",
        "estado_contrato": "Estado",
        "dias_vencimiento": "Días restantes",
        "riesgo": "Riesgo",
        "tiene_garantia": "Garantía",
        "monto_total": "Monto CLP",
    }
    alertas = alertas.rename(columns={k: v for k, v in renombres.items() if k in alertas.columns})
    if "Días restantes" in alertas.columns:
        alertas = alertas.sort_values("Días restantes")
    return alertas


# ─────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("""
    <div style="padding: 8px 0 20px; border-bottom: 1px solid #2e5490;">
      <div style="font-size:1.2rem; font-weight:700; letter-spacing:.02em;">📋 Contratos</div>
      <div style="font-size:0.72rem; opacity:.7; margin-top:2px;">Softys Chile · Compras Estratégicas</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)
    st.markdown("**📁 Cargar archivo Pivot**")
    uploaded = st.file_uploader(
        "Sube el Pivot (.xlsx) descargado de Ariba",
        type=["xlsx", "xls"],
        label_visibility="collapsed",
    )
    st.caption("Archivo directo de SAP Ariba Analysis (hoja Data)")

    st.markdown("---")

    if uploaded:
        st.markdown("**🎛️ Filtros**")
        # Placeholder: se rellenarán después de cargar los datos
        filtro_placeholder = st.empty()


if not uploaded:
    # ── PANTALLA DE BIENVENIDA
    st.markdown("""
    <div style="display:flex; flex-direction:column; align-items:center; justify-content:center;
         padding: 80px 40px; text-align:center;">
      <div style="font-size:3.5rem; margin-bottom:16px;">📋</div>
      <h1 style="font-size:1.8rem; font-weight:700; color:#0d1f3c; margin-bottom:8px;">
        Dashboard de Gestión de Contratos
      </h1>
      <p style="color:#6b7280; font-size:1rem; max-width:480px; line-height:1.6;">
        Sube el archivo <strong>Pivot</strong> descargado directamente desde
        <strong>SAP Ariba Analysis</strong> para visualizar todos tus indicadores al instante.
      </p>
      <div style="margin-top:32px; display:grid; grid-template-columns:1fr 1fr 1fr; gap:20px; max-width:600px;">
        <div style="background:#f0fdf4; border-radius:10px; padding:16px; border:1px solid #d1fae5;">
          <div style="font-size:1.5rem">📊</div>
          <div style="font-weight:600; font-size:.85rem; margin-top:6px;">10 KPIs automáticos</div>
          <div style="font-size:.75rem; color:#6b7280; margin-top:4px;">riesgo, montos, garantías</div>
        </div>
        <div style="background:#eff6ff; border-radius:10px; padding:16px; border:1px solid #dbeafe;">
          <div style="font-size:1.5rem">🔍</div>
          <div style="font-weight:600; font-size:.85rem; margin-top:6px;">Filtros dinámicos</div>
          <div style="font-size:.75rem; color:#6b7280; margin-top:4px;">comprador, estado, riesgo</div>
        </div>
        <div style="background:#fefce8; border-radius:10px; padding:16px; border:1px solid #fde68a;">
          <div style="font-size:1.5rem">⚠️</div>
          <div style="font-weight:600; font-size:.85rem; margin-top:6px;">Alertas de vencimiento</div>
          <div style="font-size:.75rem; color:#6b7280; margin-top:4px;">acción inmediata</div>
        </div>
      </div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

# ─────────────────────────────────────────────────────────────
# CARGA DE DATOS
# ─────────────────────────────────────────────────────────────

if uploaded.size > MAX_FILE_MB * 1024 * 1024:
    st.error(f"❌ Archivo demasiado grande (máx. {MAX_FILE_MB} MB).")
    st.stop()

uploaded.seek(0)
content = uploaded.read()
file_hash = hashlib.md5(content).hexdigest()

with st.spinner("🔄 Procesando el Pivot de Ariba..."):
    try:
        df_full = cargar_pivot(file_hash, content)
    except Exception as exc:
        st.error(f"❌ No se pudo procesar el archivo: {exc}")
        st.stop()

if df_full.empty:
    st.error("❌ No se encontraron contratos válidos. Verifica que sea un Pivot válido de Ariba.")
    st.stop()

# ─────────────────────────────────────────────────────────────
# FILTROS EN SIDEBAR (ahora que tenemos datos)
# ─────────────────────────────────────────────────────────────

with st.sidebar:
    with filtro_placeholder.container():
        # Riesgo
        opciones_riesgo = ["Todos"] + sorted(df_full["riesgo"].dropna().unique().tolist())
        sel_riesgo = st.selectbox("🚦 Riesgo spot", opciones_riesgo)

        # Comprador
        compradores_lista = ["Todos"] + sorted(df_full["comprador"].dropna().unique().tolist())
        sel_comprador = st.selectbox("👤 Comprador", compradores_lista)

        # Estado contrato
        estados_lista = ["Todos"] + sorted(df_full["estado_contrato"].dropna().unique().tolist())
        sel_estado = st.selectbox("📄 Estado contrato", estados_lista)

        # Garantía
        sel_garantia = st.selectbox("🔒 Garantía", ["Todas", "Con garantía", "Sin garantía"])

        # Año inicio
        if "anio_inicio" in df_full.columns:
            anios_disp = sorted(df_full["anio_inicio"].dropna().astype(int).unique().tolist())
            if anios_disp:
                sel_anio = st.selectbox("📅 Año inicio", ["Todos"] + [str(a) for a in anios_disp])
            else:
                sel_anio = "Todos"
        else:
            sel_anio = "Todos"

        # Es indefinido
        sel_indefinido = st.selectbox("♾️ Contratos indefinidos", ["Todos", "Solo indefinidos", "Solo con fecha"])

        st.markdown("---")
        st.caption(f"📁 {uploaded.name}\n\n{len(df_full):,} contratos cargados")

# ─────────────────────────────────────────────────────────────
# APLICAR FILTROS
# ─────────────────────────────────────────────────────────────

df = df_full.copy()
if sel_riesgo != "Todos":
    df = df[df["riesgo"] == sel_riesgo]
if sel_comprador != "Todos":
    df = df[df["comprador"] == sel_comprador]
if sel_estado != "Todos":
    df = df[df["estado_contrato"] == sel_estado]
if sel_garantia == "Con garantía":
    df = df[df["tiene_garantia"]]
elif sel_garantia == "Sin garantía":
    df = df[~df["tiene_garantia"]]
if sel_anio != "Todos" and "anio_inicio" in df.columns:
    df = df[df["anio_inicio"] == int(sel_anio)]
if sel_indefinido == "Solo indefinidos":
    df = df[df["es_indefinido"]]
elif sel_indefinido == "Solo con fecha":
    df = df[~df["es_indefinido"]]

if df.empty:
    st.warning("⚠️ No hay contratos que coincidan con los filtros seleccionados.")
    st.stop()

# ─────────────────────────────────────────────────────────────
# ENCABEZADO
# ─────────────────────────────────────────────────────────────

st.markdown(f"""
<div style="display:flex; justify-content:space-between; align-items:flex-end; margin-bottom:8px;">
  <div>
    <h1 style="font-size:1.5rem; font-weight:700; color:#0d1f3c; margin:0; line-height:1.2;">
      Dashboard de Gestión de Contratos
    </h1>
    <div style="color:#6b7280; font-size:0.8rem; margin-top:3px;">
      Softys Chile · Fuente: Pivot Ariba · {df['contrato_ariba'].nunique():,} contratos activos
    </div>
  </div>
  <div style="font-size:0.75rem; color:#9ca3af;">
    Actualizado: {datetime.now().strftime('%d/%m/%Y %H:%M')}
  </div>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────
# KPIs
# ─────────────────────────────────────────────────────────────

kpi_cards(df)

# ─────────────────────────────────────────────────────────────
# FILA 1 DE GRÁFICOS: Riesgo + Timeline + Estados
# ─────────────────────────────────────────────────────────────

col1, col2, col3 = st.columns([1, 1.4, 1.2])
with col1:
    st.plotly_chart(grafico_donut_riesgo(df), use_container_width=True)
with col2:
    fig_tl = grafico_timeline_vencimientos(df)
    if fig_tl:
        st.plotly_chart(fig_tl, use_container_width=True)
    else:
        st.info("Sin datos de vencimiento en el rango seleccionado.")
with col3:
    st.plotly_chart(grafico_estados(df), use_container_width=True)

# ─────────────────────────────────────────────────────────────
# ALERTAS DE ACCIÓN INMEDIATA
# ─────────────────────────────────────────────────────────────

st.markdown('<div class="section-title">🚨 Alertas de Acción Inmediata</div>', unsafe_allow_html=True)

df_alertas = tabla_alertas(df)
if not df_alertas.empty:
    def highlight_risk(val):
        if "ALTO" in str(val):
            return "background-color:#fef2f2; color:#991b1b; font-weight:600;"
        if "MEDIO" in str(val):
            return "background-color:#fffbeb; color:#92400e; font-weight:600;"
        return ""

    styled = df_alertas.style.map(highlight_risk, subset=["Riesgo"] if "Riesgo" in df_alertas.columns else [])
    if "Monto CLP" in df_alertas.columns:
        styled = styled.format({"Monto CLP": "{:,.0f}", "Días restantes": "{:.0f}"}, na_rep="—")
    st.dataframe(styled, use_container_width=True, height=260)
    col_a, col_b = st.columns(2)
    col_a.info(f"💡 **{(df_alertas['Riesgo'] == 'ALTO 🔴').sum() if 'Riesgo' in df_alertas.columns else 0}** contratos en riesgo ALTO requieren regularización urgente.")
    col_b.warning(f"⚠️ **{(df_alertas['Riesgo'] == 'MEDIO 🟡').sum() if 'Riesgo' in df_alertas.columns else 0}** contratos próximos a vencer deben renovarse.")
else:
    st.success("✅ No hay contratos en riesgo alto o medio con los filtros actuales.")

# ─────────────────────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📌 Por Comprador",
    "🏢 Proveedores",
    "💰 Montos",
    "🔒 Garantías",
    "🔍 Explorador",
])

# ── TAB 1: Por Comprador
with tab1:
    st.plotly_chart(grafico_compradores(df), use_container_width=True)
    # Tabla resumen por comprador
    resumen = df.groupby("comprador").agg(
        Contratos=("contrato_ariba", "count"),
        Riesgo_Alto=("riesgo", lambda x: (x == "ALTO 🔴").sum()),
        Riesgo_Medio=("riesgo", lambda x: (x == "MEDIO 🟡").sum()),
        Vigentes=("riesgo", lambda x: (x == "BAJO 🟢").sum()),
        Indefinidos=("es_indefinido", "sum"),
        Con_Garantia=("tiene_garantia", "sum"),
    ).reset_index().sort_values("Contratos", ascending=False)
    st.dataframe(resumen, use_container_width=True, height=300)

# ── TAB 2: Proveedores
with tab2:
    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(grafico_top_proveedores(df, 15), use_container_width=True)
    with c2:
        # Proveedores con más riesgo
        if "proveedor" in df.columns:
            prov_riesgo = df[df["riesgo"] == "ALTO 🔴"].groupby("proveedor").size().reset_index(name="Contratos vencidos")
            prov_riesgo = prov_riesgo.sort_values("Contratos vencidos", ascending=False).head(15)
            if not prov_riesgo.empty:
                fig_pr = px.bar(prov_riesgo, y="proveedor", x="Contratos vencidos", orientation="h",
                                title="Proveedores con más contratos vencidos",
                                color="Contratos vencidos", color_continuous_scale="Reds")
                fig_pr.update_layout(
                    paper_bgcolor="white", plot_bgcolor="white",
                    font=dict(family="DM Sans", size=10),
                    coloraxis_showscale=False,
                    yaxis=dict(categoryorder="total ascending", tickfont=dict(size=9)),
                    height=380,
                    margin=dict(t=40, b=20),
                )
                st.plotly_chart(fig_pr, use_container_width=True)
            else:
                st.success("✅ Ningún proveedor tiene contratos en riesgo ALTO con los filtros actuales.")

# ── TAB 3: Montos
with tab3:
    fig_evol = grafico_evolucion_anual(df)
    if fig_evol:
        st.plotly_chart(fig_evol, use_container_width=True)
    c1, c2 = st.columns(2)
    with c1:
        fig_mc = grafico_monto_comprador(df)
        if fig_mc:
            st.plotly_chart(fig_mc, use_container_width=True)
    with c2:
        # Monto por estado
        if "monto_total" in df.columns:
            dm_est = df.groupby("estado_contrato")["monto_total"].sum().reset_index()
            dm_est = dm_est[dm_est["monto_total"] > 0].sort_values("monto_total")
            if not dm_est.empty:
                fig_mest = px.bar(dm_est, y="estado_contrato", x="monto_total", orientation="h",
                                  title="Monto total por estado del contrato",
                                  color="monto_total", color_continuous_scale="Blues",
                                  labels={"estado_contrato": "", "monto_total": "Monto CLP"})
                fig_mest.update_traces(hovertemplate="<b>%{y}</b><br>$%{x:,.0f}<extra></extra>")
                fig_mest.update_layout(
                    paper_bgcolor="white", plot_bgcolor="white",
                    font=dict(family="DM Sans", size=10),
                    coloraxis_showscale=False,
                    yaxis=dict(tickfont=dict(size=9)),
                    height=280,
                    margin=dict(t=40, b=20),
                )
                st.plotly_chart(fig_mest, use_container_width=True)

    # Estadísticas de monto
    if "monto_total" in df.columns:
        montos = df[df["monto_total"] > 0]["monto_total"]
        if not montos.empty:
            mc1, mc2, mc3, mc4 = st.columns(4)
            mc1.metric("Monto total", fmt_millones(montos.sum()))
            mc2.metric("Monto promedio", fmt_millones(montos.mean()))
            mc3.metric("Monto máximo", fmt_millones(montos.max()))
            mc4.metric("Contratos con monto", f"{len(montos):,}")

# ── TAB 4: Garantías
with tab4:
    col_g1, col_g2 = st.columns(2)
    with col_g1:
        gar_counts = df["tiene_garantia"].map({True: "Con garantía ✅", False: "Sin garantía ❌"}).value_counts()
        fig_gar = go.Figure(go.Pie(
            labels=gar_counts.index, values=gar_counts.values,
            hole=0.5, marker_colors=["#059669", "#e5e7eb"],
            textinfo="percent+value",
        ))
        fig_gar.update_layout(
            title="Aplicación de garantías",
            paper_bgcolor="white",
            font=dict(family="DM Sans", size=10),
            height=260,
            margin=dict(t=40, b=20),
        )
        st.plotly_chart(fig_gar, use_container_width=True)

    with col_g2:
        # Garantías por comprador
        gar_comp = df[df["tiene_garantia"]].groupby("comprador").size().reset_index(name="Con garantía")
        gar_comp = gar_comp.sort_values("Con garantía", ascending=False)
        if not gar_comp.empty:
            fig_gc = px.bar(gar_comp, y="comprador", x="Con garantía", orientation="h",
                            title="Contratos con garantía por comprador",
                            color="Con garantía", color_continuous_scale="Greens",
                            labels={"comprador": ""})
            fig_gc.update_layout(
                paper_bgcolor="white", plot_bgcolor="white",
                font=dict(family="DM Sans", size=10),
                coloraxis_showscale=False,
                yaxis=dict(categoryorder="total ascending", tickfont=dict(size=9)),
                height=280,
                margin=dict(t=40, b=20),
            )
            st.plotly_chart(fig_gc, use_container_width=True)

    # Alertas garantías: requieren pero no tienen fecha próxima
    df_gar_vence = df[df["tiene_garantia"] & df["riesgo"].isin(["ALTO 🔴", "MEDIO 🟡"])].copy()
    if not df_gar_vence.empty:
        st.markdown("**⚠️ Contratos con garantía en riesgo:**")
        cols_gar = [c for c in ["contrato_ariba", "proveedor", "comprador", "estado_contrato", "dias_vencimiento", "riesgo"] if c in df_gar_vence.columns]
        st.dataframe(df_gar_vence[cols_gar].sort_values("dias_vencimiento"), use_container_width=True, height=220)

# ── TAB 5: Explorador
with tab5:
    st.markdown("### 🔍 Explorador de contratos")
    col_busq, col_top = st.columns([3, 1])
    with col_busq:
        busqueda = st.text_input("Buscar por proveedor, descripción o ID de contrato", placeholder="Ej: LOGISTICA, CW2284016...")
    with col_top:
        top_n = st.selectbox("Mostrar", [50, 100, 200, 500, "Todos"], index=1)

    cols_disponibles = df.columns.tolist()
    cols_default = [c for c in ["contrato_ariba", "proveedor", "comprador", "estado_contrato",
                                  "fecha_inicio", "fecha_termino", "dias_vencimiento",
                                  "riesgo", "tiene_garantia", "monto_total", "rut"] if c in cols_disponibles]
    cols_sel = st.multiselect("Columnas a mostrar", options=cols_disponibles, default=cols_default)

    df_exp = df.copy()
    if busqueda.strip():
        mask = pd.Series(False, index=df_exp.index)
        for col in ["proveedor", "descripcion", "contrato_ariba", "nombre_proyecto"]:
            if col in df_exp.columns:
                mask |= df_exp[col].astype(str).str.contains(busqueda.strip(), case=False, na=False)
        df_exp = df_exp[mask]

    if "dias_vencimiento" in df_exp.columns:
        df_exp = df_exp.sort_values("dias_vencimiento")

    if top_n != "Todos":
        df_exp = df_exp.head(int(top_n))

    if cols_sel:
        df_exp = df_exp[cols_sel]

    st.dataframe(df_exp, use_container_width=True, height=500)
    st.caption(f"Mostrando {len(df_exp):,} contratos · {len(df):,} en filtro activo · {len(df_full):,} total cargado")

# ─────────────────────────────────────────────────────────────
# EXPORTACIÓN
# ─────────────────────────────────────────────────────────────

st.markdown('<div class="section-title">📥 Exportar</div>', unsafe_allow_html=True)
ex1, ex2, ex3, ex4 = st.columns(4)

ts = datetime.now().strftime("%Y%m%d_%H%M")

with ex1:
    csv = df.to_csv(index=False).encode("utf-8-sig")
    st.download_button("💾 Todos (filtrado) · CSV", csv,
                       file_name=f"contratos_{ts}.csv", mime="text/csv")

with ex2:
    criticos = df[df["riesgo"].isin(["ALTO 🔴", "MEDIO 🟡"])]
    csv_c = criticos.to_csv(index=False).encode("utf-8-sig")
    st.download_button("🔴 Solo en riesgo · CSV", csv_c,
                       file_name=f"contratos_riesgo_{ts}.csv", mime="text/csv")

with ex3:
    urg = df[df["dias_vencimiento"].between(0, 60)] if "dias_vencimiento" in df.columns else pd.DataFrame()
    csv_u = urg.to_csv(index=False).encode("utf-8-sig")
    st.download_button("⚠️ Vencen en 60 días · CSV", csv_u,
                       file_name=f"contratos_urgentes_{ts}.csv", mime="text/csv")

with ex4:
    gar = df[df["tiene_garantia"]]
    csv_g = gar.to_csv(index=False).encode("utf-8-sig")
    st.download_button("🔒 Con garantía · CSV", csv_g,
                       file_name=f"contratos_garantia_{ts}.csv", mime="text/csv")

# ─────────────────────────────────────────────────────────────
# DIAGNÓSTICO TÉCNICO
# ─────────────────────────────────────────────────────────────

with st.expander("🔧 Diagnóstico técnico"):
    d1, d2 = st.columns(2)
    with d1:
        st.markdown("**Datos cargados:**")
        st.json({
            "Total filas en Pivot": len(df_full),
            "Contratos activos (sin Cerrados)": len(df_full),
            "Con fecha inicio": int(df_full["fecha_inicio"].notna().sum()) if "fecha_inicio" in df_full.columns else "N/A",
            "Con fecha término": int(df_full["fecha_termino"].notna().sum()) if "fecha_termino" in df_full.columns else "N/A",
            "Con garantía": int(df_full["tiene_garantia"].sum()),
            "Indefinidos": int(df_full["es_indefinido"].sum()),
        })
    with d2:
        st.markdown("**Selección actual:**")
        st.json({
            "Contratos filtrados": len(df),
            "Compradores únicos": int(df["comprador"].nunique()),
            "Proveedores únicos": int(df["proveedor"].nunique()) if "proveedor" in df.columns else 0,
            "Riesgo ALTO": int((df["riesgo"] == "ALTO 🔴").sum()),
            "Riesgo MEDIO": int((df["riesgo"] == "MEDIO 🟡").sum()),
            "Procesado": datetime.now().strftime("%d/%m/%Y %H:%M"),
        })

st.markdown(f"""
<div style="text-align:center; color:#9ca3af; font-size:0.72rem; margin-top:32px; padding-top:16px; border-top:1px solid #f3f4f6;">
  Softys Chile · Compras Estratégicas · Generado {datetime.now().strftime('%d/%m/%Y %H:%M')} · Fuente: SAP Ariba Analysis
</div>
""", unsafe_allow_html=True)
