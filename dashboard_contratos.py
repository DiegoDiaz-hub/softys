import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import re
import hashlib
from io import BytesIO
from typing import Dict, List, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

# ==============================
# 🧠 CONFIGURACIÓN INICIAL
# ==============================
st.set_page_config(
    page_title="Gestión de Contratos — Softys",
    page_icon="📋",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==============================
# 🎨 ESTILOS PERSONALIZADOS
# ==============================
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #1e3a5f, #2d5986);
        border-radius: 12px;
        padding: 16px 20px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 12px rgba(0,0,0,0.2);
    }
    .metric-value { font-size: 2rem; font-weight: 700; margin: 4px 0; }
    .metric-label { font-size: 0.8rem; opacity: 0.85; text-transform: uppercase; letter-spacing: 1px; }
    .alert-box {
        border-left: 5px solid #e74c3c;
        background: #fdf2f2;
        padding: 12px 16px;
        border-radius: 0 8px 8px 0;
        margin-bottom: 8px;
        font-size: 0.9rem;
    }
    .warn-box {
        border-left: 5px solid #f39c12;
        background: #fef9ed;
        padding: 12px 16px;
        border-radius: 0 8px 8px 0;
        margin-bottom: 8px;
        font-size: 0.9rem;
    }
    .success-box {
        border-left: 5px solid #27ae60;
        background: #f0fdf4;
        padding: 12px 16px;
        border-radius: 0 8px 8px 0;
        margin-bottom: 8px;
        font-size: 0.9rem;
    }
    div[data-testid="stMetricValue"] { font-size: 1.8rem; }
</style>
""", unsafe_allow_html=True)

# ==============================
# 🛡️ CONSTANTES
# ==============================
MAX_FILE_SIZE_BYTES = 50 * 1024 * 1024
DIAS_ALERTA_VENCIMIENTO = 30

# ==============================
# ✅ LISTA MAESTRA DE COMPRADORES (ESTRICTA)
# ==============================
STRATEGIC_BUYERS = {
    'Patricio Espinoza', 'Jorge Urrutia', 'Bárbara García', 'Claudio Berrios',
    'Martina Fuentes', 'Joseph España', 'Michelle Palma', 'Juan Figueroa',
    'Magdalena Farias', 'Denisse Andrea Gonzalez Terrile'
}
TACTICAL_BUYERS = {
    'Leonardo Nacarate', 'Martina Fuentes', 'Scarlette Lucero',
    'Margarita Lineros', 'Erika Silva', 'Karina Satelo', 'Pablo Labs'
}
TYPO_CORRECTIONS = {
    'jorge uturria': 'Jorge Urrutia', 'jorgue urrutia': 'Jorge Urrutia',
    'dennis andrea gonzales': 'Denisse Andrea Gonzalez Terrile',
    'denisse andrea gonzalez terrile': 'Denisse Andrea Gonzalez Terrile',
    'juan daniel figueroa': 'Juan Figueroa',
    'joseph eduardo españa escalona': 'Joseph España',
    'michelle esperanza': 'Michelle Palma',
    'leonardo nacarete': 'Leonardo Nacarate'
}

def normalize_name(name: str) -> str:
    if pd.isna(name) or str(name).strip() == '': return ''
    clean = str(name).strip().lower()
    clean = ''.join(c for c in clean if c not in 'áéíóúüñ')
    return clean

def classify_buyer_strict(raw_name: str) -> tuple:
    clean_raw = normalize_name(raw_name)
    if not clean_raw: return None, None
    for typo, correct in TYPO_CORRECTIONS.items():
        if typo in clean_raw or clean_raw in typo:
            clean_raw = normalize_name(correct); break
    for official in STRATEGIC_BUYERS:
        if clean_raw == normalize_name(official) or clean_raw in normalize_name(official) or normalize_name(official) in clean_raw:
            return 'strategic', official
    for official in TACTICAL_BUYERS:
        if clean_raw == normalize_name(official) or clean_raw in normalize_name(official) or normalize_name(official) in clean_raw:
            return 'tactical', official
    return None, None

# ==============================
# 🔧 FUNCIONES BACKEND - NUEVO PARSER PARA PIVOT CRUDO
# ==============================

def parse_fecha(valor) -> pd.Timestamp:
    if pd.isna(valor) or str(valor).strip() in ['99.99.9999', '2999', '31/12/2999', 'Indefinido', '']:
        return pd.NaT
    if isinstance(valor, pd.Timestamp):
        return valor if valor.year < 2900 else pd.NaT
    if isinstance(valor, (int, float)):
        try:
            ts = pd.Timestamp('1899-12-30') + pd.Timedelta(days=int(valor))
            return ts if ts.year < 2900 else pd.NaT
        except:
            return pd.NaT
    valor_str = str(valor).strip()
    valor_limpio = valor_str.replace('"-"', '-').replace('/', '-').replace('.', '-')
    formatos = ['%d-%m-%Y', '%d/%m/%Y', '%m/%d/%Y', '%Y-%m-%d', '%d-%m-%y', '%m/%d/%y']
    for fmt in formatos:
        try:
            return pd.to_datetime(valor_limpio, format=fmt, dayfirst=True)
        except:
            continue
    try:
        ts = pd.to_datetime(valor_limpio, errors='coerce', dayfirst=True)
        return ts if pd.notna(ts) and ts.year < 2900 else pd.NaT
    except:
        return pd.NaT

def limpiar_monto(valor) -> float:
    if pd.isna(valor): return 0.0
    try:
        limpio = str(valor).replace('.', '').replace(',', '.').replace('$', '').replace('UF', '').strip()
        return float(limpio) if limpio else 0.0
    except:
        return 0.0

def load_pivot_ariba(file_content: bytes) -> pd.DataFrame:
    """
    Carga el Pivot de Ariba detectando automáticamente la fila con headers concatenados.
    """
    # Leer primeras 100 filas para detectar estructura
    df_raw = pd.read_excel(BytesIO(file_content), header=None, nrows=100, engine='openpyxl')
    
    # Buscar fila que contenga 'ContractId' (indicador de headers reales)
    header_row_idx = None
    header_col_idx = None
    
    for i in range(len(df_raw)):
        for j in range(min(10, df_raw.shape[1])):  # Buscar en primeras 10 columnas
            cell_val = str(df_raw.iloc[i, j]).lower() if pd.notna(df_raw.iloc[i, j]) else ''
            if 'contractid' in cell_val and ',' in cell_val:
                header_row_idx = i
                header_col_idx = j
                break
        if header_row_idx is not None:
            break
    
    if header_row_idx is None:
        # Fallback: intentar leer directamente asumiendo estructura típica
        try:
            df = pd.read_excel(BytesIO(file_content), header=12, engine='openpyxl')
            df.columns = [str(c).strip() for c in df.columns]
            return df.dropna(how='all').reset_index(drop=True)
        except:
            raise ValueError("No se pudo detectar automáticamente la fila de encabezados. Verifica que el archivo sea un export válido de Ariba Analysis.")
    
    # Extraer y parsear los nombres de columnas desde la celda concatenada
    raw_headers = str(df_raw.iloc[header_row_idx, header_col_idx])
    column_names = [c.strip() for c in raw_headers.split(',') if c.strip()]
    
    # Leer datos reales (saltando filas de metadata)
    df_data = pd.read_excel(BytesIO(file_content), header=None, skiprows=range(header_row_idx + 1), engine='openpyxl')
    
    # Asignar nombres de columna
    if len(column_names) <= df_data.shape[1]:
        df_data.columns = column_names + [f'Extra_{i}' for i in range(len(column_names), df_data.shape[1])]
    else:
        df_data.columns = column_names[:df_data.shape[1]]
    
    # Limpiar filas vacías
    df_data = df_data.dropna(how='all').reset_index(drop=True)
    
    return df_data
def transformar_pivot_a_consolidado(df_pivot: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma el Pivot crudo al formato esperado por el dashboard:
    - Mapea campos técnicos de Ariba a nombres en español
    - Valida compradores contra lista maestra
    - Filtra contratos cerrados y datos inválidos
    - Formatea fechas y montos
    """
    # Mapeo de campos Ariba → Consolidado
    ARIBA_MAP = {
        'ContractId': 'contrato_ariba',
        'ProjectInfo.ProjectName': 'descripcion',
        'Owner.UserName': 'comprador_estrategico_raw',
        'ContractStatus': 'estado_contrato',
        'UF_string11': 'rut',
        'UF_string10': 'cod_sap',
        'AffectedParties.CommonSupplierName': 'proveedor',
        'EffectiveDate.Day': 'fecha_inicio',
        'ExpirationDate.Day': 'fecha_termino',
        'Description': 'descripcion_alt',
        'Region.RegionNameL2': 'region',
        'IsEvergreen': 'es_indefinido_raw',
        'UF_boolean1': 'aplica_garantia',
        'sum(Amount)': 'monto_contrato',
    }
    
    df = pd.DataFrame()
    for ariba_col, target_col in ARIBA_MAP.items():
        if ariba_col in df_pivot.columns:
            df[target_col] = df_pivot[ariba_col].copy()
    
    # Validación estricta de compradores
    if 'comprador_estrategico_raw' in df.columns:
        raw_owners = df['comprador_estrategico_raw'].fillna('').astype(str)
        classified = raw_owners.apply(classify_buyer_strict)
        df['comprador_estrategico'] = [x[1] if x[0] == 'strategic' else '' for x in classified]
        df['comprador_tactico'] = [x[1] if x[0] == 'tactical' else '' for x in classified]
    else:
        df['comprador_estrategico'] = ''
        df['comprador_tactico'] = ''
    
    # 🗑️ Eliminar filas sin compradores válidos
    mask_valid_buyer = (df['comprador_estrategico'] != '') | (df['comprador_tactico'] != '')
    df = df[mask_valid_buyer].reset_index(drop=True)
    
    # 🚫 Eliminar contratos Cerrados
    if 'estado_contrato' in df.columns:
        mask_no_cerrado = ~df['estado_contrato'].astype(str).str.strip().str.lower().isin(['cerrado', 'cerrados'])
        df = df[mask_no_cerrado].reset_index(drop=True)
    
    # 🛡️ Copiar Estratégico a Táctico si está vacío
    mask_tactical_empty = (df['comprador_tactico'] == '') & (df['comprador_estrategico'] != '')
    df.loc[mask_tactical_empty, 'comprador_tactico'] = df.loc[mask_tactical_empty, 'comprador_estrategico']
    
    # Formatear fechas
    for date_col in ['fecha_inicio', 'fecha_termino']:
        if date_col in df.columns:
            df[date_col] = df[date_col].apply(parse_fecha)
    
    # Días para vencimiento
    hoy = pd.Timestamp.today().normalize()
    df['dias_para_vencimiento'] = (df['fecha_termino'] - hoy).dt.days
    
    # Es indefinido
    def es_indef(row):
        raw = str(row.get('es_indefinido_raw', '')).strip().lower()
        if raw in ['sí', 'si', 'yes', '1', 'true', 'indefinido']:
            return True
        if pd.notna(row.get('fecha_termino')) and row['fecha_termino'].year > 2100:
            return True
        return False
    df['es_indefinido'] = df.apply(es_indef, axis=1)
    
    # Riesgo spot
    def clasificar_riesgo(estado: str, dias_restantes, es_indefinido: bool = False) -> str:
        estados_bajos = ['Publicado', 'En revisión', 'Aprobado']
        estados_medios = ['Próximo a vencer', 'Por vencer', 'En modificación', 'Modificación del borrador']
        estados_altos = ['Vencido', 'Cancelado', 'Terminado']
        estados_revisar = ['Borrador', 'En espera']
        if es_indefinido: return 'BAJO 🟢'
        if pd.isna(dias_restantes): return 'REVISAR ⚪'
        dias = int(dias_restantes)
        if estado in estados_altos or dias < 0: return 'ALTO 🔴'
        if estado in estados_medios or dias <= 30: return 'MEDIO 🟡'
        if estado in estados_revisar: return 'REVISAR ⚪'
        if estado in estados_bajos or dias > 30: return 'BAJO 🟢'
        return 'REVISAR ⚪'
    
    df['riesgo_spot'] = df.apply(
        lambda r: clasificar_riesgo(str(r.get('estado_contrato', '')), r.get('dias_para_vencimiento'), r.get('es_indefinido', False)),
        axis=1
    )
    
    # Monto
    if 'monto_contrato' in df.columns:
        df['monto_contrato_num'] = pd.to_numeric(df['monto_contrato'], errors='coerce').fillna(0)
    
    # Limpiar comprador (quitar filas sin contrato)
    df = df[df['contrato_ariba'].notna() & (df['contrato_ariba'].astype(str).str.strip() != '')]
    
    return df

def clasificar_riesgo_contrato(estado: str, dias_restantes, es_indefinido: bool = False) -> str:
    estados_bajos = ['Publicado', 'En revisión', 'Aprobado']
    estados_medios = ['Próximo a vencer', 'Por vencer', 'En modificación', 'Modificación del borrador']
    estados_altos = ['Vencido', 'Cancelado', 'Terminado']
    estados_revisar = ['Borrador', 'En espera']
    if es_indefinido: return 'BAJO 🟢'
    if pd.isna(dias_restantes): return 'REVISAR ⚪'
    dias = int(dias_restantes)
    if estado in estados_altos or dias < 0: return 'ALTO 🔴'
    if estado in estados_medios or dias <= 30: return 'MEDIO 🟡'
    if estado in estados_revisar: return 'REVISAR ⚪'
    if estado in estados_bajos or dias > 30: return 'BAJO 🟢'
    return 'REVISAR ⚪'

@st.cache_data(show_spinner=False)
def cargar_y_procesar_contratos(file_hash: str, file_content: bytes, usar_pivot_crudo: bool = True) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
    if usar_pivot_crudo:
        # Parsear Pivot crudo de Ariba
        df_pivot = cargar_pivot_crudo(file_content)
        df = transformar_pivot_a_consolidado(df_pivot)
        df_bg = pd.DataFrame()  # El Pivot crudo no trae hoja BG separada
        reporte = {
            'fuente_principal': 'Pivot Ariba (crudo)',
            'contratos_totales': len(df),
            'sin_fecha_termino': int(df['fecha_termino'].isna().sum()),
            'sin_proveedor': int(df['proveedor'].isna().sum() if 'proveedor' in df.columns else 0),
            'sin_area': 'N/A (requiere consolidado)',
            'compradores_unicos': df['comprador_estrategico'].nunique(),
        }
        return df, df_bg, reporte
    else:
        # Modo legacy: archivo ya limpio con hojas Info Ariba, Consolidado, etc.
        sheets = pd.read_excel(BytesIO(file_content), sheet_name=None, engine='openpyxl')
        if 'Info Ariba' not in sheets:
            raise ValueError("No se encontró la hoja 'Info Ariba' en el archivo.")
        df_ariba = sheets['Info Ariba'].dropna(how='all').dropna(axis=1, how='all').copy()
        df_ariba = df_ariba.rename(columns={
            'ID de contrato': 'contrato_ariba', 'Proyecto - Nombre del proyecto': 'descripcion',
            'Nombre del propietario': 'comprador_estrategico', 'Partes afectadas - Proveedor común': 'proveedor',
            'Rut empresa proveedor': 'rut', 'Código acreedor SAP': 'cod_sap',
            'Es Indefinido': 'es_indefinido_raw', 'Región - Región (L2)': 'region',
            'Fecha de entrada en vigor - Fecha': 'fecha_inicio', 'Fecha de expiración - Fecha': 'fecha_termino',
            'Estado del contrato': 'estado_contrato', 'Aplica Garantía': 'aplica_garantia',
            'sum(Importe Monto total Contrato)': 'monto_contrato',
        })
        df_ariba.columns = df_ariba.columns.astype(str)
        hoja_consol = next((h for h in ['Consolidado de Contratos', 'Antiguo'] if h in sheets), None)
        if hoja_consol:
            df_consol = sheets[hoja_consol].dropna(how='all').dropna(axis=1, how='all').copy()
            df_consol.columns = [' '.join(c.split()) for c in df_consol.columns.astype(str)]
            df_consol = df_consol.rename(columns={
                'Contrato Ariba': 'contrato_ariba', 'Área': 'area', 'Gerencia': 'gerencia',
                'Planta': 'planta', 'Comprador Táctico': 'comprador_tactico',
                'Comprador Estratégico': 'comprador_estrategico_consol',
                'Monto Garantía': 'monto_garantia', 'Vencimiento Garantía': 'vencimiento_garantia',
                'Tipo Garantía': 'tipo_garantia', 'N° Garantia': 'n_garantia',
                'Moneda Garantía': 'moneda_garantia', 'Aplica Boleta de Garantía (Ariba)': 'boleta_ariba',
                'Aplica Boleta de Garantía (Contrato firmado)': 'boleta_contrato',
                'Administrador de Contrato': 'administrador_contrato', 'Correo Electrónico': 'correo',
                'Ingresa a Planta': 'ingresa_planta', 'Contratos Indefinidos': 'contratos_indefinidos',
            })
            cols_merge = [c for c in ['contrato_ariba', 'area', 'gerencia', 'planta', 'comprador_tactico',
                'monto_garantia', 'vencimiento_garantia', 'tipo_garantia', 'n_garantia', 'moneda_garantia',
                'boleta_ariba', 'boleta_contrato', 'administrador_contrato', 'correo', 'ingresa_planta', 'contratos_indefinidos'
            ] if c in df_consol.columns]
            df = df_ariba.merge(df_consol[cols_merge], on='contrato_ariba', how='left')
        else:
            df = df_ariba.copy()
        df_bg = pd.DataFrame()
        if 'BG' in sheets:
            df_bg = sheets['BG'].dropna(how='all').dropna(axis=1, how='all').copy()
            df_bg.columns = df_bg.columns.astype(str).str.strip().str.lower().str.replace(' ', '_').str.replace('-', '_')
        hoy = pd.Timestamp.today().normalize()
        df['fecha_termino'] = df['fecha_termino'].apply(parse_fecha)
        df['fecha_inicio'] = df['fecha_inicio'].apply(parse_fecha)
        if 'vencimiento_garantia' in df.columns:
            df['vencimiento_garantia'] = df['vencimiento_garantia'].apply(parse_fecha)
        df['dias_para_vencimiento'] = (df['fecha_termino'] - hoy).dt.days
        def es_indef(row):
            raw = str(row.get('es_indefinido_raw', '')).strip().lower()
            if raw in ['sí', 'si', 'yes', '1', 'true', 'indefinido']: return True
            if pd.notna(row.get('fecha_termino')) and row['fecha_termino'].year > 2100: return True
            if pd.notna(row.get('contratos_indefinidos')):
                v = str(row['contratos_indefinidos']).strip().lower()
                if v in ['sí', 'si', 'yes', 'indefinido']: return True
            return False
        df['es_indefinido'] = df.apply(es_indef, axis=1)
        df['riesgo_spot'] = df.apply(lambda r: clasificar_riesgo_contrato(str(r.get('estado_contrato', '')), r.get('dias_para_vencimiento'), r.get('es_indefinido', False)), axis=1)
        if 'monto_contrato' in df.columns:
            df['monto_contrato_num'] = pd.to_numeric(df['monto_contrato'], errors='coerce').fillna(0)
        if 'monto_garantia' in df.columns:
            df['monto_garantia_num'] = df['monto_garantia'].apply(limpiar_monto)
        df = df[df['contrato_ariba'].notna() & (df['contrato_ariba'].astype(str).str.strip() != '')]
        reporte = {
            'fuente_principal': 'Info Ariba (limpio)',
            'contratos_totales': len(df),
            'sin_fecha_termino': int(df['fecha_termino'].isna().sum()),
            'sin_proveedor': int(df['proveedor'].isna().sum()),
            'sin_area': int(df['area'].isna().sum()) if 'area' in df.columns else 'N/A',
            'compradores_unicos': df['comprador_estrategico'].nunique(),
        }
        return df, df_bg, reporte

# ==============================
# 📊 FUNCIONES DE VISUALIZACIÓN (sin cambios)
# ==============================

def crear_kpi_cards(df: pd.DataFrame) -> None:
    total = len(df)
    vigentes = len(df[df['riesgo_spot'] == 'BAJO 🟢'])
    por_vencer = len(df[df['riesgo_spot'] == 'MEDIO 🟡'])
    vencidos = len(df[df['riesgo_spot'] == 'ALTO 🔴'])
    revisar = len(df[df['riesgo_spot'] == 'REVISAR ⚪'])
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("📋 Total Contratos", f"{total:,}")
    k2.metric("✅ Vigentes", f"{vigentes:,}", delta=f"{vigentes/total*100:.1f}%" if total else "0%")
    k3.metric("⚠️ Por Vencer (≤30d)", f"{por_vencer:,}", delta_color="inverse")
    k4.metric("🚨 Vencidos", f"{vencidos:,}", delta_color="inverse")
    k5.metric("🔍 Revisar", f"{revisar:,}")

COLOR_MAP = {'BAJO 🟢': '#27ae60', 'MEDIO 🟡': '#f39c12', 'ALTO 🔴': '#e74c3c', 'REVISAR ⚪': '#95a5a6'}

def crear_grafico_riesgo(df: pd.DataFrame) -> go.Figure:
    datos = df['riesgo_spot'].value_counts().reset_index()
    datos.columns = ['Riesgo', 'Cantidad']
    fig = px.pie(datos, values='Cantidad', names='Riesgo', color='Riesgo', color_discrete_map=COLOR_MAP, title="🎯 Distribución de Riesgo", hole=0.4)
    fig.update_traces(textinfo='percent+label')
    return fig

def crear_timeline_vencimientos(df: pd.DataFrame) -> go.Figure:
    if 'dias_para_vencimiento' not in df.columns: return None
    df_f = df[(df['dias_para_vencimiento'] >= -30) & (df['dias_para_vencimiento'] <= 90)].copy()
    df_f = df_f.dropna(subset=['fecha_termino'])
    if df_f.empty: return None
    df_f['mes_venc'] = df_f['fecha_termino'].dt.to_period('M').astype(str)
    agrup = df_f.groupby('mes_venc').size().reset_index(name='Cantidad')
    fig = px.bar(agrup, x='mes_venc', y='Cantidad', title="📅 Vencimientos Próximos (90 días)", color='Cantidad', color_continuous_scale='YlOrRd')
    fig.update_layout(xaxis_title="Mes", yaxis_title="Contratos")
    return fig

def crear_tabla_alertas(df: pd.DataFrame) -> pd.DataFrame:
    mask_riesgo = df['riesgo_spot'].isin(['ALTO 🔴', 'MEDIO 🟡'])
    mask_bg = pd.Series([False] * len(df), index=df.index)
    if 'aplica_garantia' in df.columns:
        mask_bg = df['aplica_garantia'].astype(str).str.lower().str.contains('sí|si|yes', na=False)
    alertas = []
    for _, row in df[mask_riesgo & mask_bg].iterrows():
        alertas.append({
            'Contrato': row.get('contrato_ariba', 'N/A'), 'Proveedor': row.get('proveedor', 'N/A'),
            'Comprador': row.get('comprador_estrategico', 'N/A'), 'Riesgo': row.get('riesgo_spot', 'N/A'),
            'Días Restantes': row.get('dias_para_vencimiento', 'N/A'), 'Estado Contrato': row.get('estado_contrato', 'N/A'),
            'Monto Garantía': f"{row.get('monto_contrato_num', 0):,.0f}" if 'monto_contrato_num' in row else 'N/A',
            'Acción': 'Renovar' if row.get('riesgo_spot') == 'MEDIO 🟡' else 'Regularizar',
        })
    return pd.DataFrame(alertas).sort_values('Días Restantes') if alertas else pd.DataFrame()

# ==============================
# 🎛️ INTERFAZ PRINCIPAL
# ==============================

st.title("📋 Dashboard de Gestión de Contratos")
st.markdown("**Softys Chile** · Compras Estratégicas y Tácticas · Fuente: Pivot Ariba (crudo)")
st.divider()

with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/1/14/Softys_logo.svg/320px-Softys_logo.svg.png", use_container_width=True)
    st.header("📁 Carga de Archivo")
    
    modo_carga = st.radio("Tipo de archivo", ["🔄 Pivot crudo de Ariba", "📄 Consolidado ya limpio"], index=0)
    uploaded_file = st.file_uploader(
        "Sube el archivo (.xlsx)",
        type=['xlsx', 'xls'],
        help="Pivot crudo: archivo directo de Ariba Analysis. Consolidado: archivo ya procesado con hojas Info Ariba, Consolidado, etc."
    )
    st.divider()
    st.caption("💡 El archivo se procesa localmente. Ningún dato sale de tu equipo.")

if not uploaded_file:
    st.info("👆 Sube el archivo **Pivot de Ariba** o **Consolidado de Contratos** para comenzar.")
    st.markdown("""
    ### ¿Qué verás en este dashboard?
    - 🔴 **Alertas de contratos vencidos y por vencer**
    - 📊 **KPIs de riesgo spot** por gerencia, área y comprador
    - 🔍 **Análisis de boletas de garantía**
    - 📥 **Exportación filtrada** lista para reportes

    > **Modo Pivot crudo:** Se parsea automáticamente el archivo de Ariba Analysis, se validan compradores contra lista maestra y se filtran contratos cerrados.
    > **Modo Consolidado:** Usa el archivo ya procesado con hojas Info Ariba, Consolidado, BG, etc.
    """)
    st.stop()

if uploaded_file.size > MAX_FILE_SIZE_BYTES:
    st.error("❌ Archivo demasiado grande (máx. 50 MB).")
    st.stop()

uploaded_file.seek(0)
content = uploaded_file.read()
h = hashlib.md5(content).hexdigest()

try:
    with st.spinner("🔄 Procesando datos (modo: " + ("Pivot crudo" if modo_carga == "🔄 Pivot crudo de Ariba" else "Consolidado limpio") + ")..."):
        usar_crudo = (modo_carga == "🔄 Pivot crudo de Ariba")
        df, df_bg, reporte_calidad = cargar_y_procesar_contratos(h, content, usar_pivot_crudo=usar_crudo)
except Exception as e:
    st.error(f"❌ Error al procesar el archivo: {str(e)}")
    st.stop()

if df.empty:
    st.error("❌ No se encontraron contratos válidos después del procesamiento.")
    st.stop()

# ==============================
# 🎛️ FILTROS EN SIDEBAR
# ==============================

with st.sidebar:
    st.header("🎛️ Filtros")
    estados = ['Todos'] + sorted(df['riesgo_spot'].dropna().unique().tolist())
    riesgo_sel = st.selectbox("Riesgo Spot", estados)
    compradores = ['Todos'] + sorted(df['comprador_estrategico'].dropna().unique().astype(str).tolist())
    comprador_sel = st.selectbox("👤 Comprador Estratégico", compradores)
    if 'gerencia' in df.columns:
        gerencias = ['Todas'] + sorted(df['gerencia'].dropna().unique().astype(str).tolist())
        gerencia_sel = st.selectbox("Gerencia", gerencias)
    else:
        gerencia_sel = 'Todas'
    if 'region' in df.columns:
        areas = ['Todas'] + sorted(df['region'].dropna().unique().astype(str).tolist())
        area_sel = st.selectbox("Área/Región", areas)
    else:
        area_sel = 'Todas'
    estados_contrato = ['Todos'] + sorted(df['estado_contrato'].dropna().unique().astype(str).tolist())
    estado_sel = st.selectbox("Estado Contrato", estados_contrato)

# Aplicar filtros
df_f = df.copy()
if riesgo_sel != 'Todos': df_f = df_f[df_f['riesgo_spot'] == riesgo_sel]
if comprador_sel != 'Todos': df_f = df_f[df_f['comprador_estrategico'] == comprador_sel]
if gerencia_sel != 'Todas' and 'gerencia' in df_f.columns: df_f = df_f[df_f['gerencia'] == gerencia_sel]
if area_sel != 'Todas' and 'region' in df_f.columns: df_f = df_f[df_f['region'] == area_sel]
if estado_sel != 'Todos': df_f = df_f[df_f['estado_contrato'] == estado_sel]

# ==============================
# 📊 KPIs Y GRÁFICOS
# ==============================

st.subheader("📊 Resumen Ejecutivo")
crear_kpi_cards(df_f)
col_graf1, col_graf2 = st.columns(2)
with col_graf1:
    st.plotly_chart(crear_grafico_riesgo(df_f), use_container_width=True)
with col_graf2:
    fig_tl = crear_timeline_vencimientos(df_f)
    if fig_tl:
        st.plotly_chart(fig_tl, use_container_width=True)

# ==============================
# 🚨 ALERTAS
# ==============================

st.subheader("🚨 Alertas de Acción Inmediata")
df_alertas = crear_tabla_alertas(df_f)
if not df_alertas.empty:
    def highlight_risk(val):
        if 'ALTO' in str(val): return 'background-color: #fef2f2'
        if 'MEDIO' in str(val): return 'background-color: #fffbeb'
        return ''
    st.dataframe(df_alertas.style.map(highlight_risk, subset=['Riesgo']), use_container_width=True)
    st.info(f"💡 **{len(df_alertas)} contratos** requieren acción inmediata.")
else:
    st.success("✅ No hay contratos críticos con boleta de garantía pendiente.")

# ==============================
# 📋 TABS PRINCIPALES
# ==============================

tab1, tab2, tab3, tab4 = st.tabs(["📊 Resumen General", "🏢 Por Área / Estado", "🔒 Garantías", "🔍 Explorador"])

with tab1:
    c1, c2 = st.columns(2)
    with c1:
        estado_counts = df_f['estado_contrato'].value_counts().reset_index()
        estado_counts.columns = ['Estado', 'Cantidad']
        fig_est = px.bar(estado_counts, x='Cantidad', y='Estado', orientation='h', title='Contratos por Estado', color='Cantidad', color_continuous_scale='Blues')
        fig_est.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig_est, use_container_width=True)
    with c2:
        if 'region' in df_f.columns:
            df_region = df_f.groupby(['region', 'riesgo_spot']).size().reset_index(name='Cantidad')
            fig_riesgo = px.bar(df_region, x='region', y='Cantidad', color='riesgo_spot', title='Riesgo por Área/Región', color_discrete_map=COLOR_MAP, barmode='stack')
            fig_riesgo.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_riesgo, use_container_width=True)
    comp_counts = df_f.groupby(['comprador_estrategico', 'riesgo_spot']).size().reset_index(name='Cantidad')
    fig_comp = px.bar(comp_counts, x='Cantidad', y='comprador_estrategico', color='riesgo_spot', title='📌 Contratos por Comprador', barmode='stack', orientation='h', color_discrete_map=COLOR_MAP)
    fig_comp.update_layout(yaxis={'categoryorder': 'total ascending'}, height=400)
    st.plotly_chart(fig_comp, use_container_width=True)

with tab2:
    if 'region' in df_f.columns:
        df_area = df_f.groupby(['region', 'riesgo_spot']).size().reset_index(name='Cantidad')
        top_areas = df_f['region'].value_counts().head(12).index
        df_area = df_area[df_area['region'].isin(top_areas)]
        fig_area = px.bar(df_area, x='Cantidad', y='region', color='riesgo_spot', title='Top 12 Áreas por Riesgo', barmode='stack', orientation='h', color_discrete_map=COLOR_MAP)
        fig_area.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig_area, use_container_width=True)
    else:
        st.info("ℹ️ Columna 'Área/Región' no disponible en este modo.")

with tab3:
    if 'aplica_garantia' in df.columns:
        df_con_bg = df_f[df_f['aplica_garantia'].astype(str).str.lower().str.contains('sí|si|yes', na=False)]
        if not df_con_bg.empty:
            st.success(f"✅ {len(df_con_bg)} contratos con garantía aplicada están siendo monitoreados.")
        else:
            st.info("ℹ️ No hay contratos con garantía aplicada en los datos filtrados.")
    else:
        st.info("ℹ️ Campo 'Aplica Garantía' no disponible en este modo.")

with tab4:
    st.markdown("### 🔍 Explorador de Datos")
    cols_default = [c for c in ['contrato_ariba', 'proveedor', 'comprador_estrategico', 'region', 'estado_contrato', 'fecha_termino', 'dias_para_vencimiento', 'riesgo_spot'] if c in df_f.columns]
    col_mostrar = st.multiselect("Columnas a mostrar", options=df_f.columns.tolist(), default=cols_default)
    search_term = st.text_input("🔎 Buscar proveedor o descripción")
    df_view = df_f.copy()
    if search_term:
        mask = pd.Series([False] * len(df_view), index=df_view.index)
        for col in ['proveedor', 'descripcion', 'contrato_ariba']:
            if col in df_view.columns:
                mask |= df_view[col].astype(str).str.contains(search_term, case=False, na=False)
        df_view = df_view[mask]
    if col_mostrar:
        df_view = df_view[col_mostrar]
    if 'dias_para_vencimiento' in df_view.columns:
        df_view = df_view.sort_values('dias_para_vencimiento')
    st.dataframe(df_view, use_container_width=True, height=450)
    st.caption(f"Mostrando {len(df_view):,} de {len(df_f):,} contratos filtrados")

# ==============================
# 📥 EXPORTACIÓN
# ==============================

st.divider()
st.subheader("📥 Exportar Resultados")
ec1, ec2, ec3 = st.columns(3)
with ec1:
    csv_data = df_f.to_csv(index=False).encode('utf-8-sig')
    st.download_button("💾 Descargar Filtrado (CSV)", data=csv_data, file_name=f"contratos_filtrado_{datetime.now().strftime('%Y%m%d_%H%M')}.csv", mime="text/csv")
with ec2:
    criticos = df_f[df_f['riesgo_spot'].isin(['ALTO 🔴', 'MEDIO 🟡'])]
    csv_crit = criticos.to_csv(index=False).encode('utf-8-sig')
    st.download_button("🔴 Solo Contratos en Riesgo", data=csv_crit, file_name=f"contratos_riesgo_{datetime.now().strftime('%Y%m%d_%H%M')}.csv", mime="text/csv")
with ec3:
    urgentes = df_f[df_f['dias_para_vencimiento'].between(0, 60)] if 'dias_para_vencimiento' in df_f.columns else pd.DataFrame()
    csv_urg = urgentes.to_csv(index=False).encode('utf-8-sig')
    st.download_button("⚠️ Vencen en 60 Días", data=csv_urg, file_name=f"contratos_urgentes_{datetime.now().strftime('%Y%m%d_%H%M')}.csv", mime="text/csv")

# ==============================
# 🔧 DIAGNÓSTICO TÉCNICO
# ==============================

with st.expander("🔧 Diagnóstico Técnico"):
    d1, d2 = st.columns(2)
    with d1:
        st.markdown("**Calidad de datos:**")
        st.json(reporte_calidad)
    with d2:
        st.markdown("**Resumen del dataset:**")
        st.json({
            "Total contratos procesados": len(df),
            "Contratos filtrados": len(df_f),
            "Compradores únicos": int(df['comprador_estrategico'].nunique()),
            "Proveedores únicos": int(df['proveedor'].nunique()),
            "Última actualización": datetime.now().strftime('%d/%m/%Y %H:%M'),
        })

# ==============================
# ℹ️ FOOTER
# ==============================

st.divider()
st.caption(f"""
🔹 Dashboard generado: {datetime.now().strftime('%d/%m/%Y %H:%M')}  
🔹 **Fuente**: {"Pivot Ariba (crudo) - parseado automáticamente" if modo_carga == "🔄 Pivot crudo de Ariba" else "Consolidado de Contratos (ya limpio)"}  
🔹 **Validación**: Compradores filtrados contra lista maestra oficial  
🔹 **Filtros aplicados**: Contratos "Cerrados" eliminados automáticamente
""")

# ==============================
# 🤖 ASISTENTE VIRTUAL (GEMINI) - SIN CAMBIOS
# ==============================

import google.generativeai as genai
st.divider()
st.subheader("💬 Asistente Virtual de Compras")
st.caption("Modelo: Gemini 2.0 Flash")
api_key_gemini = st.secrets.get("GEMINI_API_KEY", None)
if not api_key_gemini:
    api_key_gemini = st.text_input("🔑 Tu API Key de Gemini", type="password", help="Obtén una gratis en aistudio.google.com")
if api_key_gemini:
    try:
        genai.configure(api_key=api_key_gemini)
        model = genai.GenerativeModel('gemini-2.0-flash')
        if "messages" not in st.session_state:
            st.session_state.messages = []
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])
        if prompt := st.chat_input("Ej: ¿Qué contratos vencen este mes?"):
            st.session_state.messages.append({"role": "user", "content": prompt})
            with st.chat_message("user"):
                st.markdown(prompt)
            with st.chat_message("assistant"):
                with st.spinner("🤖 Pensando..."):
                    try:
                        cols_ia = [c for c in ['contrato_ariba', 'proveedor', 'comprador_estrategico', 'estado_contrato', 'fecha_termino', 'riesgo_spot', 'region'] if c in df_f.columns]
                        datos_muestra = df_f[cols_ia].head(10).to_string(index=False)
                        prompt_sistema = f"""
Eres un asistente de gestión de contratos de Softys Chile.
Fuente de datos: {"Pivot Ariba procesado" if modo_carga == "🔄 Pivot crudo de Ariba" else "Consolidado de Contratos"} ({len(df_f)} contratos filtrados).

MUESTRA (10 de {len(df_f)} registros):
{datos_muestra}

REGLAS:
1. Responde SOLO con esta información.
2. Si no está en la muestra, indica: 'No tengo ese detalle en la muestra actual'.
3. Sé breve y directo.
"""
                        response = model.generate_content([prompt_sistema, prompt])
                        respuesta = response.text
                        st.markdown(respuesta)
                        st.session_state.messages.append({"role": "assistant", "content": respuesta})
                    except Exception as e:
                        if "429" in str(e):
                            st.error("⚠️ Límite diario de Gemini alcanzado. Intenta mañana.")
                        else:
                            st.error(f"Error: {str(e)}")
    except Exception as e:
        st.error(f"Error de configuración Gemini: {str(e)}")
else:
    st.info("👈 Ingresa tu API Key de Gemini para activar el asistente.")
