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
    page_title="Comparador de Contratos — Softys",
    page_icon="🔍",
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
    .diff-highlight {
        background-color: #fff3cd;
        padding: 2px 6px;
        border-radius: 4px;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# ==============================
# 🛡️ CONSTANTES
# ==============================
MAX_FILE_SIZE_BYTES = 50 * 1024 * 1024

# ==============================
# ✅ LISTA MAESTRA DE COMPRADORES (ESTRICTA)
# ==============================
STRATEGIC_BUYERS = {
    'Patricio Espinoza', 'Jorge Urrutia', 'Bárbara García', 'Claudio Berrios',
    'Martina Fuentes', 'Joseph España', 'Michelle Palma', 'Juan Figueroa',
    'Magdalena Farias', 'Denisse Andrea Gonzalez Terrile', 'Jorge Alfonso Urrutia Carillo',
    'Viviana Grandón', 'Priscilla Gre Guerra', 'Juan Daniel Figueroa'
}

TACTICAL_BUYERS = {
    'Leonardo Nacarate', 'Martina Fuentes', 'Scarlette Lucero',
    'Margarita Lineros', 'Erika Silva', 'Karina Satelo', 'Pablo Labs',
    'Dayana Dávila', 'BPO'
}

TYPO_CORRECTIONS = {
    'jorge uturria': 'Jorge Urrutia', 'jorgue urrutia': 'Jorge Urrutia',
    'dennis andrea gonzales': 'Denisse Andrea Gonzalez Terrile',
    'denisse andrea gonzalez terrile': 'Denisse Andrea Gonzalez Terrile',
    'juan daniel figueroa': 'Juan Daniel Figueroa',
    'juan figueroa': 'Juan Figueroa',
    'joseph eduardo españa escalona': 'Joseph España',
    'joseph españa': 'Joseph España',
    'michelle esperanza': 'Michelle Palma',
    'leonardo nacarete': 'Leonardo Nacarate',
    'priscilla gre guerra': 'Priscilla Gre Guerra',
    'dayana davila': 'Dayana Dávila'
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
# 🔧 FUNCIONES DE CARGA Y PROCESAMIENTO
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

def cargar_pivot_crudo(file_content: bytes) -> pd.DataFrame:
    """Parsea el Pivot crudo de Ariba con estructura real de columnas posicionales."""
    # Leer sin header para detectar estructura
    df = pd.read_excel(BytesIO(file_content), header=None, engine='openpyxl')
    
    # Saltar filas de metadata (las que contienen "This Worksheet" o "Query_Filters")
    start_row = 0
    for i in range(len(df)):
        first_cell = str(df.iloc[i, 0]).lower() if pd.notna(df.iloc[i, 0]) else ''
        if first_cell.startswith('cw') and len(first_cell) > 2:  # Contrato válido (CW...)
            start_row = i
            break
    
    if start_row == 0:
        raise ValueError("No se encontraron contratos válidos en el Pivot. Verifica que el archivo contenga datos de Ariba Analysis.")
    
    # Leer desde la fila de datos
    df = pd.read_excel(BytesIO(file_content), header=None, skiprows=start_row, engine='openpyxl')
    
    # Asignar nombres de columnas basados en la estructura del Pivot de Ariba
    # Columnas típicas: 0=ContractId, 1=ProjectName, 2=BeginDate, 3=Owner, 4=SAPCode, 5=IsEvergreen, 6=Region, 7=RUT, 8=Supplier, 9=Description, 10=EffectiveDate, 11=Year, 12=Status, 13=ExpirationDate...
    if len(df.columns) >= 13:
        df.columns = [
            'ContractId', 'ProjectName', 'BeginDate', 'Owner', 'SAPCode', 
            'IsEvergreen', 'Region', 'RUT', 'Supplier', 'Description',
            'EffectiveDate', 'Year', 'Status', 'ExpirationDate'
        ] + [f'Extra_{i}' for i in range(13, len(df.columns))]
    else:
        df.columns = [f'Column_{i}' for i in range(len(df.columns))]
    
    # Limpiar filas vacías
    df = df.dropna(how='all').reset_index(drop=True)
    
    return df

def cargar_consolidado_drive(file_content: bytes) -> pd.DataFrame:
    """Carga el Consolidado de Contratos del drive."""
    sheets = pd.read_excel(BytesIO(file_content), sheet_name=None, engine='openpyxl')
    if 'Consolidado de Contratos' in sheets:
        df = sheets['Consolidado de Contratos'].dropna(how='all').dropna(axis=1, how='all')
    else:
        df = list(sheets.values())[0].dropna(how='all').dropna(axis=1, how='all')
    df.columns = [str(c).strip() for c in df.columns]
    return df.reset_index(drop=True)

def procesar_pivot_a_comparar(df_pivot: pd.DataFrame) -> pd.DataFrame:
    """Procesa el Pivot para comparación usando columnas posicionales."""
    df = pd.DataFrame()
    
    # Mapeo basado en posición/nombre de columnas del Pivot
    if 'ContractId' in df_pivot.columns:
        df['contrato_ariba'] = df_pivot['ContractId']
    elif 'Column_0' in df_pivot.columns:
        df['contrato_ariba'] = df_pivot['Column_0']
    
    if 'Owner' in df_pivot.columns:
        df['comprador_raw'] = df_pivot['Owner']
    elif 'Column_3' in df_pivot.columns:
        df['comprador_raw'] = df_pivot['Column_3']
    
    if 'Status' in df_pivot.columns:
        df['estado_pivot'] = df_pivot['Status']
    elif 'Column_12' in df_pivot.columns:
        df['estado_pivot'] = df_pivot['Column_12']
    
    if 'Supplier' in df_pivot.columns:
        df['proveedor'] = df_pivot['Supplier']
    elif 'Column_8' in df_pivot.columns:
        df['proveedor'] = df_pivot['Column_8']
    
    if 'Description' in df_pivot.columns:
        df['descripcion'] = df_pivot['Description']
    elif 'Column_9' in df_pivot.columns:
        df['descripcion'] = df_pivot['Column_9']
    
    if 'ExpirationDate' in df_pivot.columns:
        df['fecha_termino'] = df_pivot['ExpirationDate']
    elif 'Column_13' in df_pivot.columns:
        df['fecha_termino'] = df_pivot['Column_13']
    
    if 'Region' in df_pivot.columns:
        df['region'] = df_pivot['Region']
    elif 'Column_6' in df_pivot.columns:
        df['region'] = df_pivot['Column_6']
    
    # Validación estricta de compradores
    if 'comprador_raw' in df.columns:
        raw_owners = df['comprador_raw'].fillna('').astype(str)
        classified = raw_owners.apply(classify_buyer_strict)
        df['comprador_estrategico'] = [x[1] if x[0] == 'strategic' else '' for x in classified]
        df['comprador_tactico'] = [x[1] if x[0] == 'tactical' else '' for x in classified]
    else:
        df['comprador_estrategico'] = ''
        df['comprador_tactico'] = ''
    
    # Filtrar solo contratos válidos
    df = df[df['contrato_ariba'].notna() & (df['contrato_ariba'].astype(str).str.strip() != '')]
    df = df[~df['contrato_ariba'].astype(str).str.lower().str.contains('query|field|this worksheet', na=False)]
    
    return df

def procesar_consolidado_a_comparar(df_consol: pd.DataFrame) -> pd.DataFrame:
    """Procesa el Consolidado para comparación."""
    df = pd.DataFrame()
    
    # Buscar columnas con nombres similares
    col_map = {}
    for col in df_consol.columns:
        col_lower = col.lower()
        if 'contrato' in col_lower and ('ariba' in col_lower or 'sap' in col_lower):
            col_map['contrato_ariba'] = col
        elif 'comprador' in col_lower and 'estratégico' in col_lower:
            col_map['comprador_estrategico'] = col
        elif 'comprador' in col_lower and 'táctico' in col_lower:
            col_map['comprador_tactico'] = col
        elif 'estado' in col_lower and 'contrato' in col_lower:
            col_map['estado_consol'] = col
        elif 'proveedor' in col_lower:
            col_map['proveedor'] = col
        elif 'fecha' in col_lower and ('término' in col_lower or 'fin' in col_lower or 'expiración' in col_lower):
            col_map['fecha_termino'] = col
        elif 'descripción' in col_lower or 'descripcion' in col_lower:
            col_map['descripcion'] = col
        elif 'área' in col_lower or 'region' in col_lower:
            col_map['region'] = col
    
    # Mapear columnas encontradas
    for target_col, source_col in col_map.items():
        df[target_col] = df_consol[source_col]
    
    # Filtrar contratos válidos
    if 'contrato_ariba' in df.columns:
        df = df[df['contrato_ariba'].notna() & (df['contrato_ariba'].astype(str).str.strip() != '')]
        df = df[~df['contrato_ariba'].astype(str).str.lower().str.contains('contrato ariba', na=False)]
    
    return df

def comparar_archivos(df_pivot: pd.DataFrame, df_consol: pd.DataFrame) -> pd.DataFrame:
    """Compara ambos archivos y detecta incongruencias."""
    df_pivot_proc = procesar_pivot_a_comparar(df_pivot)
    df_consol_proc = procesar_consolidado_a_comparar(df_consol)
    
    # Merge para comparar
    merged = pd.merge(df_pivot_proc, df_consol_proc, on='contrato_ariba', how='outer', 
                      suffixes=('_pivot', '_consol'), indicator=True)
    
    diferencias = []
    for _, row in merged.iterrows():
        contrato = row['contrato_ariba']
        estado_pivot = str(row.get('estado_pivot', '')).strip()
        estado_consol = str(row.get('estado_consol', '')).strip()
        
        # Obtener comprador
        comprador_pivot = row.get('comprador_estrategico_pivot', '') or row.get('comprador_tactico_pivot', '')
        comprador_consol = row.get('comprador_estrategico_consol', '') or row.get('comprador_tactico_consol', '')
        
        if pd.notna(comprador_pivot) and comprador_pivot:
            comprador = comprador_pivot
        elif pd.notna(comprador_consol) and comprador_consol:
            comprador = comprador_consol
        else:
            continue
        
        # Verificar si hay diferencia de estados
        if estado_pivot != estado_consol and estado_pivot and estado_consol:
            diferencias.append({
                'Contrato': contrato,
                'Comprador': comprador,
                'Estado en Pivot (Ariba)': estado_pivot,
                'Estado en Consolidado (Drive)': estado_consol,
                'Proveedor': row.get('proveedor_pivot') or row.get('proveedor_consol', ''),
                'Descripción': row.get('descripcion_pivot') or row.get('descripcion_consol', ''),
                'Fecha Término': str(row.get('fecha_termino_pivot') or row.get('fecha_termino_consol', ''))[:10],
                'Región': row.get('region_pivot') or row.get('region_consol', ''),
            })
    
    return pd.DataFrame(diferencias)

# ==============================
# 📊 FUNCIONES DE VISUALIZACIÓN
# ==============================

def crear_kpi_comparacion(df_diff: pd.DataFrame) -> None:
    total = len(df_diff)
    por_comprador = df_diff.groupby('Comprador').size().to_dict()
    k1, k2, k3 = st.columns(3)
    k1.metric("🔍 Total Incongruencias", f"{total:,}")
    k2.metric("👥 Compradores Afectados", f"{len(por_comprador):,}")
    k3.metric("📋 Contratos a Actualizar", f"{total:,}", delta_color="inverse")

def crear_grafico_por_comprador(df_diff: pd.DataFrame) -> go.Figure:
    if df_diff.empty:
        return go.Figure()
    datos = df_diff['Comprador'].value_counts().reset_index()
    datos.columns = ['Comprador', 'Cantidad']
    fig = px.bar(datos, x='Cantidad', y='Comprador', orientation='h',
                 title='👥 Incongruencias por Comprador',
                 color='Cantidad', color_continuous_scale='Reds')
    fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=400)
    return fig

def crear_grafico_estados(df_diff: pd.DataFrame) -> go.Figure:
    if df_diff.empty:
        return go.Figure()
    datos = df_diff['Estado en Pivot (Ariba)'].value_counts().reset_index()
    datos.columns = ['Estado en Ariba', 'Cantidad']
    fig = px.pie(datos, values='Cantidad', names='Estado en Ariba',
                 title='📊 Distribución de Estados en Pivot',
                 hole=0.4, color_discrete_sequence=px.colors.qualitative.Set3)
    fig.update_traces(textinfo='percent+label')
    return fig

# ==============================
# 🎛️ INTERFAZ PRINCIPAL
# ==============================

st.title("🔍 Comparador de Contratos - Pivot vs Consolidado")
st.markdown("**Softys Chile** · Detecta incongruencias entre Ariba y el Drive")
st.divider()

with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/1/14/Softys_logo.svg/320px-Softys_logo.svg.png", use_container_width=True)
    st.header("📁 Carga de Archivos")
    st.info("💡 Sube ambos archivos para comparar:")
    pivot_file = st.file_uploader("1️⃣ Pivot crudo de Ariba", type=['xlsx', 'xls'], key='pivot')
    consol_file = st.file_uploader("2️⃣ Consolidado del Drive", type=['xlsx', 'xls'], key='consol')
    st.divider()
    st.caption("🔒 Los archivos se procesan localmente.")

if not pivot_file or not consol_file:
    st.info("👆 Sube **ambos archivos** para comenzar la comparación.")
    st.markdown("""
    ### ¿Qué hace esta herramienta?
    - 🔍 **Compara** el estado de los contratos entre Ariba (Pivot) y el Consolidado del Drive
    - 📊 **Identifica** incongruencias donde los estados no coinciden
    - 👥 **Filtra** por comprador estratégico/táctico
    - 📥 **Exporta** lista de contratos a actualizar
    
    > **Importante:** Solo se consideran compradores de la lista maestra oficial.
    """)
    st.stop()

if pivot_file.size > MAX_FILE_SIZE_BYTES or consol_file.size > MAX_FILE_SIZE_BYTES:
    st.error("❌ Archivo demasiado grande (máx. 50 MB).")
    st.stop()

try:
    with st.spinner("🔄 Procesando y comparando archivos..."):
        pivot_content = pivot_file.read()
        consol_content = consol_file.read()
        df_pivot = cargar_pivot_crudo(pivot_content)
        df_consol = cargar_consolidado_drive(consol_content)
        df_diferencias = comparar_archivos(df_pivot, df_consol)
except Exception as e:
    st.error(f"❌ Error al procesar: {str(e)}")
    st.stop()

if df_diferencias.empty:
    st.success("✅ ¡Excelente! No se encontraron incongruencias entre los archivos.")
    st.markdown("Los estados de los contratos en el Pivot de Ariba y el Consolidado del Drive están sincronizados.")
    st.stop()

# ==============================
# 📊 RESULTADOS
# ==============================

st.subheader("📊 Resumen de Incongruencias")
crear_kpi_comparacion(df_diferencias)
col1, col2 = st.columns(2)
with col1:
    st.plotly_chart(crear_grafico_por_comprador(df_diferencias), use_container_width=True)
with col2:
    st.plotly_chart(crear_grafico_estados(df_diferencias), use_container_width=True)

# ==============================
# 🎛️ FILTROS
# ==============================

st.divider()
st.subheader("🎛️ Filtrar por Comprador")
compradores = ['Todos'] + sorted(df_diferencias['Comprador'].dropna().unique().astype(str).tolist())
comprador_sel = st.selectbox("Seleccionar Comprador", compradores)
df_filtrado = df_diferencias.copy()
if comprador_sel != 'Todos':
    df_filtrado = df_diferencias[df_diferencias['Comprador'] == comprador_sel]

# ==============================
# 📋 TABLA DE DIFERENCIAS
# ==============================

st.divider()
st.subheader(f"📋 Contratos con Incongruencias{' - ' + comprador_sel if comprador_sel != 'Todos' else ''}")
st.markdown(f"**Total:** {len(df_filtrado)} contratos requieren actualización en el Drive")

if not df_filtrado.empty:
    st.dataframe(df_filtrado, use_container_width=True, height=400)
    
    with st.expander("📝 Ver detalles completos"):
        st.write(df_filtrado.to_html(escape=False), unsafe_allow_html=True)

# ==============================
# 📥 EXPORTACIÓN
# ==============================

st.divider()
st.subheader("📥 Exportar Resultados")
col1, col2 = st.columns(2)
with col1:
    csv_data = df_filtrado.to_csv(index=False).encode('utf-8-sig')
    st.download_button(
        label="💾 Descargar CSV",
        data=csv_data,
        file_name=f"incongruencias_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
        mime="text/csv",
        use_container_width=True
    )
with col2:
    excel_data = df_filtrado.to_excel(index=False, engine='openpyxl')
    st.download_button(
        label="📊 Descargar Excel",
        data=excel_data,
        file_name=f"incongruencias_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )

# ==============================
# ℹ️ FOOTER
# ==============================

st.divider()
st.caption(f"""
🔹 Comparación generada: {datetime.now().strftime('%d/%m/%Y %H:%M')}  
🔹 **Fuente Pivot:** {len(df_pivot)} contratos  
🔹 **Fuente Consolidado:** {len(df_consol)} contratos  
🔹 **Incongruencias detectadas:** {len(df_diferencias)} contratos  
🔹 **Filtro aplicado:** {comprador_sel}
""")
