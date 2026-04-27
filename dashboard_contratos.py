import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# ==============================
# 🧠 CONFIGURACIÓN INICIAL
# ==============================
st.set_page_config(page_title="Comparador de Contratos — Softys", page_icon="🔍", layout="wide")

# ==============================
# 🎨 ESTILOS
# ==============================
st.markdown("""
<style>
    .diff-match { color: #27ae60; font-weight: bold; }
    .diff-mismatch { color: #e74c3c; font-weight: bold; background: #fff3cd; padding: 2px 6px; border-radius: 4px; }
    .metric-card { background: linear-gradient(135deg, #1e3a5f, #2d5986); border-radius: 10px; padding: 15px; color: white; text-align: center; }
    div[data-testid="stMetricValue"] { font-size: 1.8rem; }
</style>
""", unsafe_allow_html=True)

# ==============================
# ✅ LISTAS OFICIALES DE COMPRADORES
# ==============================
STRATEGIC_BUYERS = {
    'Patricio Espinoza', 'Jorge Urrutia', 'Bárbara García', 'Claudio Berrios',
    'Martina Fuentes', 'Joseph España', 'Michelle Palma', 'Juan Figueroa',
    'Magdalena Farias', 'Denisse Andrea Gonzalez Terrile', 'Jorge Alfonso Urrutia Carillo',
    'Viviana Grandón', 'Priscilla Gre Guerra', 'Juan Daniel Figueroa', 'Dayana Dávila'
}
TACTICAL_BUYERS = {
    'Leonardo Nacarate', 'Martina Fuentes', 'Scarlette Lucero',
    'Margarita Lineros', 'Erika Silva', 'Karina Satelo', 'Pablo Labs',
    'Dayana Dávila', 'BPO'
}
ALL_VALID_BUYERS = STRATEGIC_BUYERS | TACTICAL_BUYERS

TYPO_CORRECTIONS = {
    'jorge uturria': 'Jorge Urrutia', 'jorgue urrutia': 'Jorge Urrutia',
    'dennis andrea gonzales': 'Denisse Andrea Gonzalez Terrile',
    'denisse andrea gonzalez terrile': 'Denisse Andrea Gonzalez Terrile',
    'juan daniel figueroa': 'Juan Daniel Figueroa', 'juan figueroa': 'Juan Figueroa',
    'joseph eduardo españa escalona': 'Joseph España', 'joseph españa': 'Joseph España',
    'michelle esperanza': 'Michelle Palma', 'leonardo nacarete': 'Leonardo Nacarate',
    'priscilla gre guerra': 'Priscilla Gre Guerra', 'dayana davila': 'Dayana Dávila'
}

def normalize_name(name):
    if pd.isna(name) or not str(name).strip(): return ''
    clean = str(name).strip().lower()
    clean = ''.join(c for c in clean if c not in 'áéíóúüñ')
    return clean

def is_valid_buyer(name):
    clean = normalize_name(name)
    if not clean: return False
    for typo, correct in TYPO_CORRECTIONS.items():
        if typo in clean or clean in typo: clean = normalize_name(correct); break
    return any(clean in normalize_name(b) or normalize_name(b) in clean for b in ALL_VALID_BUYERS)

def parse_date(val):
    if pd.isna(val) or str(val).strip() in ['99.99.9999', '2999', '31/12/2999', 'Indefinido', '']:
        return pd.NaT
    if isinstance(val, (int, float)):
        try: return pd.Timestamp('1899-12-30') + pd.Timedelta(days=int(val))
        except: return pd.NaT
    try: return pd.to_datetime(str(val).strip(), dayfirst=True, errors='coerce')
    except: return pd.NaT

def normalize_status(val):
    if pd.isna(val): return ''
    s = str(val).strip().lower()
    s = ''.join(c for c in s if c not in 'áéíóúüñ')
    return s

# ==============================
# 🔧 CARGA Y MAPEO INTELIGENTE
# ==============================
def load_and_map(file, file_type):
    xls = pd.ExcelFile(file)
    sheet = xls.sheet_names[0]
    df = pd.read_excel(xls, sheet_name=sheet, header=None)
    
    # Buscar fila de datos (empieza con CW o contiene ID de contrato)
    start_idx = None
    for i in range(len(df)):
        first = str(df.iloc[i, 0]).strip().upper()
        if first.startswith('CW') and len(first) > 2:
            start_idx = i; break
    if start_idx is None:
        raise ValueError(f"No se encontraron contratos válidos en {file_type}")
    
    df_data = df.iloc[start_idx:].reset_index(drop=True)
    df_data.columns = df_data.iloc[0]
    df_data = df_data[1:].reset_index(drop=True)
    
    # Mapeo flexible por palabras clave
    col_map = {}
    for col in df_data.columns:
        c = str(col).lower()
        if 'id de contrato' in c or 'contractid' in c or 'contrato ariba' in c or 'contrato sap' in c:
            col_map['contrato'] = col
        elif 'propietario' in c or 'owner' in c or 'comprador' in c:
            col_map['comprador'] = col
        elif 'inicio' in c and ('fecha' in c or 'entrada' in c):
            col_map['inicio'] = col
        elif 'termino' in c or 'expiracion' in c or 'fin' in c or 'expiración' in c:
            col_map['fin'] = col
        elif 'estado' in c:
            col_map['estado'] = col
            
    if 'contrato' not in col_map:
        raise ValueError(f"No se encontró columna de contrato en {file_type}")
        
    df_out = pd.DataFrame()
    df_out['contrato'] = df_data[col_map['contrato']].astype(str).str.strip()
    df_out['comprador'] = df_data.get(col_map.get('comprador', ''), '')
    df_out['inicio'] = df_data.get(col_map.get('inicio', ''), pd.NaT).apply(parse_date)
    df_out['fin'] = df_data.get(col_map.get('fin', ''), pd.NaT).apply(parse_date)
    df_out['estado'] = df_data.get(col_map.get('estado', ''), '')
    
    # Filtrar solo compradores oficiales y contratos válidos
    df_out = df_out[df_out['contrato'].str.startswith('CW') & df_out['comprador'].apply(is_valid_buyer)]
    df_out['contrato'] = df_out['contrato'].str.upper()
    df_out = df_out.drop_duplicates(subset=['contrato']).set_index('contrato')
    return df_out

# ==============================
# 🔍 COMPARACIÓN
# ==============================
def comparar_archivos(df_pivot, df_consol):
    merged = pd.merge(df_pivot, df_consol, left_index=True, right_index=True, how='outer', suffixes=('_pivot', '_consol'))
    
    resultados = []
    for contrato, row in merged.iterrows():
        comp_pivot = str(row.get('comprador_pivot', '') or row.get('comprador_consol', '')).strip()
        if not comp_pivot: continue
            
        ini_p, ini_c = row.get('inicio_pivot', pd.NaT), row.get('inicio_consol', pd.NaT)
        fin_p, fin_c = row.get('fin_pivot', pd.NaT), row.get('fin_consol', pd.NaT)
        est_p, est_c = str(row.get('estado_pivot', '')).strip(), str(row.get('estado_consol', '')).strip()
        
        diff_ini = ini_p != ini_c
        diff_fin = fin_p != fin_c
        diff_est = normalize_status(est_p) != normalize_status(est_c)
        
        if diff_ini or diff_fin or diff_est:
            accion = "🔄 Actualizar en Consolidado"
            if pd.isna(ini_c) and pd.isna(fin_c) and not est_c: accion = "📥 Agregar al Consolidado"
            elif pd.isna(ini_p) and pd.isna(fin_p) and not est_p: accion = "🗑️ Solo existe en Consolidado"
            
            resultados.append({
                'Contrato': contrato,
                'Comprador': comp_pivot,
                'Inicio (Pivot)': ini_p.strftime('%d/%m/%Y') if pd.notna(ini_p) else '⚠️ Vacío',
                'Inicio (Consol)': ini_c.strftime('%d/%m/%Y') if pd.notna(ini_c) else '⚠️ Vacío',
                '¿Coincide Inicio?': '✅ Sí' if not diff_ini else '❌ No',
                'Fin (Pivot)': fin_p.strftime('%d/%m/%Y') if pd.notna(fin_p) else '⚠️ Vacío',
                'Fin (Consol)': fin_c.strftime('%d/%m/%Y') if pd.notna(fin_c) else '⚠️ Vacío',
                '¿Coincide Fin?': '✅ Sí' if not diff_fin else '❌ No',
                'Estado (Pivot)': est_p if est_p else '⚠️ Vacío',
                'Estado (Consol)': est_c if est_c else '⚠️ Vacío',
                '¿Coincide Estado?': '✅ Sí' if not diff_est else '❌ No',
                'Acción': accion
            })
            
    return pd.DataFrame(resultados)

# ==============================
# 🎛️ INTERFAZ
# ==============================
st.title("🔍 Comparador de Contratos: Pivot vs Consolidado")
st.caption("Detecta diferencias en Fechas y Estado para actualizar tu base de datos")

c1, c2 = st.columns(2)
with c1:
    file_pivot = st.file_uploader("📤 Subir Pivot Ariba (Crudo)", type=['xlsx', 'xls'])
with c2:
    file_consol = st.file_uploader("📤 Subir Consolidado de Contratos", type=['xlsx', 'xls'])

if file_pivot and file_consol:
    with st.spinner("🔄 Procesando y comparando archivos..."):
        try:
            df_p = load_and_map(file_pivot, "Pivot")
            df_c = load_and_map(file_consol, "Consolidado")
            df_diff = comparar_archivos(df_p, df_c)
            
            total = len(df_diff)
            solo_pivot = len(df_diff[df_diff['Acción'].str.contains('Agregar')])
            solo_consol = len(df_diff[df_diff['Acción'].str.contains('Solo existe')])
            diferencias = total - solo_pivot - solo_consol
            
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("🔍 Total Comparados", f"{total:,}")
            k2.metric("❌ Diferencias detectadas", f"{diferencias:,}")
            k3.metric("📥 Nuevos en Pivot", f"{solo_pivot:,}")
            k4.metric("🗑️ Solo en Consolidado", f"{solo_consol:,}")
            
            st.divider()
            st.subheader("📋 Contratos que requieren actualización")
            
            if not df_diff.empty:
                def color_diff(val):
                    if '❌' in str(val): return 'background-color: #fff3cd; color: #856404;'
                    if '🔄' in str(val) or '📥' in str(val): return 'background-color: #d4edda; color: #155724;'
                    return ''
                
                st.dataframe(
                    df_diff.style.applymap(color_diff, subset=['¿Coincide Inicio?', '¿Coincide Fin?', '¿Coincide Estado?', 'Acción']),
                    use_container_width=True, height=400
                )
                
                csv = df_diff.to_csv(index=False).encode('utf-8-sig')
                st.download_button("💾 Descargar reporte de diferencias (CSV)", csv, "diferencias_contratos.csv", "text/csv")
            else:
                st.success("✅ ¡Perfecto! No hay diferencias. Ambos archivos están sincronizados.")
                
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")
else:
    st.info("👆 Sube ambos archivos para comenzar la comparación.")
    
