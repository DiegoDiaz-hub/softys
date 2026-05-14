# config.py — Constantes y configuración global Softys Dashboard

# ── Compradores ───────────────────────────────────────────────
ESTRATEGICOS = {
    "Bárbara García", "BPO", "Claudio Berrios",
    "Denisse Andrea Gonzalez Terrile", "Jorge Alfonso Urrutia Carillo",
    "Jorge Urrutia", "Joseph España", "Juan Daniel Figueroa",
    "Juan Figueroa", "Magdalena Farias", "Martina Fuentes",
    "Patricio Espinoza", "Viviana Grandón",
}
TACTICOS = {
    "BPO", "Dayana Dávila", "Joseph España",
    "Leonardo Nacarate", "Patricio Espinoza",
}
TODOS_COMPRADORES = ESTRATEGICOS | TACTICOS

# ── Normalización de nombres ──────────────────────────────────
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
}

# ── Mapeo columnas Pivot Ariba ────────────────────────────────
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

# ── Mapeo columnas Consolidado ────────────────────────────────
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

# ── Colores por riesgo ────────────────────────────────────────
RIESGO_COLORES = {
    "BAJO 🟢":    "#00A651",
    "MEDIO 🟡":   "#F59E0B",
    "ALTO 🔴":    "#E02020",
    "REVISAR ⚪": "#8FA3B8",
}

# ── Colores sincronización ────────────────────────────────────
SYNC_COLORES = {
    "OK":               "#00A651",
    "DESACTUALIZADO":   "#E02020",
    "NUEVO EN ARIBA":   "#0072CE",
    "SOLO CONSOLIDADO": "#6C3FC4",
    "REVISAR":          "#F59E0B",
}
SYNC_BG = {
    "OK":               "#F0FDF4",
    "DESACTUALIZADO":   "#FEF2F2",
    "NUEVO EN ARIBA":   "#EAF2FB",
    "SOLO CONSOLIDADO": "#F5F0FF",
    "REVISAR":          "#FFFBEB",
}
SYNC_FG = {
    "OK":               "#14532D",
    "DESACTUALIZADO":   "#7F1D1D",
    "NUEVO EN ARIBA":   "#1E3A5F",
    "SOLO CONSOLIDADO": "#3B1A78",
    "REVISAR":          "#78350F",
}