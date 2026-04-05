"""
MoneyFlow Dashboard — Streamlit

Visualiza movimientos de débito y crédito con categorías,
saldo disponible y deuda en tarjeta de crédito.
"""

import json
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="MoneyFlow",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="collapsed",
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tracker.categories import backfill_tracker_categories

DATA_DIR = PROJECT_ROOT / "data" / "processed"

st.markdown(
    """
    <style>
    [data-testid="stMetricLabel"] {
        font-size: 0.82rem !important;
    }
    [data-testid="stMetricValue"] {
        font-size: 1.35rem !important;
        line-height: 1.1 !important;
    }
    [data-testid="stMetricDelta"] {
        font-size: 0.75rem !important;
    }
    div[data-testid="stAlert"] p {
        font-size: 0.9rem !important;
    }
    @media (max-width: 900px) {
        [data-testid="stMetricLabel"] {
            font-size: 0.7rem !important;
        }
        [data-testid="stMetricValue"] {
            font-size: 0.95rem !important;
        }
        [data-testid="stMetricDelta"] {
            font-size: 0.65rem !important;
        }
        div[data-testid="stAlert"] p {
            font-size: 0.78rem !important;
        }
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Carga de datos
# ---------------------------------------------------------------------------

@st.cache_data
def load_data(_mtime: float = 0):
    movs = pd.read_csv(DATA_DIR / "movimientos_consolidados.csv", parse_dates=["fecha_oper"])
    movs["cargo"]  = pd.to_numeric(movs["cargo"],  errors="coerce")
    movs["abono"]  = pd.to_numeric(movs["abono"],  errors="coerce")
    movs["monto"] = movs["cargo"].fillna(0) + movs["abono"].fillna(0)
    movs["saldo_acumulado"] = pd.to_numeric(movs["saldo_acumulado"], errors="coerce")
    movs["categoria"] = movs["categoria"].fillna("No identificado").replace("", "No identificado")
    movs["categoria"] = movs["categoria"].replace("Indefinido", "No identificado")
    return movs


@st.cache_data
def load_msi(periodo_key: str):
    # Busca msi_activos_{YYYY-MM}.csv; si no existe, usa el más reciente disponible
    path = DATA_DIR / f"msi_activos_{periodo_key}.csv"
    if not path.exists():
        candidates = sorted(DATA_DIR.glob("msi_activos_*.csv"), reverse=True)
        path = candidates[0] if candidates else DATA_DIR / "msi_activos.csv"
    if not path.exists():
        return pd.DataFrame(columns=["descripcion","fecha_compra","monto_original",
                                     "saldo_pendiente","pago_requerido","pago_num",
                                     "total_pagos","tasa","progreso"])
    df = pd.read_csv(path)
    df["saldo_pendiente"] = pd.to_numeric(df["saldo_pendiente"], errors="coerce")
    df["pago_requerido"]  = pd.to_numeric(df["pago_requerido"],  errors="coerce")
    df["monto_original"]  = pd.to_numeric(df["monto_original"],  errors="coerce")
    df["progreso"] = df["pago_num"].astype(str) + " / " + df["total_pagos"].astype(str)
    return df


@st.cache_data
def load_metricas(periodo_key: str):
    path = DATA_DIR / f"metricas_credito_{periodo_key}.json"
    if not path.exists():
        candidates = sorted(DATA_DIR.glob("metricas_credito_*.json"), reverse=True)
        path = candidates[0] if candidates else DATA_DIR / "metricas_credito.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


_csv_mtime = (DATA_DIR / "movimientos_consolidados.csv").stat().st_mtime
df_all = load_data(_csv_mtime)

# ---------------------------------------------------------------------------
# Selector de periodo  (necesario antes de cargar MSI/métricas)
# ---------------------------------------------------------------------------

def _multiselect_all(label: str, options: list, key: str, container=None) -> list:
    """Multiselect con botón Todos / Limpiar integrado."""
    ctx = container or st
    if key not in st.session_state:
        st.session_state[key] = list(options)
    all_sel = sorted(st.session_state.get(key, [])) == sorted(options)
    col_ms, col_btn = ctx.columns([5, 1])
    with col_btn:
        st.write("")
        if st.button("✕" if all_sel else "✓ Todo", key=f"_btn_{key}", use_container_width=True):
            st.session_state[key] = [] if all_sel else list(options)
    with col_ms:
        return st.multiselect(label, options, key=key)


todos_periodos = sorted(df_all["periodo"].dropna().unique().tolist(), reverse=True)
periodos_deb   = [p for p in todos_periodos if "débito"  in p.lower()]
periodos_cred  = [p for p in todos_periodos if "crédito" in p.lower()]

sc1, sc2 = st.columns(2)
sel_deb  = _multiselect_all("Periodos Débito",  periodos_deb,  "sel_per_deb",  sc1)
sel_cred = _multiselect_all("Periodos Crédito", periodos_cred, "sel_per_cred", sc2)

periodos_sel = sel_deb + sel_cred
df = df_all[df_all["periodo"].isin(periodos_sel)].copy() if periodos_sel else df_all.copy()

# Clave YYYY-MM del periodo de crédito más reciente seleccionado (para MSI/métricas)
if sel_cred:
    ref_df = df_all[
        (df_all["periodo"].isin(sel_cred)) &
        (df_all["producto"] == "crédito")
    ]
    max_fecha = ref_df["fecha_oper"].max()
    periodo_key = f"{max_fecha.year}-{max_fecha.month:02d}"
elif not df.empty:
    max_fecha   = df["fecha_oper"].max()
    periodo_key = f"{max_fecha.year}-{max_fecha.month:02d}"
else:
    periodo_key = ""

msi   = load_msi(periodo_key)
metro = load_metricas(periodo_key)

def _rango(periodos: list[str]) -> str:
    sub = df_all[df_all["periodo"].isin(periodos)]
    if sub.empty:
        return ""
    lo = sub["fecha_oper"].min().strftime("%d/%b/%Y")
    hi = sub["fecha_oper"].max().strftime("%d/%b/%Y")
    return f"{lo} – {hi}"

# ---------------------------------------------------------------------------
# Métricas clave
# ---------------------------------------------------------------------------

debito_df_hdr   = df[df["producto"] == "débito"]
egresos_deb_hdr = debito_df_hdr[debito_df_hdr["tipo"] == "egreso"]
ingresos_deb_hdr= debito_df_hdr[debito_df_hdr["tipo"] == "ingreso"]
total_eg_hdr    = egresos_deb_hdr["cargo"].sum()
total_ing_hdr   = ingresos_deb_hdr["abono"].sum()
balance_neto_hdr= total_ing_hdr - total_eg_hdr
saldo_final_hdr = debito_df_hdr["saldo_acumulado"].dropna().iloc[-1] if not debito_df_hdr.empty else 0
saldo_inicial_hdr = saldo_final_hdr - balance_neto_hdr

saldo_debito    = saldo_final_hdr
deuda_tdc       = metro.get("pago_sin_intereses", 0) or 0
deuda_msi       = msi["saldo_pendiente"].sum()
deuda_total     = metro.get("saldo_deudor_total", deuda_tdc + deuda_msi) or (deuda_tdc + deuda_msi)
limite_credito  = metro.get("limite_credito", 0) or 0
uso_credito_pct = round((deuda_tdc / limite_credito * 100) if limite_credito else 0, 1)
uso_msi_pct     = round((deuda_msi / limite_credito * 100) if limite_credito else 0, 1)
uso_total_pct   = round((deuda_total / limite_credito * 100) if limite_credito else 0, 1)

credito_df_hdr = df[df["producto"] == "crédito"]
compras_cred   = credito_df_hdr[credito_df_hdr["tipo"] == "egreso"]["cargo"].sum()
pagos_cred     = credito_df_hdr[credito_df_hdr["tipo"] == "ingreso"]["abono"].sum()
adeudo_ant     = metro.get("adeudo_anterior", 0) or 0

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.title("💰 MoneyFlow")
st.caption("Dashboard de salud financiera personal")
st.divider()

r1 = st.columns(9)
r1[0].metric("Saldo Inicial",    f"${saldo_inicial_hdr:,.2f}")
r1[1].metric("Total Egresos",    f"${total_eg_hdr:,.2f}",     "salida",  delta_color="inverse")
r1[2].metric("Total Ingresos",   f"${total_ing_hdr:,.2f}",    "entrada", delta_color="normal")
r1[3].metric("Balance Neto",     f"${balance_neto_hdr:,.2f}", delta_color="normal")
r1[4].metric("💵 Saldo Disponible", f"${saldo_debito:,.2f}")
r1[5].metric("💳 Deuda TDC Regular", f"${deuda_tdc:,.2f}",   f"{uso_credito_pct}% límite", delta_color="inverse")
r1[6].metric("📅 Deuda MSI",     f"${deuda_msi:,.2f}",        f"{uso_msi_pct}% límite · {len(msi)} planes", delta_color="inverse")
r1[7].metric("🏦 Deuda Total",   f"${deuda_total:,.2f}",      delta_color="inverse")
r1[8].metric("📊 Uso TDC",       f"{uso_total_pct}%",         f"${limite_credito - deuda_total:,.2f} disp.", delta_color="inverse")

st.divider()

# ---------------------------------------------------------------------------
# Filtros globales
# ---------------------------------------------------------------------------

with st.expander("🔍 Filtros", expanded=False):
    fc1, fc2, fc3 = st.columns(3)
    todas_cats = sorted(df["categoria"].unique())
    sel_producto = _multiselect_all("Producto",   ["débito", "crédito"], "sel_producto", fc1)
    sel_tipo     = _multiselect_all("Tipo",        ["egreso", "ingreso"], "sel_tipo",     fc2)
    sel_cats     = _multiselect_all("Categorías",  todas_cats,            "sel_cats",     fc3)

mask = (
    df["producto"].isin(sel_producto) &
    df["tipo"].isin(sel_tipo) &
    df["categoria"].isin(sel_cats)
)
filtered = df[mask].copy()

# Mismo filtro aplicado a todos los periodos (para el gráfico histórico)
mask_all = (
    df_all["producto"].isin(sel_producto) &
    df_all["tipo"].isin(sel_tipo) &
    df_all["categoria"].isin(sel_cats)
)
filtered_all = df_all[mask_all].copy()

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab_res, tab_mov, tab_debito, tab_credito, tab_msi, tab_track = st.tabs([
    "📊 Resumen",
    "📋 Movimientos",
    "🏦 Débito",
    "💳 Crédito",
    "📅 MSI",
    "🎯 Tracker",
])

# ---- RESUMEN ---------------------------------------------------------------
with tab_res:
    egresos = filtered[filtered["tipo"] == "egreso"]

    def _ing_egr_chart(periodos: list[str], producto: str, titulo: str):
        data = df_all[df_all["periodo"].isin(periodos) & (df_all["producto"] == producto)]
        periodos_ord = sorted(data["periodo"].unique())
        ing = data[data["tipo"] == "ingreso"].groupby("periodo")["abono"].sum().rename("Ingresos")
        egr = data[data["tipo"] == "egreso"].groupby("periodo")["cargo"].sum().rename("Egresos")
        prd = pd.concat([ing, egr], axis=1).fillna(0).reindex(periodos_ord).reset_index()
        prd = prd.melt(id_vars="periodo", var_name="Tipo", value_name="Monto")
        fig = px.bar(
            prd, x="periodo", y="Monto", color="Tipo", barmode="group",
            color_discrete_map={"Ingresos": "#4CAF50", "Egresos": "#F44336"},
            labels={"periodo": "", "Monto": "Monto ($)"},
            text_auto=".2s",
        )
        fig.update_layout(height=300, margin=dict(t=10, b=10), legend_title="",
                          xaxis_tickangle=-15)
        rango = _rango(periodos)
        st.subheader(titulo)
        if rango:
            st.caption(rango)
        st.plotly_chart(fig, use_container_width=True, key=f"res_{titulo}")

    # Resumen TDC — siempre el periodo de crédito más reciente disponible
    candidates = sorted(DATA_DIR.glob("metricas_credito_*.json"), reverse=True)
    metro_latest = load_metricas(candidates[0].stem.rsplit("_", 1)[-1]) if candidates else {}

    pago_min     = metro_latest.get("pago_minimo", 0) or 0
    pago_sin_int = metro_latest.get("pago_sin_intereses", 0) or 0
    saldo_reg    = metro_latest.get("saldo_cargos_regulares", 0) or 0
    saldo_msi_m  = metro_latest.get("saldo_msi", 0) or 0
    cred_disp    = metro_latest.get("credito_disponible", 0) or 0
    adeudo_ant_r = metro_latest.get("adeudo_anterior", 0) or 0
    deuda_total_latest = metro_latest.get("saldo_deudor_total", 0) or 0
    limite_credito_latest = metro_latest.get("limite_credito", 0) or 0

    # Compras y pagos del periodo de crédito seleccionado (para desglose)
    cred_periodo = df[df["producto"] == "crédito"]
    compras_periodo = cred_periodo[cred_periodo["tipo"] == "egreso"]["cargo"].sum()
    pagos_periodo   = cred_periodo[cred_periodo["tipo"] == "ingreso"]["abono"].sum()

    col_izq, col_der = st.columns(2)

    with col_izq:
        st.subheader("Cómo se calcula la Deuda TDC Regular")
        desglose_data = {
            "Concepto": [
                "Adeudo periodo anterior",
                "＋ Compras del periodo",
                "－ Pagos recibidos",
                "= Pago para no generar intereses",
            ],
            "Monto": [adeudo_ant_r, compras_periodo, pagos_periodo, pago_sin_int],
        }
        desglose_df = pd.DataFrame(desglose_data)
        desglose_df["Monto"] = desglose_df["Monto"].apply(lambda x: f"${x:,.2f}" if x else "—")
        st.dataframe(desglose_df, use_container_width=True, hide_index=True)

    with col_der:
        st.subheader("Resumen Estado de Cuenta TDC (periodo más reciente)")
        resumen_data = {
            "Concepto": [
                "Adeudo periodo anterior",
                "Saldo cargos regulares",
                "Saldo cargo a meses (MSI)",
                "Saldo deudor total",
                "Pago para no generar intereses",
                "Pago mínimo",
                "Límite de crédito",
                "Crédito disponible",
            ],
            "Monto": [
                adeudo_ant_r, saldo_reg, saldo_msi_m, deuda_total_latest,
                pago_sin_int, pago_min, limite_credito_latest, cred_disp,
            ],
        }
        resumen_df = pd.DataFrame(resumen_data)
        resumen_df["Monto"] = resumen_df["Monto"].apply(lambda x: f"${x:,.2f}" if x else "—")
        st.dataframe(resumen_df, use_container_width=True, hide_index=True)

    col_graf_izq, col_graf_der = st.columns(2)
    with col_graf_izq:
        _ing_egr_chart(sel_deb,  "débito",  "Débito — Ingresos vs Egresos")
    with col_graf_der:
        _ing_egr_chart(sel_cred, "crédito", "Crédito — Compras vs Pagos")

# ---- MOVIMIENTOS -----------------------------------------------------------
with tab_mov:
    st.subheader(f"Movimientos ({len(filtered)} total)")

    display = filtered[[
        "fecha_oper", "periodo", "producto", "tipo", "descripcion", "categoria", "cargo", "abono", "saldo_acumulado"
    ]].copy()
    display["fecha_oper"] = display["fecha_oper"].dt.strftime("%Y-%m-%d")
    display.columns = ["Fecha", "Periodo", "Producto", "Tipo", "Descripción", "Categoría", "Cargo", "Abono", "Saldo"]
    display["Saldo"] = pd.to_numeric(display["Saldo"], errors="coerce")

    # Estado de "seleccionar todo"
    if "sel_all" not in st.session_state:
        st.session_state.sel_all = False

    if st.button("Seleccionar todo" if not st.session_state.sel_all else "Deseleccionar todo"):
        st.session_state.sel_all = not st.session_state.sel_all

    display.insert(0, "Sel", st.session_state.sel_all)

    edited = st.data_editor(
        display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Sel":   st.column_config.CheckboxColumn("Sel", help="Selecciona para sumar", width="small"),
            "Cargo": st.column_config.NumberColumn("Cargo", format="$%.2f"),
            "Abono": st.column_config.NumberColumn("Abono", format="$%.2f"),
            "Saldo": st.column_config.NumberColumn("Saldo", format="$%.2f"),
        },
        disabled=["Fecha", "Periodo", "Producto", "Tipo", "Descripción", "Categoría", "Cargo", "Abono", "Saldo"],
    )

    # Tarjeta de totales según selección
    sel_rows = edited[edited["Sel"] == True]
    if not sel_rows.empty:
        total_cargo_sel = sel_rows["Cargo"].fillna(0).sum()
        total_abono_sel = sel_rows["Abono"].fillna(0).sum()
        neto_sel        = total_abono_sel - total_cargo_sel
        st.info(
            f"**{len(sel_rows)} movimiento(s) seleccionado(s)** — "
            f"Cargos: **${total_cargo_sel:,.2f}** | "
            f"Abonos: **${total_abono_sel:,.2f}** | "
            f"Neto: **${neto_sel:,.2f}**"
        )

# ---- DÉBITO ----------------------------------------------------------------
with tab_debito:
    debito_df   = df[df["producto"] == "débito"].copy()
    egresos_deb = debito_df[debito_df["tipo"] == "egreso"]

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Evolución del Saldo")
        fig_saldo = px.area(
            debito_df.sort_values("fecha_oper"),
            x="fecha_oper", y="saldo_acumulado",
            color_discrete_sequence=["#2196F3"],
            labels={"fecha_oper": "Fecha", "saldo_acumulado": "Saldo ($)"},
        )
        fig_saldo.update_layout(height=320, margin=dict(t=10))
        st.plotly_chart(fig_saldo, use_container_width=True)

    with col2:
        st.subheader("Gastos por Categoría")
        cat_deb = (
            egresos_deb.groupby("categoria")["cargo"]
            .sum().reset_index().sort_values("cargo", ascending=False)
        )
        fig_cat = px.bar(
            cat_deb, x="categoria", y="cargo",
            color_discrete_sequence=["#2196F3"],
            labels={"cargo": "Monto ($)", "categoria": ""},
        )
        fig_cat.update_layout(height=320, margin=dict(t=10), xaxis_tickangle=-45)
        st.plotly_chart(fig_cat, use_container_width=True)

# ---- CRÉDITO ---------------------------------------------------------------
with tab_credito:
    credito_df   = df[df["producto"] == "crédito"].copy()
    egresos_cred = credito_df[credito_df["tipo"] == "egreso"]

    total_compras = egresos_cred["cargo"].sum()
    total_pagos   = credito_df[credito_df["tipo"] == "ingreso"]["abono"].sum()

    m1, m2, m3 = st.columns(3)
    m1.metric("Total Compras",        f"${total_compras:,.2f}", "salida",  delta_color="inverse")
    m2.metric("Total Pagos Recibidos",f"${total_pagos:,.2f}",   "entrada", delta_color="normal")
    m3.metric("Límite de Crédito",    f"${limite_credito:,.2f}")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Gasto diario TDC")
        daily = (
            egresos_cred.groupby("fecha_oper")["cargo"]
            .sum().reset_index()
            .sort_values("fecha_oper")
        )
        fig_daily = px.line(
            daily, x="fecha_oper", y="cargo",
            markers=True,
            color_discrete_sequence=["#FF5722"],
            labels={"fecha_oper": "Fecha", "cargo": "Monto ($)"},
        )
        fig_daily.update_layout(height=360, margin=dict(t=10))
        st.plotly_chart(fig_daily, use_container_width=True)

    with col2:
        st.subheader("Gastos por Categoría")
        cat_cred = (
            egresos_cred.groupby("categoria")["cargo"]
            .sum().reset_index().sort_values("cargo", ascending=False)
        )
        fig_cred = px.bar(
            cat_cred, x="categoria", y="cargo",
            color_discrete_sequence=["#FF5722"],
            labels={"cargo": "Monto ($)", "categoria": ""},
        )
        fig_cred.update_layout(height=360, margin=dict(t=10), xaxis_tickangle=-45)
        st.plotly_chart(fig_cred, use_container_width=True)

    st.subheader("Compras vs Pagos del Periodo")
    credit_periods_df = credito_df.copy()
    period_order = (
        credit_periods_df.groupby("periodo")["fecha_oper"]
        .max()
        .sort_values()
        .index
        .tolist()
    )
    compras_periodo_df = (
        credit_periods_df[credit_periods_df["tipo"] == "egreso"]
        .groupby("periodo", as_index=False)["cargo"]
        .sum()
        .rename(columns={"cargo": "Monto"})
    )
    compras_periodo_df["Concepto"] = "Compras"
    pagos_periodo_df = (
        credit_periods_df[credit_periods_df["tipo"] == "ingreso"]
        .groupby("periodo", as_index=False)["abono"]
        .sum()
        .rename(columns={"abono": "Monto"})
    )
    pagos_periodo_df["Concepto"] = "Pagos"
    comp_pago_df = pd.concat([compras_periodo_df, pagos_periodo_df], ignore_index=True)
    comp_pago_df["periodo"] = pd.Categorical(comp_pago_df["periodo"], categories=period_order, ordered=True)
    comp_pago_df = comp_pago_df.sort_values(["periodo", "Concepto"])
    fig_cp = px.bar(
        comp_pago_df, x="periodo", y="Monto", barmode="group",
        color="Concepto",
        color_discrete_map={"Compras": "#F44336", "Pagos": "#4CAF50"},
        labels={"Monto": "Monto ($)", "periodo": ""},
        text_auto=".2s",
    )
    fig_cp.update_layout(height=320, margin=dict(t=10, b=10), legend_title="", xaxis_tickangle=-15)
    st.plotly_chart(fig_cp, use_container_width=True)

    st.subheader("Compras Month over Month")
    compras_mom = (
        compras_periodo_df.copy()
        .assign(periodo=pd.Categorical(compras_periodo_df["periodo"], categories=period_order, ordered=True))
        .sort_values("periodo")
    )
    compras_mom["MontoAnterior"] = compras_mom["Monto"].shift(1)
    compras_mom["VariacionPct"] = ((compras_mom["Monto"] - compras_mom["MontoAnterior"]) / compras_mom["MontoAnterior"] * 100)
    compras_mom = compras_mom.dropna(subset=["VariacionPct"]).copy()

    if compras_mom.empty:
        st.info("Se necesitan al menos dos periodos de crédito para calcular el month over month de compras.")
    else:
        compras_mom["Color"] = compras_mom["VariacionPct"].apply(lambda x: "#F44336" if x > 0 else "#4CAF50")
        fig_mom = go.Figure(go.Bar(
            x=compras_mom["periodo"].astype(str),
            y=compras_mom["VariacionPct"],
            marker_color=compras_mom["Color"],
            text=compras_mom["VariacionPct"].apply(lambda x: f"{x:+.1f}%"),
            textposition="outside",
            customdata=compras_mom[["Monto", "MontoAnterior"]],
            hovertemplate=(
                "Periodo: %{x}<br>"
                "Variación: %{y:+.1f}%<br>"
                "Compras actuales: $%{customdata[0]:,.2f}<br>"
                "Compras previas: $%{customdata[1]:,.2f}<extra></extra>"
            ),
        ))
        fig_mom.update_layout(
            height=320,
            margin=dict(t=10, b=10),
            xaxis_title="Periodo",
            yaxis_title="Variación (%)",
            showlegend=False,
        )
        st.plotly_chart(fig_mom, use_container_width=True)

# ---- MSI -------------------------------------------------------------------
with tab_msi:
    st.subheader("Deudas a Meses Sin Intereses")

    monto_original_total = msi["monto_original"].sum()
    saldo_pendiente_total = msi["saldo_pendiente"].sum()
    total_pagado_msi = (monto_original_total - saldo_pendiente_total)

    t1, t2, t3, t4 = st.columns(4)
    t1.metric("Monto Original Total",  f"${monto_original_total:,.2f}")
    t2.metric("Saldo Pendiente Total",  f"${saldo_pendiente_total:,.2f}")
    t3.metric("Total Pagado",          f"${total_pagado_msi:,.2f}")
    t4.metric("Pago Este Mes Total",   f"${msi['pago_requerido'].sum():,.2f}")

    msi_display = msi[[
        "descripcion", "fecha_compra", "monto_original",
        "saldo_pendiente", "pago_requerido", "progreso"
    ]].copy()
    cumplimiento_total = (
        ((msi["monto_original"] - msi["saldo_pendiente"]) / msi["monto_original"]) * 100
    ).replace([float("inf"), -float("inf")], pd.NA).fillna(0).round(1)
    msi_display["cumplimiento_total"] = cumplimiento_total
    msi_display.columns = [
        "Descripción", "Fecha Compra", "Monto Original",
        "Saldo Pendiente", "Pago Este Mes", "Progreso", "Cumplimiento Total"
    ]
    msi_display["Monto Original"]  = msi_display["Monto Original"].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "")
    msi_display["Saldo Pendiente"] = msi_display["Saldo Pendiente"].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "")
    msi_display["Pago Este Mes"]   = msi_display["Pago Este Mes"].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "")
    msi_display["Cumplimiento Total"] = msi_display["Cumplimiento Total"].apply(lambda x: f"{x:.1f}%")

    selected = st.dataframe(
        msi_display,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="multi-row",
    )

    sel_rows = selected.selection.rows if selected.selection.rows else list(range(len(msi)))
    msi_sel = msi.iloc[sel_rows].copy()
    msi_sel = msi_sel.sort_values("monto_original", ascending=False)

    pagado = (msi_sel["monto_original"] - msi_sel["saldo_pendiente"]).clip(lower=0)
    pct_pagado = ((pagado / msi_sel["monto_original"]) * 100).replace(
        [float("inf"), -float("inf")], pd.NA
    ).fillna(0).round(1)
    pct_text = pct_pagado.apply(lambda p: f"{p:.0f}%")

    fig_msi = go.Figure()
    fig_msi.add_trace(go.Bar(
        name="Pagado",
        x=msi_sel["descripcion"],
        y=pagado,
        marker_color="#4CAF50",
        text=pct_text,
        textposition="outside",
    ))
    fig_msi.add_trace(go.Bar(
        name="Saldo pendiente",
        x=msi_sel["descripcion"],
        y=msi_sel["saldo_pendiente"],
        marker_color="#FF9800",
    ))
    fig_msi.update_layout(
        barmode="stack",
        height=max(360, len(msi_sel) * 24 + 180),
        margin=dict(t=10, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        xaxis_title="Descripción",
        yaxis_title="Monto ($)",
    )
    fig_msi.update_xaxes(tickangle=-90)
    st.plotly_chart(fig_msi, use_container_width=True)

# ---- TRACKER ---------------------------------------------------------------
TRACK_PATH = DATA_DIR / "track_ciclo.json"

def _load_track() -> dict:
    if TRACK_PATH.exists():
        with open(TRACK_PATH, encoding="utf-8") as f:
            state = json.load(f)
    else:
        state = {"presupuesto": 13168.0, "gastos": [], "ciclo_inicio": None}

    try:
        state, changed, _ = backfill_tracker_categories(state)
    except RuntimeError:
        return state

    if changed:
        with open(TRACK_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
    return state

with tab_track:
    track = _load_track()
    presupuesto  = track.get("presupuesto", 13168.0)
    gastos       = track.get("gastos", [])
    ciclo_inicio = track.get("ciclo_inicio", "—")
    total_gasto  = sum(g["monto"] for g in gastos)
    restante     = presupuesto - total_gasto
    pct_usado    = (total_gasto / presupuesto * 100) if presupuesto else 0

    # Header del tracker
    tr1, tr2, tr3 = st.columns(3)
    tr1.metric("Presupuesto del ciclo", f"${presupuesto:,.2f}")
    tr2.metric("Gastado (tracker)", f"${total_gasto:,.2f}", f"{pct_usado:.1f}% usado", delta_color="inverse")
    tr3.metric("Disponible", f"${restante:,.2f}", delta_color="normal")

    if pct_usado >= 100:
        st.error("🚨 Presupuesto excedido")
    elif pct_usado >= 80:
        st.warning("⚠️ Más del 80% del presupuesto usado — considera frenar.")

    # Barra de progreso
    st.progress(min(pct_usado / 100, 1.0))

    if ciclo_inicio and ciclo_inicio != "—":
        st.caption(f"Ciclo iniciado: {ciclo_inicio[:10]}")

    st.divider()

    if not gastos:
        st.info("No hay gastos registrados en este ciclo. Envía `/gasto 350 Uber` al bot de Telegram.")
    else:
        gdf_raw = pd.DataFrame(gastos)
        gdf_raw["categoria"] = gdf_raw.get("categoria", "").fillna("").replace("", "No identificado")
        gdf_raw["categoria"] = gdf_raw["categoria"].replace("Indefinido", "No identificado")
        gdf_raw["fecha_dt"] = pd.to_datetime(gdf_raw["fecha"])

        gdf = gdf_raw[["fecha_dt", "monto", "descripcion", "categoria"]].sort_values("fecha_dt", ascending=False).copy()
        gdf["fecha"] = gdf["fecha_dt"].dt.strftime("%Y-%m-%d %H:%M")
        gdf = gdf[["fecha", "monto", "descripcion", "categoria"]]
        gdf.columns = ["Fecha", "Monto", "Descripción", "Categoría"]

        col_tbl, col_chart = st.columns([3, 2])

        with col_tbl:
            st.subheader(f"Gastos del ciclo ({len(gastos)})")

            if "track_sel_all" not in st.session_state:
                st.session_state.track_sel_all = False

            if st.button(
                "Seleccionar todo" if not st.session_state.track_sel_all else "Deseleccionar todo",
                key="track_select_toggle",
            ):
                st.session_state.track_sel_all = not st.session_state.track_sel_all

            tracker_display = gdf.copy()
            tracker_display.insert(0, "Sel", st.session_state.track_sel_all)

            edited_tracker = st.data_editor(
                tracker_display,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Sel": st.column_config.CheckboxColumn("Sel", help="Selecciona para sumar", width="small"),
                    "Monto": st.column_config.NumberColumn("Monto", format="$%.2f"),
                },
                disabled=["Fecha", "Monto", "Descripción", "Categoría"],
            )

            sel_tracker = edited_tracker[edited_tracker["Sel"] == True]
            if not sel_tracker.empty:
                total_sel = sel_tracker["Monto"].fillna(0).sum()
                st.info(
                    f"**{len(sel_tracker)} gasto(s) seleccionado(s)** — "
                    f"Total: **${total_sel:,.2f}**"
                )

        with col_chart:
            st.subheader("Por categoría")
            top = gdf_raw.groupby("categoria")["monto"].sum().sort_values(ascending=True)
            fig_track = px.bar(
                top.reset_index(),
                x="monto", y="categoria",
                orientation="h",
                labels={"monto": "Total ($)", "categoria": ""},
                text_auto=".2s",
            )
            fig_track.update_layout(height=300, margin=dict(t=10, b=10))
            st.plotly_chart(fig_track, use_container_width=True)

        st.subheader("Gasto por fecha")
        gasto_por_fecha = (
            gdf_raw.assign(fecha=gdf_raw["fecha_dt"].dt.date)
            .groupby("fecha", as_index=False)["monto"]
            .sum()
            .sort_values("fecha")
        )
        gasto_por_fecha["fecha_label"] = gasto_por_fecha["fecha"].astype(str)
        fig_track_line = px.line(
            gasto_por_fecha,
            x="fecha_label",
            y="monto",
            markers=True,
            labels={"fecha_label": "Fecha", "monto": "Monto total ($)"},
        )
        fig_track_line.update_xaxes(type="category")
        fig_track_line.update_layout(height=320, margin=dict(t=10, b=10))
        st.plotly_chart(fig_track_line, use_container_width=True)

    st.divider()
    st.caption("Comandos del bot de Telegram: `/gasto 350 Uber` · `/status` · `/update_presupuesto 14000` · `/reset`")
