import streamlit as st
import pandas as pd
import os
import plotly.express as px
import plotly.graph_objects as go

# 👉 ML IMPORT
from ml_model import predict_future

# ==============================
# 🎨 PAGE CONFIG
# ==============================
st.set_page_config(
    page_title="AI-Powered E-Commerce Analytics",
    layout="wide"
)

# ==============================
# 🎨 PREMIUM CSS (UNCHANGED)
# ==============================
st.markdown("""
<style>
.main {
    background-color: #0e1117;
}

.card1 {
    background-color: #1c1f26;
    padding: 25px;
    border-radius: 18px;
    box-shadow: 0px 4px 20px rgba(0,0,0,0.6);
    text-align: center;
    color:white;
}
.card {
    background: rgba(255, 255, 255, 0.05);
    padding: 25px;
    border-radius: 18px;
    backdrop-filter: blur(10px);
    box-shadow: 0px 8px 25px rgba(0,0,0,0.5);
    text-align: center;
    transition: 0.3s;
}           
.card:hover, .card1:hover {
    transform: scale(1.05);
}

.section {
    background:#1c1f26 ;
    padding: 5px;
    border-radius: 18px;
    box-shadow: 0px 5px 20px rgba(0,0,0,0.4);
    margin-bottom: 20px;
}
</style>
""", unsafe_allow_html=True)

# ==============================
# 📁 DATA PATH
# ==============================
BASE_PATH = "C:/delta/gold_csv/"

def load_csv(folder):
    path = os.path.join(BASE_PATH, folder)
    if not os.path.exists(path):
        return pd.DataFrame()

    files = [f for f in os.listdir(path) if f.endswith(".csv")]
    if not files:
        return pd.DataFrame()

    return pd.read_csv(os.path.join(path, files[0]), engine="python")

# ==============================
# 🔄 REFRESH BUTTON
# ==============================
if st.button("🔄 Refresh Dashboard"):
    st.rerun()

# ==============================
# 📥 LOAD DATA
# ==============================
kpi = load_csv("kpi")
product_sales = load_csv("product_sales")
revenue_trend = load_csv("revenue_trend")
orders_per_minute = load_csv("orders_per_minute")

# ==============================
# 🔍 SIDEBAR FILTERS
# ==============================
st.sidebar.title("🎯 Smart Filters")

selected_products = []
if not product_sales.empty:
    selected_products = st.sidebar.multiselect(
        "📦 Select Products",
        options=list(product_sales["product"].unique()),
        default=list(product_sales["product"].unique())
    )

top_n = st.sidebar.slider(
    "🏆 Show Top N Products",
    min_value=3,
    max_value=20,
    value=5
)

sort_option = st.sidebar.selectbox(
    "📊 Sort By",
    ["Revenue", "Orders"]
)

# ==============================
# 🎯 APPLY FILTERS
# ==============================
filtered_data = product_sales.copy()

if not filtered_data.empty:
    if selected_products:
        filtered_data = filtered_data[
            filtered_data["product"].isin(selected_products)
        ]

    if sort_option == "Revenue":
        filtered_data = filtered_data.sort_values(
            by="total_sales", ascending=False
        )
    else:
        filtered_data = filtered_data.sort_values(
            by="total_orders", ascending=False
        )

    filtered_data = filtered_data.head(top_n)

st.sidebar.metric("Filtered Products", len(filtered_data))

# ==============================
# 🏷️ TITLE
# ==============================
st.title("🚀 AI-Powered E-Commerce Analytics Dashboard")
st.caption("Real-time Insights | Product Intelligence | Sales Trends")

# ==============================
# 🔥 KPI CARDS
# ==============================
if not kpi.empty:
    col1, col2, col3 = st.columns(3)

    col1.markdown(f"""
    <div class="card1">
        <h4>Total Revenue</h4>
        <h2>₹ {round(kpi['total_revenue'][0],2)}</h2>
    </div>
    """, unsafe_allow_html=True)

    col2.markdown(f"""
    <div class="card">
        <h4>Total Orders</h4>
        <h2>{int(kpi['total_orders'][0])}</h2>
    </div>
    """, unsafe_allow_html=True)

    col3.markdown(f"""
    <div class="card">
        <h4>Avg Order Value</h4>
        <h2>₹ {round(kpi['avg_order_value'][0],2)}</h2>
    </div>
    """, unsafe_allow_html=True)

# ==============================
# 🏆 PRODUCT ANALYTICS
# ==============================
st.markdown("## 🏆 Product Performance")

col1, col2 = st.columns(2)

with col1:
    st.markdown('<div class="section">', unsafe_allow_html=True)
    if not filtered_data.empty:
        fig = px.bar(
            filtered_data,
            x="product",
            y="total_sales",
            title="Top Products by Revenue"
        )
        st.plotly_chart(fig, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

with col2:
    st.markdown('<div class="section">', unsafe_allow_html=True)
    if not filtered_data.empty:
        fig = px.pie(
            filtered_data,
            names="product",
            values="total_sales",
            hole=0.5,
            title="Revenue Contribution"
        )
        st.plotly_chart(fig, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

# ==============================
# 📈 TREND ANALYSIS
# ==============================
st.markdown("## 📈 Sales Trends")

if not revenue_trend.empty:
    revenue_trend["moving_avg"] = revenue_trend["revenue"].rolling(3).mean()

col1, col2 = st.columns(2)

with col1:
    st.markdown('<div class="section">', unsafe_allow_html=True)
    if not revenue_trend.empty:
        fig = px.line(
            revenue_trend,
            y=["revenue", "moving_avg"],
            title="Revenue Trend (with Moving Avg)"
        )
        st.plotly_chart(fig, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

with col2:
    st.markdown('<div class="section">', unsafe_allow_html=True)
    if not orders_per_minute.empty:
        fig = px.line(
            orders_per_minute,
            y="order_count",
            title="Orders Per Minute"
        )
        st.plotly_chart(fig, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

# ==============================
# 🔮 SALES PREDICTION (NEW)
# ==============================
st.markdown("## 🔮 Sales Prediction (AI)")

if not revenue_trend.empty:

    col1, col2 = st.columns([1,2])

    with col1:
        future_steps = st.slider(
            "Select Future Steps",
            min_value=5,
            max_value=50,
            value=10
        )

        if st.button("Train & Predict"):
            future_df = predict_future(revenue_trend, future_steps)
            st.session_state["future"] = future_df

    with col2:
        if "future" in st.session_state:
            future_df = st.session_state["future"]

            fig = go.Figure()

            fig.add_trace(go.Scatter(
                y=revenue_trend["revenue"],
                mode='lines',
                name='Actual Revenue'
            ))

            fig.add_trace(go.Scatter(
                x=list(range(len(revenue_trend), len(revenue_trend) + len(future_df))),
                y=future_df["predicted_revenue"],
                mode='lines',
                name='Predicted Revenue',
                line=dict(dash='dash')
            ))

            fig.update_layout(title="Future Revenue Prediction")

            st.plotly_chart(fig, use_container_width=True)

# ==============================
# 🧠 AI INSIGHTS
# ==============================
st.markdown("## 🧠 Smart Insights")

if not filtered_data.empty:
    best = filtered_data.iloc[0]
    worst = filtered_data.iloc[-1]

    st.success(f"🏆 Best Product: {best['product']} (₹ {round(best['total_sales'],2)})")
    st.error(f"⚠️ Lowest Product: {worst['product']} (₹ {round(worst['total_sales'],2)})")

if not revenue_trend.empty:
    growth = revenue_trend["revenue"].pct_change().mean() * 100
    st.info(f"📊 Avg Revenue Growth: {round(growth,2)}%")

# ==============================
# 📋 RAW DATA
# ==============================
with st.expander("📄 View Filtered Data"):
    st.dataframe(filtered_data)