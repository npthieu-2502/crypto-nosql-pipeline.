import streamlit as st
import pandas as pd
import redis
import os
from cassandra.cluster import Cluster
import time
import plotly.graph_objects as go
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

CASSANDRA_HOST = os.getenv('CASSANDRA_HOST', 'localhost')
CASSANDRA_PORT = int(os.getenv('CASSANDRA_PORT', 9042))
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

# --- Configuration ---
st.set_page_config(page_title="Market Data | Crypto", layout="wide", initial_sidebar_state="collapsed")

# Tối ưu hóa khoảng trống, thu nhỏ font chữ để ôm sát màn hình
st.markdown("""
    <style>
        #MainMenu {visibility: hidden;}
        header {visibility: hidden;}
        footer {visibility: hidden;}
        .block-container {
            padding-top: 1rem;
            padding-bottom: 0rem;
        }
        .metric-container {
            background-color: #1E1E1E;
            padding: 10px 15px;
            border-radius: 5px;
            border-left: 4px solid #2962FF;
        }
        .metric-label {
            color: #8A929A;
            font-size: 12px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        .metric-value {
            color: #FFFFFF;
            font-size: 26px;
            font-weight: bold;
            font-family: 'Courier New', Courier, monospace;
            margin-top: 2px;
        }
        hr {
            margin-top: 0.5rem;
            margin-bottom: 0.5rem;
        }
    </style>
""", unsafe_allow_html=True)

# --- Connections ---
@st.cache_resource
def get_cassandra_session():
    try:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        return cluster.connect('crypto_ks')
    except Exception:
        return None

@st.cache_resource
def get_redis_client():
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping()
        return r
    except:
        return None

cassandra_session = get_cassandra_session()
redis_client = get_redis_client()

# --- Controls ---
st.markdown("<h4 style='color: #E0E0E0; margin-bottom: 0px;'>Cryptocurrency Market Data Pipeline</h4>", unsafe_allow_html=True)

col_opt1, col_opt2, _ = st.columns([1.5, 1.5, 9])
with col_opt1:
    selected_symbol = st.selectbox("Trading Pair", ["BTCUSDT", "ETHUSDT"], label_visibility="collapsed")
with col_opt2:
    refresh_rate = st.slider("Update Interval (s)", 1, 5, 1, label_visibility="collapsed")

st.markdown("<hr style='border: 1px solid #333;'>", unsafe_allow_html=True)

placeholder = st.empty()

while True:
    with placeholder.container():
        # Đọc dữ liệu
        live_price = None
        if redis_client:
            live_price = redis_client.get(f"live_price:{selected_symbol}")
            
        # Layout: Hàng trên cùng hiển thị Metric Cards
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if live_price:
                st.markdown(f"""
                <div class="metric-container">
                    <div class="metric-label">{selected_symbol} Live Price</div>
                    <div class="metric-value">${float(live_price):,.2f}</div>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown("""
                <div class="metric-container" style="border-left: 4px solid #EF5350;">
                    <div class="metric-label">System Status</div>
                    <div class="metric-value" style="font-size: 16px;">Awaiting Stream...</div>
                </div>
                """, unsafe_allow_html=True)
                
        with col2:
            current_time = datetime.now().strftime("%H:%M:%S")
            st.markdown(f"""
            <div class="metric-container" style="border-left: 4px solid #FF9800;">
                <div class="metric-label">Last Updated</div>
                <div class="metric-value">{current_time}</div>
            </div>
            """, unsafe_allow_html=True)

        # Layout: Phần biểu đồ
        if cassandra_session:
            try:
                query = f"SELECT window_start, open_price, high_price, low_price, close_price FROM ohlc_1m WHERE symbol='{selected_symbol}' LIMIT 120;"
                rows = cassandra_session.execute(query)
                df = pd.DataFrame(list(rows))
                
                if not df.empty:
                    df = df.sort_values(by='window_start')
                    
                    fig = go.Figure(data=[go.Candlestick(
                        x=df['window_start'],
                        open=df['open_price'],
                        high=df['high_price'],
                        low=df['low_price'],
                        close=df['close_price'],
                        name=selected_symbol,
                        increasing_line_color='#26A69A',
                        decreasing_line_color='#EF5350'
                    )])
                    
                    fig.update_layout(
                        margin=dict(l=5, r=5, t=10, b=5),
                        height=420,  
                        xaxis_rangeslider_visible=False,
                        template="plotly_dark",
                        paper_bgcolor="#121212",
                        plot_bgcolor="#121212",
                        xaxis=dict(showgrid=True, gridcolor='#2B2B2B'),
                        yaxis=dict(showgrid=True, gridcolor='#2B2B2B', side='right')
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Accumulating historical data...")
            except Exception as e:
                st.error(f"Database Error: {e}")
                    
    time.sleep(refresh_rate)
