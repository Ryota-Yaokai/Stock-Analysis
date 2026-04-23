import streamlit as st
import wrds
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.gridspec as gridspec
from datetime import date, timedelta

# ==========================================
# 1. Page Configuration & Styling
# ==========================================
st.set_page_config(
    page_title="ProTrade Terminal | WRDS",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for Dark Mode
st.markdown("""
    <style>
    .main { background-color: #121212; color: #E0E0E0; }
    .stButton>button { background-color: #1c4b82; color: white; border-radius: 4px; width: 100%; }
    .stButton>button:hover { background-color: #2c5f9e; color: white; }
    h1, h2, h3 { color: #ffffff; }
    </style>
    """, unsafe_allow_html=True)

# ==========================================
# 2. WRDS Connection (Cached for Speed)
# ==========================================
@st.cache_resource
def connect_wrds(user, pwd):
    try:
        conn = wrds.Connection(wrds_username=user, wrds_password=pwd, auto_connect=True)
        return conn
    except Exception as e:
        return None

# ==========================================
# 3. Sidebar Configuration
# ==========================================
with st.sidebar:
    st.header("🔐 WRDS Credentials")
    if 'user' not in st.session_state:
        st.session_state.user = ""
    if 'pwd' not in st.session_state:
        st.session_state.pwd = ""
        
    user_input = st.text_input("Username", value=st.session_state.user)
    pwd_input = st.text_input("Password", type="password", value=st.session_state.pwd)
    
    if st.button("Login to WRDS"):
        if user_input and pwd_input:
            st.session_state.user = user_input
            st.session_state.pwd = pwd_input
            st.rerun()
        else:
            st.warning("Please enter credentials")

    st.divider()
    
    # Only show settings if logged in
    if st.session_state.user:
        st.header("⚙️ Chart Settings")
        show_vol = st.checkbox("Show Volume", value=True)
        show_bench = st.checkbox("Compare S&P 500", value=True)
        ma_period = st.slider("MA Period", 5, 200, 20)
    else:
        st.info("Please login to access settings")

# ==========================================
# 4. Data Fetching Functions
# ==========================================
@st.cache_data(ttl=3600)
def fetch_stock_data(_conn, ticker, start_date):
    # 1. Get Permno first
    q_permno = f"SELECT permno FROM crsp.msenames WHERE ticker='{ticker}' AND namedt<='{start_date}' ORDER BY namedt DESC LIMIT 1"
    try:
        permno_df = _conn.raw_sql(q_permno)
        if permno_df.empty: return None, "Ticker not found"
        permno = permno_df.iloc[0]['permno']
        
        # 2. Fetch Data
        q_data = f"""
            SELECT date, prc, ret, vol, shrout 
            FROM crsp.dsf 
            WHERE permno={permno} AND date >= '{start_date}'
            ORDER BY date
        """
        df = _conn.raw_sql(q_data, date_cols=['date'])
        if not df.empty:
            df.set_index('date', inplace=True)
            df['prc'] = df['prc'].abs() # Handle negative prices
            return df, None
        return None, "No data returned"
    except Exception as e:
        return None, str(e)

@st.cache_data(ttl=3600)
def fetch_sp500_data(_conn, start_date):
    # S&P 500 Index (Permno 934000 in DSI)
    q_sp = f"""
        SELECT date, vwretd 
        FROM crsp.dsi 
        WHERE date >= '{start_date}' AND vwretd IS NOT NULL
        ORDER BY date
    """
    try:
        df = _conn.raw_sql(q_sp, date_cols=['date'])
        if not df.empty:
            df.set_index('date', inplace=True)
            # Calculate Cumulative Return
            df['cum_ret'] = (1 + df['vwretd']).cumprod() * 100
            return df['cum_ret']
        return None
    except:
        return None

# ==========================================
# 5. Main Execution
# ==========================================
st.title("📊 ProTrade Terminal")

if not st.session_state.user:
    st.warning("Please login in the sidebar to start.")
    st.stop()

# Connect
conn = connect_wrds(st.session_state.user, st.session_state.pwd)
if not conn:
    st.error("Connection Failed. Check credentials.")
    st.stop()

# Inputs
col1, col2 = st.columns([1, 2])
with col1:
    ticker = st.text_input("Ticker", "AAPL").upper()
with col2:
    # 修改点：将默认日期改为 2023年1月1日
    start_date = st.date_input("Start Date", date(2023, 1, 1))

if st.button("Run Analysis"):
    with st.spinner(f"Fetching data for {ticker}..."):
        df_stock, error = fetch_stock_data(conn, ticker, start_date)
        
        if df_stock is None:
            st.error(f"Error: {error}")
        else:
            # --- Data Processing ---
            # Calculate Stock Cumulative Return for Benchmarking
            df_stock['cum_ret'] = (1 + df_stock['ret']).cumprod() * 100
            
            # Fetch Benchmark
            sp_data = None
            if show_bench:
                sp_data = fetch_sp500_data(conn, start_date)
            
            # --- Metrics ---
            last_price = df_stock['prc'].iloc[-1]
            last_ret = df_stock['ret'].iloc[-1]
            
            m1, m2, m3 = st.columns(3)
            m1.metric("Price", f"${last_price:.2f}")
            m2.metric("Daily Ret", f"{last_ret:.2%}")
            m3.metric("Records", len(df_stock))
            
            # --- PLOTTING LOGIC (Main Chart) ---
            st.subheader("Technical Analysis")
            
            # 1. Define Axes based on Volume toggle
            if show_vol:
                fig = plt.figure(figsize=(15, 9), facecolor='#121212')
                gs = gridspec.GridSpec(2, 1, height_ratios=[3, 1], hspace=0.05)
                ax1 = fig.add_subplot(gs[0]) # Price Axis
                ax2 = fig.add_subplot(gs[1], sharex=ax1) # Volume Axis
            else:
                fig, ax1 = plt.subplots(figsize=(15, 6), facecolor='#121212')
                ax2 = None # Explicitly None to avoid errors
            
            # 2. Plot Price & MA
            ax1.plot(df_stock.index, df_stock['prc'], label='Price', color='#00ff00', linewidth=1)
            
            # MA Calculation & Plot
            ma_val = df_stock['prc'].rolling(window=ma_period).mean()
            ax1.plot(ma_val.index, ma_val, label=f'MA{ma_period}', color='cyan', linestyle='--', alpha=0.7)
            
            # 3. Plot Benchmark (S&P 500)
            if show_bench and sp_data is not None:
                # Reindex SP data to match stock dates
                sp_aligned = sp_data.reindex(df_stock.index)
                ax1.plot(sp_aligned.index, sp_aligned, label='S&P 500 (Cum. Ret)', color='orange', alpha=0.6)
                ax1.legend(loc='upper left')
            else:
                ax1.legend(loc='upper left')
                
            ax1.set_ylabel("Price / Index")
            ax1.grid(True, linestyle=':', alpha=0.3)
            ax1.set_facecolor('#121212')
            
            # 4. Plot Volume (Only if ax2 exists)
            if ax2 is not None:
                colors = ['#00ff00' if r >= 0 else '#ff0000' for r in df_stock['ret']]
                ax2.bar(df_stock.index, df_stock['vol'], color=colors, alpha=0.5, width=1)
                ax2.set_ylabel("Volume")
                ax2.grid(True, linestyle=':', alpha=0.3)
                ax2.set_facecolor('#121212')
                
                # Format Date Axis (Only needed on bottom axis)
                ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
                ax2.xaxis.set_major_locator(mdates.AutoDateLocator())
                plt.setp(ax2.get_xticklabels(), rotation=45, ha='right')
            else:
                # If no volume, format the main axis
                ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
                ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
                plt.setp(ax1.get_xticklabels(), rotation=45, ha='right')

            st.pyplot(fig)

            # ==========================================
            # 6. Raw Data Visualization (New Feature)
            # ==========================================
            st.divider()
            st.subheader("📊 Raw Data Analysis")
            
            col_viz1, col_viz2 = st.columns(2)
            
            with col_viz1:
                st.markdown("#### Return Distribution")
                # Histogram of Returns
                fig_hist, ax_hist = plt.subplots(figsize=(8, 4), facecolor='#121212')
                # Drop NaNs
                returns = df_stock['ret'].dropna()
                
                # Plot Histogram
                ax_hist.hist(returns, bins=50, color='#1f77b4', alpha=0.7, edgecolor='black')
                ax_hist.axvline(returns.mean(), color='red', linestyle='dashed', linewidth=1, label='Mean')
                
                ax_hist.set_title(f"{ticker} Daily Returns Distribution", color='white')
                ax_hist.set_xlabel("Daily Return", color='gray')
                ax_hist.set_ylabel("Frequency", color='gray')
                ax_hist.grid(True, linestyle=':', alpha=0.3)
                ax_hist.legend()
                ax_hist.set_facecolor('#121212')
                ax_hist.tick_params(colors='gray')
                
                st.pyplot(fig_hist)
            
            with col_viz2:
                st.markdown("#### Recent Data Points")
                # Show last 10 rows of raw data
                st.dataframe(df_stock[['prc', 'vol', 'ret']].tail(10))

            # ==========================================
            # 7. Download CSV (New Feature)
            # ==========================================
            st.divider()
            st.subheader("📥 Data Export")
            
            # Prepare CSV
            csv = df_stock.to_csv().encode('utf-8')
            
            st.download_button(
                label="Download CSV Data",
                data=csv,
                file_name=f'{ticker}_historical_data.csv',
                mime='text/csv',
            )
        