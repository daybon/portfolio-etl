import streamlit as st
import pandas as pd
import plotly.express as px
from database import DatabaseManager
from etl_pipeline import PortfolioETL
import yfinance as yf
from datetime import datetime

st.set_page_config(page_title="Portfolio Analytics", layout="wide")

# Title
st.title("Portfolio Analytics Dashboard")
st.markdown("---")

# Sidebar for user input
with st.sidebar:
    st.header("Add Your Holdings")
    
    # Portfolio input form
    with st.form("portfolio_form"):
        ticker = st.text_input("Stock Ticker (e.g., AAPL)", "AAPL")
        quantity = st.number_input("Quantity", min_value=1, value=10)
        purchase_price = st.number_input("Purchase Price ($)", min_value=0.0, value=150.0)
        
        submitted = st.form_submit_button("Add to Portfolio")
        
        if submitted:
            # Add to CSV file
            new_data = pd.DataFrame({
                'Ticker': [ticker.upper()],
                'Quantity': [quantity],
                'PurchasePrice': [purchase_price]
            })
            
            try:
                existing_data = pd.read_csv('portfolio.csv')
                updated_data = pd.concat([existing_data, new_data], ignore_index=True)
            except:
                updated_data = new_data
                
            updated_data.to_csv('portfolio.csv', index=False)
            st.success(f"Added {quantity} shares of {ticker.upper()} at ${purchase_price}")
            st.rerun()
    
    # Display current portfolio with delete option
    st.markdown("---")
    st.subheader("Current Portfolio")
    try:
        current_portfolio = pd.read_csv('portfolio.csv')
        
        # Display the portfolio
        st.dataframe(current_portfolio, use_container_width=True)
        
        # Create delete form
        with st.form("delete_form"):
            st.write("Delete a Stock")
            
            # Create dropdown with tickers
            tickers_to_delete = current_portfolio['Ticker'].tolist()
            if tickers_to_delete:
                ticker_to_delete = st.selectbox(
                    "Select stock to delete",
                    tickers_to_delete,
                    key="delete_select"
                )
                
                delete_submitted = st.form_submit_button("Delete Stock")
                
                if delete_submitted:
                    # Remove the selected ticker
                    updated_portfolio = current_portfolio[current_portfolio['Ticker'] != ticker_to_delete]
                    updated_portfolio.to_csv('portfolio.csv', index=False)
                    st.success(f"Deleted {ticker_to_delete} from portfolio!")
                    st.rerun()
                elif delete_submitted and not confirm_delete:
                    st.warning("Please check the confirmation box to delete")
            else:
                st.info("No stocks in portfolio to delete")
                
    except:
        st.info("No portfolio data yet. Add stocks above.")


    st.markdown("---")
    # Run ETL button
    if st.button("Refresh", type="primary"):
        with st.spinner("Fetching latest data..."):
            try:
                etl = PortfolioETL()
                db = DatabaseManager()
                
                db.connect()
                portfolio_details, portfolio_metrics = etl.run()
                db.save_initial_holdings(portfolio_details)
                db.save_portfolio_snapshot(portfolio_details, portfolio_metrics)
                db.close()
                
                st.success("Portfolio data updated successfully!")
            except Exception as e:
                st.error(f"Error: {e}")

# Connect to database
db = DatabaseManager()
try:
    db.connect()
    
    # Main dashboard metrics - Combined View
    st.subheader("Portfolio Metrics")
    
    # Create 6 columns for all metrics
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    try:
        # Get latest portfolio metrics from database
        with db.conn.cursor() as cur:
            cur.execute("""
                SELECT total_value, total_return, volatility, sharpe_ratio
                FROM portfolio_snapshots 
                ORDER BY snapshot_date DESC 
                LIMIT 1
            """)
            latest = cur.fetchone()
        
        # Read current portfolio for calculation
        portfolio_df = pd.read_csv('portfolio.csv')
        tickers = portfolio_df['Ticker'].tolist()
        
        # Get current prices for live calculations
        if tickers:
            price_data = yf.download(tickers, period="1d", auto_adjust=True)['Close'].iloc[-1]
            
            # Calculate live metrics
            total_invested = 0
            total_current = 0
            
            for _, row in portfolio_df.iterrows():
                ticker = row['Ticker']
                quantity = row['Quantity']
                purchase_price = row['PurchasePrice']
                current_price = price_data[ticker] if ticker in price_data.index else 0
                
                total_invested += quantity * purchase_price
                total_current += quantity * current_price
            
            total_gain_loss = total_current - total_invested
            total_gain_loss_pct = (total_gain_loss / total_invested) * 100 if total_invested > 0 else 0
            
            # Display all metrics together
            with col1:
                st.metric("Total Invested", f"${total_invested:,.2f}")
            with col2:
                st.metric("Current Value", f"${total_current:,.2f}")
            with col3:
                st.metric("Total Gain/Loss", 
                         f"${total_gain_loss:,.2f}",
                         f"{total_gain_loss_pct:.2f}%")
            
            # Database metrics (if available)
            if latest:
                with col4:
                    st.metric("Total Return", f"{latest[1]:.2f}%")
                with col5:
                    st.metric("Volatility", f"{latest[2]:.2f}%")
                with col6:
                    st.metric("Sharpe Ratio", f"{latest[3]:.2f}")
            else:
                with col4:
                    st.metric("Total Return", "N/A")
                with col5:
                    st.metric("Volatility", "N/A")
                with col6:
                    st.metric("Sharpe Ratio", "N/A")
        else:
            st.info("No stocks in portfolio. Add stocks from the sidebar.")
            
    except Exception as e:
        st.error(f"Error calculating metrics: {e}")
    
    # Stock Performance vs Purchase Price
    st.markdown("---")
    st.subheader("Stock Performance vs Purchase Price")
    
    try:
        # Read portfolio from CSV
        portfolio_df = pd.read_csv('portfolio.csv')
        tickers = portfolio_df['Ticker'].tolist()
        
        # Get current prices
        price_data = yf.download(tickers, period="1d", auto_adjust=True)['Close'].iloc[-1]
        
        # Calculate performance
        perf_data = []
        for _, row in portfolio_df.iterrows():
            ticker = row['Ticker']
            quantity = row['Quantity']
            purchase_price = row['PurchasePrice']
            current_price = price_data[ticker] if ticker in price_data.index else 0
            
            total_invested = quantity * purchase_price
            current_value = quantity * current_price
            gain_loss = current_value - total_invested
            gain_loss_pct = (gain_loss / total_invested) * 100 if total_invested > 0 else 0
            
            perf_data.append({
                'Ticker': ticker,
                'Quantity': quantity,
                'Purchase Price': purchase_price,
                'Current Price': current_price,
                'Total Invested': total_invested,
                'Current Value': current_value,
                'Gain/Loss ($)': gain_loss,
                'Gain/Loss (%)': gain_loss_pct,
                'Is Positive': gain_loss_pct > 0  # For color coding
            })
        
        perf_df = pd.DataFrame(perf_data)
        
        # Sort by Gain/Loss % (highest gain to highest loss)
        perf_df = perf_df.sort_values('Gain/Loss (%)', ascending=False)
        
        # Format display values
        display_df = perf_df.copy()
        display_df['Purchase Price'] = display_df['Purchase Price'].apply(lambda x: f"${x:.2f}")
        display_df['Current Price'] = display_df['Current Price'].apply(lambda x: f"${x:.2f}")
        display_df['Total Invested'] = display_df['Total Invested'].apply(lambda x: f"${x:,.2f}")
        display_df['Current Value'] = display_df['Current Value'].apply(lambda x: f"${x:,.2f}")
        display_df['Gain/Loss ($)'] = display_df['Gain/Loss ($)'].apply(lambda x: f"${x:,.2f}")
        display_df['Gain/Loss (%)'] = display_df['Gain/Loss (%)'].apply(lambda x: f"{x:.2f}%")
        
        # Create bar chart with color coding
        fig = px.bar(perf_df, x='Ticker', y='Gain/Loss (%)',
                     title="Percentage Gain/Loss by Stock (Sorted by Performance)",
                     color='Is Positive',
                     color_discrete_map={True: 'green', False: 'red'},
                     text=perf_df['Gain/Loss (%)'].apply(lambda x: f"{x:.2f}%"),
                     category_orders={'Ticker': perf_df['Ticker'].tolist()})
        
        # Customize hover text with 2 decimal points
        fig.update_traces(
            hovertemplate="<b>%{x}</b><br>" +
                         "Gain/Loss: %{y:.2f}%<br>" +
                         "Amount: $%{customdata[0]:,.2f}<br>" +
                         "Current Price: $%{customdata[1]:.2f}<br>" +
                         "Purchase Price: $%{customdata[2]:.2f}<extra></extra>",
            customdata=perf_df[['Gain/Loss ($)', 'Current Price', 'Purchase Price']].values,
            texttemplate='%{text}',
            textposition='outside'
        )
        
        # Format y-axis as percentage with 2 decimal points
        fig.update_layout(
            yaxis_tickformat=".2f",
            yaxis_title="Gain/Loss (%)",
            xaxis_title="Stock Ticker",
            showlegend=False,
            uniformtext_minsize=8,
            uniformtext_mode='hide'
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Display detailed table
        st.dataframe(display_df[['Ticker', 'Quantity', 'Purchase Price', 'Current Price', 
                                'Total Invested', 'Current Value', 'Gain/Loss ($)', 'Gain/Loss (%)']], 
                    use_container_width=True)
        
    except Exception as e:
        st.error(f"Error calculating performance: {e}")
    
    # Current holdings pie chart
    st.markdown("---")
    st.subheader("Portfolio Allocation")
    
    with db.conn.cursor() as cur:
        cur.execute("""
            SELECT ticker, market_value 
            FROM holdings_snapshot 
            WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM holdings_snapshot)
        """)
        holdings_data = cur.fetchall()
        
        if holdings_data:
            holdings_df = pd.DataFrame(holdings_data, columns=['Ticker', 'Value'])
            fig = px.pie(holdings_df, values='Value', names='Ticker', 
                        title="Current Asset Allocation")
            st.plotly_chart(fig, use_container_width=True)
    
    db.close()
    
except Exception as e:
    st.error(f"Database error: {e}")
