import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import numpy as np

class PortfolioETL:
    def __init__(self):
        self.portfolio_df = None
        self.price_data = None
        self.historical_data = None
        
    def extract(self):
        """Extract portfolio data, current prices, and historical data"""
        try:
            # Read portfolio from CSV
            self.portfolio_df = pd.read_csv('portfolio.csv')
            print("✅ Portfolio data extracted")
            
            # Get current prices
            tickers = self.portfolio_df['Ticker'].tolist()
            print(f"📋 Fetching prices for: {tickers}")
            
            # Add auto_adjust=True to fix the warning
            self.price_data = yf.download(tickers, period="1d", auto_adjust=True)['Close']
            print("✅ Current price data extracted")
            
            # Get historical data for volatility calculations (1 year)
            self.historical_data = yf.download(tickers, period="1y", auto_adjust=True)['Close']
            print("✅ Historical data extracted (1 year)")
            
        except Exception as e:
            print(f"❌ Extraction failed: {e}")
            raise
    
    def calculate_portfolio_metrics(self):
        """Calculate advanced portfolio metrics"""
        # Calculate daily returns for each stock
        daily_returns = self.historical_data.pct_change().dropna()
        
        # Create portfolio weights based on current market value
        current_prices = self.historical_data.iloc[-1]
        portfolio_values = current_prices * self.portfolio_df.set_index('Ticker')['Quantity']
        weights = portfolio_values / portfolio_values.sum()
        
        # Calculate portfolio daily returns
        portfolio_daily_returns = (daily_returns * weights).sum(axis=1)
        
        # Calculate metrics
        total_return = (portfolio_daily_returns + 1).prod() - 1
        volatility = portfolio_daily_returns.std() * np.sqrt(252)  # Annualized
        sharpe_ratio = (portfolio_daily_returns.mean() * 252) / (portfolio_daily_returns.std() * np.sqrt(252))
        
        return {
            'total_return': total_return * 100,  # as percentage
            'volatility': volatility * 100,      # as percentage
            'sharpe_ratio': sharpe_ratio
        }
    
    def transform(self):
        """Transform data and calculate metrics"""
        try:
            # Calculate current values (your existing code)
            self.portfolio_df['CurrentPrice'] = self.portfolio_df['Ticker'].map(
                lambda x: self.price_data[x].iloc[-1] if x in self.price_data.columns else 0
            )
            
            self.portfolio_df['MarketValue'] = (
                self.portfolio_df['Quantity'] * self.portfolio_df['CurrentPrice']
            )
            
            self.portfolio_df['CostBasis'] = (
                self.portfolio_df['Quantity'] * self.portfolio_df['PurchasePrice']
            )
            
            self.portfolio_df['UnrealizedPnl'] = (
                self.portfolio_df['MarketValue'] - self.portfolio_df['CostBasis']
            )
            
            self.portfolio_df['UnrealizedPnlPercent'] = (
                (self.portfolio_df['UnrealizedPnl'] / self.portfolio_df['CostBasis']) * 100
            )
            
            # Calculate portfolio totals
            total_market_value = self.portfolio_df['MarketValue'].sum()
            total_cost_basis = self.portfolio_df['CostBasis'].sum()
            total_pnl = total_market_value - total_cost_basis
            total_return_pct = (total_pnl / total_cost_basis) * 100
            
            # Calculate advanced metrics
            advanced_metrics = self.calculate_portfolio_metrics()
            
            portfolio_metrics = {
                'timestamp': datetime.now(),
                'total_market_value': total_market_value,
                'total_cost_basis': total_cost_basis,
                'total_unrealized_pnl': total_pnl,
                'total_return_percent': total_return_pct,
                'volatility': advanced_metrics['volatility'],
                'sharpe_ratio': advanced_metrics['sharpe_ratio'],
                'annual_return': advanced_metrics['total_return']
            }
            
            print("✅ Data transformation completed")
            return self.portfolio_df, portfolio_metrics
            
        except Exception as e:
            print(f"❌ Transformation failed: {e}")
            raise
    
    def run(self):
        """Execute the complete ETL pipeline"""
        print("🚀 Starting ETL Pipeline...")
        self.extract()
        portfolio_details, portfolio_metrics = self.transform()
        
        print(f"📊 Portfolio Value: ${portfolio_metrics['total_market_value']:,.2f}")
        print(f"📈 Total Return: {portfolio_metrics['total_return_percent']:.2f}%")
        print(f"📉 Annual Volatility: {portfolio_metrics['volatility']:.2f}%")
        print(f"🎯 Sharpe Ratio: {portfolio_metrics['sharpe_ratio']:.2f}")
        
        return portfolio_details, portfolio_metrics
