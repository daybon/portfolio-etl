import pandas as pd
import yfinance as yf
from datetime import datetime
import logging

print("✅ etl_pipeline.py loaded successfully!")

class PortfolioETL:
    def __init__(self):
        self.portfolio_df = None
        self.price_data = None
        print("✅ PortfolioETL class initialized!")
        
    def extract(self):
        """Extract portfolio data and current prices"""
        try:
            # Read portfolio from CSV
            self.portfolio_df = pd.read_csv('portfolio.csv')
            print("✅ Portfolio data extracted")
            
            # Get current prices
            tickers = self.portfolio_df['Ticker'].tolist()
            print(f"📋 Fetching prices for: {tickers}")
            
            self.price_data = yf.download(tickers, period="1d")['Close']
            print("✅ Price data extracted")
            
        except Exception as e:
            print(f"❌ Extraction failed: {e}")
            raise
    
    def transform(self):
        """Transform data and calculate metrics"""
        try:
            # Calculate current values
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
            
            portfolio_metrics = {
                'timestamp': datetime.now(),
                'total_market_value': total_market_value,
                'total_cost_basis': total_cost_basis,
                'total_unrealized_pnl': total_pnl,
                'total_return_percent': total_return_pct,
                'volatility': 0.0,
                'sharpe_ratio': 0.0
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
        
        return portfolio_details, portfolio_metrics
