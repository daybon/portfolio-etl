import psycopg2
import os
from datetime import datetime

class DatabaseManager:
    def __init__(self):
        self.conn = None
        
    def connect(self):
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(
                host='database-1.cz1wx0qlnvul.us-east-1.rds.amazonaws.com',
                database='postgres',
                user='postgres',
                password='HolyMolyZoly',
                port='5432'
            )
            print("Database connected")
            self.initialize_database()
        except Exception as e:
            print(f"Database connection failed: {e}")
            raise
    
    def initialize_database(self):
        """Create necessary tables"""
        with self.conn.cursor() as cur:
            # Portfolio holdings table (your original holdings)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS holdings (
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(10) NOT NULL,
                    quantity INTEGER NOT NULL,
                    purchase_price DECIMAL(10,2) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Portfolio snapshots table (for historical portfolio metrics)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                    id SERIAL PRIMARY KEY,
                    snapshot_date TIMESTAMP NOT NULL,
                    total_value DECIMAL(15,2) NOT NULL,
                    total_return DECIMAL(8,4),
                    volatility DECIMAL(8,4),
                    sharpe_ratio DECIMAL(8,4),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Holdings snapshot table (for historical individual holdings)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS holdings_snapshot (
                    id SERIAL PRIMARY KEY,
                    snapshot_date TIMESTAMP NOT NULL,
                    ticker VARCHAR(10) NOT NULL,
                    quantity INTEGER NOT NULL,
                    current_price DECIMAL(10,2) NOT NULL,
                    market_value DECIMAL(15,2) NOT NULL,
                    unrealized_pnl DECIMAL(15,2) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
        self.conn.commit()
        print("All database tables created")

    def save_initial_holdings(self, portfolio_df):
        """Save the original portfolio holdings to database"""
        try:
            with self.conn.cursor() as cur:
                # Clear existing holdings (optional)
                cur.execute("DELETE FROM holdings")
                
                # Insert current portfolio
                for _, holding in portfolio_df.iterrows():
                    cur.execute("""
                        INSERT INTO holdings (ticker, quantity, purchase_price)
                        VALUES (%s, %s, %s)
                    """, (
                        str(holding['Ticker']),
                        int(holding['Quantity']),
                        float(holding['PurchasePrice'])
                    ))
                
            self.conn.commit()
            print("Initial holdings saved to database")
            
        except Exception as e:
            self.conn.rollback()
            print(f"Failed to save initial holdings: {e}")
            raise
    
    def save_portfolio_snapshot(self, portfolio_details, portfolio_metrics):
        """Save current portfolio state to database"""
        try:
            with self.conn.cursor() as cur:
                # Save portfolio summary - convert ALL values to native Python types
                cur.execute("""
                    INSERT INTO portfolio_snapshots 
                    (snapshot_date, total_value, total_return, volatility, sharpe_ratio)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    portfolio_metrics['timestamp'],
                    float(portfolio_metrics['total_market_value']),
                    float(portfolio_metrics['total_return_percent']),
                    float(portfolio_metrics['volatility']),
                    float(portfolio_metrics['sharpe_ratio'])
                ))
                
                # Save individual holdings
                for _, holding in portfolio_details.iterrows():
                    cur.execute("""
                        INSERT INTO holdings_snapshot 
                        (snapshot_date, ticker, quantity, current_price, market_value, unrealized_pnl)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        portfolio_metrics['timestamp'],
                        str(holding['Ticker']),
                        int(holding['Quantity']),
                        float(holding['CurrentPrice']),
                        float(holding['MarketValue']),
                        float(holding['UnrealizedPnl'])
                    ))
                
            self.conn.commit()
            print("Portfolio snapshot saved to database")
            
        except Exception as e:
            self.conn.rollback()
            print(f"Failed to save snapshot: {e}")
            raise
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            print("Database connection closed")
