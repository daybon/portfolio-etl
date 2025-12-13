from etl_pipeline import PortfolioETL
from database import DatabaseManager

if __name__ == "__main__":
    # Initialize ETL and Database
    etl = PortfolioETL()
    db = DatabaseManager()
    
    try:
        # Connect to database
        db.connect()
        
        # Run ETL
        portfolio_details, portfolio_metrics = etl.run()

        db.save_initial_holdings(portfolio_details)
        
        # Save to database
        db.save_portfolio_snapshot(portfolio_details, portfolio_metrics)
        
        print("ETL Pipeline completed")
        
    except Exception as e:
        print(f"ETL Pipeline failed: {e}")
    finally:
        db.close()
