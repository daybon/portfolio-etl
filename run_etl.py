print("🟡 run_etl.py started executing...")

try:
    from etl_pipeline import PortfolioETL
    print("✅ Successfully imported PortfolioETL")
except ImportError as e:
    print(f"❌ Import failed: {e}")
    print("📁 Current directory files:")
    import os
    for file in os.listdir('.'):
        print(f"   - {file}")
    exit(1)

if __name__ == "__main__":
    print("🟡 Starting main execution...")
    etl = PortfolioETL()
    portfolio_details, portfolio_metrics = etl.run()
    
    # Print results
    print("\n📋 Portfolio Details:")
    print(portfolio_details[['Ticker', 'Quantity', 'CurrentPrice', 'MarketValue', 'UnrealizedPnlPercent']])
    print("🎉 ETL completed successfully!")
