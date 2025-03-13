
# for data requirements, see: docs/ADR document 1.md


# NOTES ON DATA FORMATS - Bronze Layer

Bronze is raw data - staging.
If we do transforms, they are quick and minor


Suggest we run the following system for raw data classification:

## YYYY_MM_to_YYYY_MM_tickername_interval_source.csv

e.g. for 20 years of yfinance SPY:
               1990-01-to-2020-12-SPY-5M-yfinance.csv

NOTE:
for single day data, which we are mostly going to get, use the following:
               2020-12-SPY-5M-alphavantage.csv



# API use versus data requirement

I think it will be required to download >1 year at a time
Suggest we get from the APIs in 5 year blocks, if the data is not too large.




# Suggest we use AlphaVantage as our primary API
