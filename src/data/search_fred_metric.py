from fredapi import Fred
from src.constants import FRED_API
import sys
# run this file with debug mode to check search_df content
fred = Fred(api_key=FRED_API)
search_df = fred.search("oil")

search_df['popularity'] = search_df['popularity'].astype(int)
search_df = search_df.sort_values(by=['popularity'], ascending=False)
print(search_df.head())
sys.exit()