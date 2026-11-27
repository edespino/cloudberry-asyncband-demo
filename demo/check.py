# Usage: python check.py out.parquet        (coordinator mode, one file)
#        python check.py 'out.*.parquet'    (parallel mode, one file per segment)
import sys, polars as pl
print(pl.scan_parquet(sys.argv[1]).select(pl.len()).collect().item(), "rows read by Polars")
