# pycsvshard

Suppose `test.csv` hold 10000 lines. The following code creates 10 CSV files with identical schema, `test.001.csv` through `test.010.csv`. (Do not try to create more than 999 shards.)

`python3.5 csvshard.py -n1000 test.csv`
