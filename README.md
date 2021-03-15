# Building a database of PyPI information

To refresh the database, perform the following steps:

1. `py -m pypidata raw` (raw data)
2. `py -m pypidata pkg` (extract metadata to tables)
3. `py -m pypidata chg` (changelog)
