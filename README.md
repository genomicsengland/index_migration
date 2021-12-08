# Index UKC -> AWS Migration Scripts

The scripts in this repository were used as QC scripts whilst the exact approach to migrating EDS's Index database from UKC to AWS was being refined, and were used to ensure that when the final migration was carried out the data had been faithfully transferred.

The different approaches investigated didn't always keep the `information_schema` intact, therefore `get_db_schemas_and_tables.r` was used to generate the list of tables to investigate and flag those tables that were ignored (tables that were too large to used the hashing approach and needed to be checked individually).
The main script used was `compare_two_db_copies_rc_hash.r` which generates an md5sum for each table and writes out a list of dataframes with row count and hash for each non-ignored table.
`comp.r` then combines the output data from each instance into a single dataframe for investigation.

The scripts require a `.env` file with the following variables:

```
A_NAME="UKC"
A_HOST=""
A_PORT=""
A_USER=""
A_PWD=""
B_NAME="AWS"
B_HOST=""
B_PORT=""
B_USER=""
B_PWD=""
```

The user used to log into each of the instances should be the postgres superuser so that it can read from every table regardless of any permissions set.

`compare_hash_ignored.r` runs a similar md5sum query on the ignored tables, but with some optimisation to sort the tables beforehand.
