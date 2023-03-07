# trunkfingerprint
This extension calculates a fingerprint of the DB structure.

The main function to call is a set-returning
```
trunkfingerprint.get_db_structure_hash(
       p_level int default 0,
       p_catalog regclass default null,
       p_exclude_schemas name[] default '{}'
)
```
`p_level` could be
* 0 -- single fingerprint for the whole DB structure,
* 1 -- one value for each catalog,
* 2 -- a line per each catalog object.

`p_catalog` is optional. If set, only the specified catalog is explored.

`p_exclude_schemas` is an array of schemas to exclude from calculation.

Known to run (but not necessarily well-tested) on PostgreSQL versions 10 thru 15.

Run `sudo make install` to install it, and `create extension trunkfingerprint;` to add it to a particular database.
