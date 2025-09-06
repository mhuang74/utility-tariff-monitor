# utility-tariff-monitor
Monitor for updates to Electric Utility Tariff PDF documents


## Get list of tariff has changed recently from iurdb

```
cat sample_rates.json | ./extract_essential_iurdb_fields
gzcat iurdb.json.gz | ./extract_essential_iurdb_fields > url_candidates_2.json
```
