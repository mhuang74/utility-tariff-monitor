# utility-tariff-monitor
Monitor for updates to Electric Utility Tariff PDF documents


## Utilities with recently changed commerical tariffs (from recent iurdb dump)

```
cat sample_rates.json | ./extract_essential_iurdb_fields
gzcat iurdb.json.gz | ./extract_essential_iurdb_fields > url_candidates_2.json
```
