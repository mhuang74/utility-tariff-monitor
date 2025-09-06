

jq '[.[] | select(.sector == "Commercial" and .country == "USA") | {utilityName: .utilityName, eiaId: .eiaId, sector: .sector, rateName: .rateName, effectiveDate: .effectiveDate."$date", lastRevision: (.revisions | last).date."$date", sourceReference: .sourceReference}] | unique_by(.eiaId)' sample_rates.json