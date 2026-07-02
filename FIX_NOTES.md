# Round-2 fixes (after datapoint_index succeeded with 1,513 data points)

Great progress: the columns-schema fix worked — datapoint_index now builds 1,513
data points / 4,844 occurrences, and false gaps collapsed (flow 52->20, ref 197->62).
Three remaining issues, all fixed here:

1. search_index FAILED: ORA-12899 SUBTITLE too large (1002 > 1000)
   -> char-slicing [:1000] didn't cap BYTES (Oracle measures bytes; a multibyte char
      like the middot pushed it to 1002). Added byte-safe truncation before every merge
      (subtitle<=1000, name/artifact_key/nav_id<=400, body_text<=4000 bytes).

2. search_index WARNING: ORA-00904 PII_SENSITIVITY_CATEGORY invalid
   -> pii_classifications real columns are pii_component + sensitivity_category
      (not pii_components / pii_sensitivity_category). Fixed the query + refs.

3. datapoint WARNING: ORA-00904 PII_CATEGORY invalid
   -> pii_field_matches real columns are matched_field_name + pii_attribute +
      sensitivity_category (not column_name / pii_category). Fixed the query.
      (PII flags will now actually attach to data points.)

PLUS reference_data category forward-fill:
   Your reference file logged every category as ''. Most likely the Category column
   uses a "section header" layout (named once atop each block, blank below). The
   connector now forward-fills: a blank category inherits the last non-blank one.
   If categories are TRULY absent (no column at all), they stay blank and only the
   Browse-by-Category grouping is affected — matching still works on field name.

## Apply
Replace these 3 files, then re-run:
   python -m ingestion.run datapoint_index reference_data search_index
(These three in this order: datapoint rebuilds with PII, reference re-reads categories,
search rebuilds clean.)

## Expect after
- datapoint: no PII warning; PII flags attached.
- reference_data: categories populated IF the file uses the section-header layout.
- search_index: completes (no ORA-12899), PII rows indexed.
- Remaining unresolved (flow 20, ref ~62) are now REAL gaps — see note below.
