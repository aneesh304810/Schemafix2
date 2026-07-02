"""
Datapoint 360 indexer.

Scans every column/field across all modules and builds the data-point registry:
"where does the data point 'account_id' appear?" -> across feeds, models, API
fields, interfaces. Populates dp_registry (one row per normalized data point) and
dp_occurrences (one row per place it appears).

Reads (from already-loaded catalog tables):
  - columns            (feed fields, dbt model columns, oracle table columns)
  - api_fields         (API request/response fields)
  - feed_catalog cols  (inbound/outbound feed fields, via columns)
  - pii_field_matches  (to flag PII data points)

This runs LAST (after all other connectors), reading the DB it just populated,
so it must run as a DB-reading step, not a file connector.
"""
from __future__ import annotations
import logging
import re
from collections import defaultdict

log = logging.getLogger("cp.datapoint")


def _normalize(name: str) -> str:
    """Normalize a field name so account_id / accountId / ACCOUNT_ID collapse
    to the same 'account_id'."""
    s = name or ""
    # only split camelCase when the string is NOT already all-caps or all-lower
    # with separators (i.e. it has a genuine lower->Upper boundary)
    if re.search(r"[a-z][A-Z]", s):
        s = re.sub(r"(?<=[a-z])(?=[A-Z])", "_", s)        # camelCase -> camel_Case
    s = re.sub(r"[^a-zA-Z0-9]+", "_", s).strip("_").lower()
    return s


class DatapointIndexer:
    """Reads from the DB cursor (not a file). Run after all loaders."""

    def __init__(self, conn):
        self.conn = conn

    def run(self) -> dict:
        cur = self.conn.cursor()
        registry = {}          # normalized -> dp_registry row
        occurrences = []       # dp_occurrences rows

        def add(norm, display, module, ref_key, ref_label, project_id, direction=None):
            r = registry.setdefault(norm, {
                "dp_name_normalized": norm[:256], "dp_display_name": display[:256],
                "occurrence_count": 0, "module_count": 0, "is_pii": "N",
                "pii_attribute": None, "pii_category": None,
                "primary_project_id": project_id, "project_ids_csv": "",
                "is_key": "Y" if norm.endswith("_id") or norm.endswith("id")
                          or norm in ("account", "client", "portfolio") else "N",
                "in_inbound": "N", "in_outbound": "N",
                "_modules": set(), "_projects": set(),
            })
            r["occurrence_count"] += 1
            r["_modules"].add(module)
            if direction == "inbound":
                r["in_inbound"] = "Y"
            elif direction == "outbound":
                r["in_outbound"] = "Y"
            if project_id:
                r["_projects"].add(project_id)
            occurrences.append({
                "dp_name_normalized": norm[:256], "module": module[:40],
                "ref_key": (ref_key or "")[:520], "ref_label": (ref_label or "")[:512],
                "project_id": project_id, "direction": direction,
            })

        # ---- 0. build direction map: which dataset_keys are inbound vs outbound ----
        # Inbound  = SWP EOD feeds (datasets FEED + feed_catalog direction=inbound)
        # Outbound = loaders (loader_catalog) + feed_catalog direction=outbound
        inbound_keys, outbound_keys = set(), set()
        try:
            cur.execute("""SELECT LOWER(platform_id||'.'||schema_name||'.'||object_name)
                           FROM datasets WHERE object_type = 'FEED'""")
            inbound_keys.update(r[0] for r in cur.fetchall() if r[0])
        except Exception:
            pass
        try:
            cur.execute("SELECT feed_id, direction FROM feed_catalog")
            for fid, direction in cur.fetchall():
                key = (fid or "").lower()
                (inbound_keys if direction == "inbound" else outbound_keys).add(key)
        except Exception:
            pass
        try:
            cur.execute("SELECT loader_id FROM loader_catalog")
            outbound_keys.update((r[0] or "").lower() for r in cur.fetchall())
        except Exception:
            pass

        def _direction_of(dskey):
            k = (dskey or "").lower()
            tail = k.split(".")[-1]
            if k in inbound_keys or tail in inbound_keys:
                return "inbound"
            if k in outbound_keys or tail in outbound_keys:
                return "outbound"
            return None

        # ---- 1. columns (feeds, dbt models, oracle tables) ----
        try:
            cur.execute("""SELECT column_name,
                                  LOWER(platform_id||'.'||schema_name||'.'||object_name) AS dataset_key
                           FROM columns WHERE column_name IS NOT NULL""")
            for name, dskey in cur.fetchall():
                norm = _normalize(name)
                if not norm:
                    continue
                direction = _direction_of(dskey)
                module = ("Inbound" if direction == "inbound"
                          else "Outbound" if direction == "outbound" else "Data360")
                add(norm, name, module, f"{dskey}.{name}", f"{dskey}.{name}", None,
                    direction=direction)
        except Exception as e:
            log.warning("datapoint: columns scan skipped: %s", e)

        # ---- 2. api_fields ----
        try:
            cur.execute("""SELECT field_name, endpoint_key, is_pii,
                                  pii_attribute, pii_category
                           FROM api_fields WHERE field_name IS NOT NULL""")
            for fname, ek, is_pii, pa, pc in cur.fetchall():
                norm = _normalize(fname)
                if not norm:
                    continue
                add(norm, fname, "API360", ek, ek, None)
                if is_pii == "Y":
                    registry[norm]["is_pii"] = "Y"
                    registry[norm]["pii_attribute"] = pa
                    registry[norm]["pii_category"] = pc
        except Exception as e:
            log.warning("datapoint: api_fields scan skipped: %s", e)

        # ---- 3. interface360 fields (if interface columns exist) ----
        try:
            cur.execute("""SELECT object_name, project_id FROM datasets
                           WHERE object_type = 'INTERFACE'""")
            for nm, proj in cur.fetchall():
                norm = _normalize(nm)
                if norm:
                    add(norm, nm, "Interface360", nm, nm, proj)
        except Exception:
            pass

        # ---- 4. PII flag from pii_field_matches ----
        try:
            cur.execute("""SELECT matched_field_name, pii_attribute, sensitivity_category
                           FROM pii_field_matches""")
            for cname, pa, pc in cur.fetchall():
                norm = _normalize(cname)
                if norm in registry:
                    registry[norm]["is_pii"] = "Y"
                    registry[norm]["pii_attribute"] = registry[norm]["pii_attribute"] or pa
                    registry[norm]["pii_category"] = registry[norm]["pii_category"] or pc
        except Exception as e:
            log.warning("datapoint: pii flag skipped: %s", e)

        # finalize counts
        reg_rows = []
        for norm, r in registry.items():
            r["module_count"] = len(r.pop("_modules"))
            projs = r.pop("_projects")
            r["project_ids_csv"] = ",".join(sorted(projs))[:400]
            r["primary_project_id"] = (sorted(projs)[0] if projs else None)
            reg_rows.append(r)

        cur.close()
        log.info("datapoint: %d data points, %d occurrences",
                 len(reg_rows), len(occurrences))
        return {"registry": reg_rows, "occurrences": occurrences}

    def load(self, loader, bundle):
        for r in bundle["registry"]:
            loader._merge("dp_registry", ("dp_name_normalized",), r)
        for o in bundle["occurrences"]:
            loader._merge("dp_occurrences",
                          ("dp_name_normalized", "module", "ref_key"), o)
        loader.commit()
