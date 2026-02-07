# B2B Integration: Identity Decoupling

**EPOCH:** 5 - DAG ORCHESTRATION
**CREATED:** 30 JAN 2026
**STATUS:** Core Architecture

---

## TL;DR

```
┌────────────────────────────────────────────────────────────────────────────┐
│  EXTERNAL B2B IDENTIFIERS ARE NEVER USED AS INTERNAL PRIMARY KEYS          │
│                                                                            │
│  B2B sends: dataset_id + resource_id + version_id                          │
│  We store:  platform_refs = {"dataset_id": "...", ...}   (metadata)        │
│  We use:    asset_id = SHA256(platform_id | platform_refs)[:32]  (PK)      │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## The Two Versioning Concepts

### 1. Semantic Version (THEIR DOMAIN)

**Owner:** B2B Application (DDH, ArcGIS, etc.)

**Purpose:** Track content lineage and meaning

**Example:**
```
DDH "World Bank Boundaries"
├── v1 (2023): Initial release
├── v2 (2023): Added GeoJSON format
├── v3 (2024): Updated country X boundaries
├── v4 (2024): Fixed topology errors
└── v5 (2025): Annual boundary updates
```

**Our role:** Store in `platform_refs.version_id`. Never manage, increment, or validate.

---

### 2. Revision (OUR DOMAIN)

**Owner:** Our Platform

**Purpose:** Operational rollback ("oops" recovery)

**Example:**
```
DDH version "2025-01" (their v5)
│
├── revision 1: Initial processing
├── revision 2: Reprocessed (worker bug fix)
├── revision 3: Reprocessed (COG param update)
└── revision 4: Rolled back from revision 3
```

**Key point:** The B2B's `version_id` stays CONSTANT across all our revisions.

---

## Why This Architecture?

### Problem: B2B ID Schemes Vary

| Platform | Their Identifiers |
|----------|-------------------|
| DDH | dataset_id + resource_id + version_id |
| ArcGIS | item_id + layer_index |
| GeoNode | uuid |
| CKAN | package_id + resource_id |
| Custom | ??? |

If we adopted each scheme, we'd need:
- Different primary key structures per platform
- Schema migrations for each new platform
- Platform-specific query logic everywhere

### Solution: Deterministic Hash Abstraction

```python
def generate_asset_id(platform_id: str, platform_refs: dict) -> str:
    """
    Generate deterministic asset ID from ANY platform's identifiers.

    Same inputs always produce same output.
    Different platforms produce different asset_ids even with same ref values.
    """
    composite = f"{platform_id}|{json.dumps(platform_refs, sort_keys=True)}"
    return hashlib.sha256(composite.encode()).hexdigest()[:32]
```

**Result:** One consistent 32-char hex ID regardless of source platform.

---

## The Invariant

```
For any GeospatialAsset across its entire lifecycle:

  asset_id      = CONSTANT  (our primary key, derived from platform_refs)
  platform_id   = CONSTANT  (which B2B platform)
  platform_refs = CONSTANT  (their identifiers, stored as metadata)

  revision      = CHANGES   (our operational versioning)
```

---

## Platform Registry

New B2B platforms are added without code changes:

```sql
INSERT INTO platforms (platform_id, display_name, required_refs) VALUES
  ('ddh', 'Data Distribution Hub', '["dataset_id", "resource_id", "version_id"]'),
  ('arcgis', 'ArcGIS Online', '["item_id", "layer_index"]'),
  ('geonode', 'GeoNode', '["uuid"]');
```

The orchestrator doesn't care what identifiers a platform uses - it just:
1. Receives `platform_id` + `platform_refs`
2. Generates deterministic `asset_id`
3. Stores `platform_refs` as JSONB metadata
4. Processes the asset

---

## Request → Asset Flow

```
1. DDH Request arrives:
   {
     "dataset_id": "0038272",
     "resource_id": "shapefile_v5",
     "version_id": "2025-01",
     "source_url": "https://..."
   }

2. Platform API generates asset_id:
   asset_id = SHA256("ddh|{\"dataset_id\":\"0038272\",...}")[:32]
            = "a7f3b2c1e8d9f0a1b2c3d4e5f6a7b8c9"

3. Job created with:
   job.input_params = {
     "platform_id": "ddh",
     "platform_refs": {"dataset_id": "0038272", ...},
     "source_url": "https://..."
   }

4. Asset record created/updated:
   asset_id: "a7f3b2c1..."      (our PK)
   platform_id: "ddh"            (source platform)
   platform_refs: {...}          (their IDs as metadata)
   revision: 1                   (our operational version)
   current_job_id: "job_xyz"     (processing link)

5. If DDH sends same request again:
   - Same asset_id generated (deterministic)
   - revision increments to 2
   - platform_refs unchanged
```

---

## Common Misconceptions

### "DDH's version_id maps to our workflow version"

**NO.** DDH's version_id is their semantic content version. Our workflow version is the definition version of a processing pipeline. Completely unrelated.

### "Dataset/Resource/Version maps to Workflow/Job/Node"

**NO.** Any similarity is convergent evolution. DDH's hierarchy describes data organization. Our DAG hierarchy describes execution flow. No mapping exists.

### "We should validate DDH's version format"

**NO.** We store it opaquely. DDH owns the format, meaning, and validation of their identifiers. We just hash them.

### "Revision is like DDH's version"

**NO.**
- DDH version: "This is the 5th release of boundary data" (content meaning)
- Our revision: "This is the 3rd time we processed DDH's v5" (operational state)

---

## STAC Traceability

STAC items store B2B identifiers in namespaced properties for traceability:

```json
{
  "id": "a7f3b2c1...",
  "properties": {
    "platform:dataset_id": "0038272",
    "platform:resource_id": "shapefile_v5",
    "platform:version_id": "2025-01",
    "platform:request_id": "req_abc123"
  }
}
```

This enables DDH to trace back from STAC catalog to their original request without us coupling to their ID scheme.

---

## Summary

| Principle | Implementation |
|-----------|----------------|
| B2B IDs are metadata, not keys | `platform_refs` JSONB field |
| Deterministic internal identity | `SHA256(platform_id\|refs)[:32]` |
| Semantic versioning is B2B's domain | Store in `platform_refs.version_id`, don't manage |
| Revision is our domain | `revision` field for operational rollback |
| Platform-agnostic | Registry pattern, no hardcoded ID structures |
