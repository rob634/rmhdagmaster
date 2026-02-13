# Layer Metadata — Full Implementation Plan

**Created**: 11 FEB 2026
**Status**: Implementation Plan — Awaiting Approval

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture Decision Record](#architecture-decision-record)
3. [Three-Database Topology](#three-database-topology)
4. [Phase 1: Model + Repository + Admin API](#phase-1-model--repository--admin-api)
5. [Phase 2: Projection + B2C Integration](#phase-2-projection--b2c-integration)
6. [Phase 3: External B2C + ADF Export](#phase-3-external-b2c--adf-export)
7. [Verification Checklist](#verification-checklist)

---

## Overview

**Problem**: The system creates COGs, PostGIS tables, and STAC catalog entries, but has zero presentation metadata — no styles, legends, colormaps, or field descriptions. The B2C service layer (rmhtitiler: TiTiler + TiPG + STAC API) serves raw tiles and features with no rendering guidance.

**Solution**: Three-layer metadata architecture:

```
dagapp.layer_metadata           ← Source of truth (admin-curated)
        │
        ├── pgstac.collections  ← STAC projection (raster + vector discovery)
        │       properties.rmh:style, rmh:legend, renders
        │
        └── dagapp.layer_catalog  ← OGC Features projection (vector metadata)
                style, legend, field_metadata keyed by table_name
```

**Principle**: Source of Truth → Projection → Consumption. Metadata is admin-curated (decoupled from ETL). Projection is an explicit "publish" action, not a side-effect of processing.

---

## Implementation Sequencing

Layer metadata sits within the broader Tier 4 implementation sequence (see `TODO.md`). The phases below map to the global ordering:

| Global Phase | Layer Metadata Phase | Dependency |
|---|---|---|
| **4A**: Asset Service (standalone) | — | — |
| **4B**: Gateway Business API | — | — |
| **4C**: DAG Callback Wiring | — | — |
| **4D**: Layer Metadata Phase 1 | **Phase 1** (this doc) | Asset tables deployed (4B) — `layer_metadata.asset_id` FK |
| **4E**: Direct Commands + Projection | **Phase 2** (this doc) | Phase 1 models + asset versions with artifacts (4C) |
| **4F**: External B2C + ADF | **Phase 3** (this doc) | Internal projection working (4E) |

**Why this order**: The gateway owns business domain writes (GeospatialAsset lifecycle). The orchestrator's direct commands handle non-business operations (metadata projection to STAC + layer_catalog). Layer metadata depends on the asset tables existing (FK), and projection is only meaningful once asset versions have artifacts from completed DAG jobs. See ARCHITECTURE.md Section 15.6 for the full responsibility split.

---

## Architecture Decision Record

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Metadata granularity | Per-asset (not per-version) | All versions of "flood_risk_2023" share one visual presentation |
| Style storage | JSONB | Raster/vector styles differ fundamentally; no single schema fits both |
| DDL ownership | PydanticToSQL for both tables | User decision: all schema is IaC, created by our DDL process |
| `layer_catalog` schema | `dagapp` (not `geo`) | Owned by our app; projected to B2C schemas during publish |
| STAC render extension | Phase 2 | Initially support only what TiTiler handles natively |
| Metadata templates | Future enhancement | Admin CRUD sufficient for v1 |
| CRUD execution | Gateway (sync Function App) | Simple single-table DB writes, immediate response |
| Publish execution | Orchestrator direct command | Multi-target writes need connection pool + pgstac; see ARCHITECTURE.md Section 15 |
| Vector STAC entries | Phase 2 | Create STAC items for vectors as discovery catalog |

**Execution model reference**: ARCHITECTURE.md Section 15 defines the two execution models (DAG Workflows vs Direct Commands) and the selection criteria. Metadata CRUD is gateway-local. Metadata projection (publish) is an orchestrator direct command.

---

## Three-Database Topology

Today: single database. Future: three databases. The architecture must work in both.

```
┌──────────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│   Application DB     │    │  Internal B2C    │    │  External B2C    │
│   (dagapp schema)    │    │  (geo + pgstac)  │    │  (geo + pgstac)  │
│                      │    │                  │    │                  │
│  geospatial_assets   │    │  PostGIS tables  │    │  PostGIS tables  │
│  asset_versions      │    │  STAC catalog    │    │  STAC catalog    │
│  layer_metadata  ◄───┼──publish──┐          │    │                  │
│  layer_catalog   ◄───┼──publish──┘          │    │                  │
│  jobs, nodes         │    │                  │    │                  │
└──────────────────────┘    └──────────────────┘    └──────────────────┘
        rmhdagmaster               rmhtitiler            external TiTiler
```

**Single-DB (today)**: `layer_catalog` lives in `dagapp` schema alongside everything else. pgstac is in the same database. Projection writes directly.

**Three-DB (future)**: Projection handler connects to the B2C database(s) via separate connection strings. `layer_catalog` is projected into the `geo` schema of each B2C database. pgstac collections/items are projected into each B2C `pgstac` schema.

**Key**: `layer_catalog` uses `table_name` as PK (not `asset_id`), so B2C consumers never need to know about dagapp internals. `source_asset_id` is traceability only, not an FK.

---

## Phase 1: Model + Repository + Admin API

**Goal**: Build the source of truth and admin CRUD. No projection yet.

### New Files

#### 1.1 `core/models/layer_metadata.py` — Source of Truth Model

Pydantic model following `geospatial_asset.py` pattern:

```python
class LayerMetadata(BaseModel):
    """
    Presentation metadata for a geospatial layer.
    Source of truth in dagapp. Projected to B2C schemas in Phase 2.
    Maps to: dagapp.layer_metadata
    """

    # SQL DDL METADATA
    __sql_table__: ClassVar[str] = "layer_metadata"
    __sql_schema__: ClassVar[str] = "dagapp"
    __sql_primary_key__: ClassVar[List[str]] = ["asset_id"]
    __sql_foreign_keys__: ClassVar[Dict[str, str]] = {
        "asset_id": "dagapp.geospatial_assets(asset_id)",
    }
    __sql_indexes__: ClassVar[List] = [
        ("idx_layer_metadata_data_type", ["data_type"]),
        ("idx_layer_metadata_sort", ["sort_order"]),
        ("idx_layer_metadata_published", ["published_at"], "published_at IS NOT NULL"),
    ]

    # Identity
    asset_id: str = Field(..., max_length=32, description="FK to geospatial_assets")
    data_type: str = Field(..., max_length=20, description="'vector' or 'raster'")

    # Display
    display_name: str = Field(..., max_length=256, description="Human-readable layer name")
    description: Optional[str] = Field(default=None, description="Markdown-capable description")
    attribution: Optional[str] = Field(default=None, max_length=500, description="Data source credit")
    keywords: List[str] = Field(default_factory=list, description="Searchable tags")

    # Style / Symbology (JSONB — shape varies by data_type)
    style: Dict[str, Any] = Field(default_factory=dict, description="Rendering parameters")
    #   Raster: {"colormap": "viridis", "rescale": [0, 255], "bidx": [1]}
    #   Vector: {"fill-color": "#3388ff", "fill-opacity": 0.5,
    #            "stroke-color": "#000", "stroke-width": 1}

    # Legend (JSONB — ordered list)
    legend: List[Dict[str, Any]] = Field(default_factory=list, description="Ordered legend entries")
    #   [{"label": "High Risk", "color": "#FF0000", "min": 80, "max": 100}]

    # Field Metadata — vectors only (JSONB — keyed by column name)
    field_metadata: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict,
        description="Per-column metadata keyed by column name",
    )
    #   {"population": {"display_name": "Population", "unit": "people", "format": "integer"}}

    # Visibility
    min_zoom: Optional[int] = Field(default=None, ge=0, le=24)
    max_zoom: Optional[int] = Field(default=None, ge=0, le=24)
    default_visible: bool = Field(default=True)
    sort_order: int = Field(default=0, description="Layer ordering in UI")

    # Thumbnail
    thumbnail_url: Optional[str] = Field(default=None, max_length=500)

    # Lifecycle
    published_at: Optional[datetime] = Field(default=None, description="Last projected to B2C")

    # Timestamps
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Optimistic locking
    version: int = Field(default=1, ge=1)

    model_config = {"frozen": False}
```

**Computed fields**: `is_published`, `has_style`, `has_legend`.

**No state machine** — CRUD entity, not lifecycle entity.

---

#### 1.2 `core/models/layer_catalog.py` — Projection Target Model

Pydantic model for the projection target. This table is the B2C-facing companion to PostGIS feature tables. Keyed by `table_name` (what B2C apps know), not `asset_id` (dagapp internal).

```python
class LayerCatalog(BaseModel):
    """
    Layer catalog entry for B2C consumption.
    Projected from dagapp.layer_metadata during publish.
    Keyed by table_name — B2C apps know tables, not dagapp internals.
    Maps to: dagapp.layer_catalog
    """

    # SQL DDL METADATA
    __sql_table__: ClassVar[str] = "layer_catalog"
    __sql_schema__: ClassVar[str] = "dagapp"
    __sql_primary_key__: ClassVar[List[str]] = ["table_name"]
    __sql_indexes__: ClassVar[List] = [
        ("idx_layer_catalog_data_type", ["data_type"]),
        ("idx_layer_catalog_source_asset", ["source_asset_id"], "source_asset_id IS NOT NULL"),
        ("idx_layer_catalog_sort", ["sort_order"]),
    ]

    # Identity — PK is table_name, NOT asset_id
    table_name: str = Field(..., max_length=128, description="PostGIS table name or STAC collection ID")
    data_type: str = Field(..., max_length=20, description="'vector' or 'raster'")

    # Display
    display_name: str = Field(..., max_length=256)
    description: Optional[str] = Field(default=None)
    attribution: Optional[str] = Field(default=None, max_length=500)
    keywords: List[str] = Field(default_factory=list)

    # Style / Legend / Field Metadata — same schema as LayerMetadata
    style: Dict[str, Any] = Field(default_factory=dict)
    legend: List[Dict[str, Any]] = Field(default_factory=list)
    field_metadata: Dict[str, Dict[str, Any]] = Field(default_factory=dict)

    # Visibility
    min_zoom: Optional[int] = Field(default=None, ge=0, le=24)
    max_zoom: Optional[int] = Field(default=None, ge=0, le=24)
    default_visible: bool = Field(default=True)
    sort_order: int = Field(default=0)

    # Thumbnail
    thumbnail_url: Optional[str] = Field(default=None, max_length=500)

    # Traceability (NOT an FK — may be different databases)
    source_asset_id: Optional[str] = Field(default=None, max_length=32)

    # Lifecycle
    published_at: Optional[datetime] = Field(default=None)
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = {"frozen": False}
```

**Why create both models in Phase 1**: DDL generation creates both tables upfront. Phase 2 writes to `layer_catalog` during publish. Having the table exist from day one avoids a migration step.

---

#### 1.3 `repositories/metadata_repo.py` — Async Repository (LayerMetadata)

Following `asset_repo.py` pattern:

| Method | Signature | Notes |
|--------|-----------|-------|
| `create` | `(metadata: LayerMetadata) -> LayerMetadata` | INSERT with `Json()` wrapping for JSONB fields |
| `get` | `(asset_id: str) -> Optional[LayerMetadata]` | SELECT with `dict_row` |
| `update` | `(metadata: LayerMetadata) -> bool` | Optimistic locking `WHERE version = %(version)s` |
| `delete` | `(asset_id: str) -> bool` | Hard delete (metadata is re-creatable) |
| `list_all` | `(limit=100) -> List[LayerMetadata]` | ORDER BY sort_order, display_name |
| `list_by_data_type` | `(data_type: str, limit=100) -> List[LayerMetadata]` | Filter by raster/vector |
| `_row_to_model` | `(row) -> LayerMetadata` | Convert DB row → model |

Key patterns:
- `dict_row` factory always
- `Json()` for style, legend, field_metadata, keywords
- `sql.SQL().format()` with `TABLE_LAYER_METADATA` identifier
- Named params `%(name)s`
- Optimistic locking: `WHERE version = %(version)s`, return `False` on conflict

---

#### 1.4 `function/repositories/metadata_query_repo.py` — Sync Gateway Repository

Extends `FunctionRepository` base class. Used by admin blueprint.

**New capability**: Write methods (INSERT/UPDATE/DELETE with commit). Other function repos are read-only; this one needs CRUD for admin operations.

```python
def execute_write(self, query: str, params: tuple = ()) -> int:
    """Execute INSERT/UPDATE/DELETE and commit. Returns rowcount."""
    with self._get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rowcount = cur.rowcount
        conn.commit()
        return rowcount
```

| Method | Notes |
|--------|-------|
| `get_metadata(asset_id)` | SELECT → dict |
| `list_metadata(data_type, limit, offset)` | List with optional filter |
| `create_metadata(data)` | INSERT with `Json()` wrapping, commit |
| `update_metadata(asset_id, data, expected_version)` | Optimistic locking UPDATE, commit |
| `delete_metadata(asset_id)` | DELETE, commit |
| `count_metadata(data_type)` | COUNT with optional filter |

---

#### 1.5 `services/metadata_service.py` — Business Logic

Thin wrapper over async repository. Used by orchestrator + Phase 2 projection handler.

| Method | Purpose |
|--------|---------|
| `create_metadata(asset_id, data_type, display_name, **kwargs)` | Validate asset exists → create |
| `get_metadata(asset_id)` | Get by asset_id |
| `update_metadata(asset_id, **updates)` | Merge updates, optimistic lock |
| `delete_metadata(asset_id)` | Delete |
| `list_metadata(data_type, limit)` | List with filter |

Validation:
- Verify `asset_id` exists in `geospatial_assets` before create
- Verify `data_type` matches asset's `data_type`
- `min_zoom <= max_zoom` if both set

---

#### 1.6 `function/blueprints/metadata_bp.py` — Admin API

Following `admin_bp.py` pattern (sync, Azure Functions Blueprint):

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `admin/layer-metadata` | GET | List all (query: `data_type`, `limit`, `offset`) |
| `admin/layer-metadata/{asset_id}` | GET | Get single |
| `admin/layer-metadata` | POST | Create (body: JSON) |
| `admin/layer-metadata/{asset_id}` | PUT | Update (body must include `version` for optimistic lock) |
| `admin/layer-metadata/{asset_id}` | DELETE | Delete |

**Error codes**: 400 (validation), 404 (not found), 409 (version conflict), 500 (server).

**Create body**:
```json
{
    "asset_id": "abc123def456abc123def456abc123de",
    "data_type": "raster",
    "display_name": "Flood Risk 2023",
    "style": {"colormap": "viridis", "rescale": [0, 100]},
    "legend": [{"label": "High Risk", "color": "#FF0000", "min": 80, "max": 100}],
    "keywords": ["flood", "hazard"]
}
```

**Update body** (partial — only supplied fields change, `version` required):
```json
{
    "version": 1,
    "style": {"colormap": "magma", "rescale": [0, 255]}
}
```

---

#### 1.7 `tests/test_layer_metadata.py` — Unit Tests

| Test Class | Tests |
|------------|-------|
| `TestLayerMetadataModel` | Creation with defaults, field validation (max_length, zoom range), computed fields, `model_dump()` serialization, JSONB field types |
| `TestLayerMetadataRaster` | Raster style/legend examples, colormap fields |
| `TestLayerMetadataVector` | Vector style examples, field_metadata structure |
| `TestLayerCatalogModel` | Creation, table_name PK, source_asset_id traceability |
| `TestSchemaGeneration` | `generate_all()` includes `layer_metadata` AND `layer_catalog` tables |

Estimated: ~25-30 tests.

---

### Modified Files (Phase 1)

| # | File | Change |
|---|------|--------|
| 1.8 | `core/models/__init__.py` | Export `LayerMetadata`, `LayerCatalog` |
| 1.9 | `repositories/database.py` | Add `TABLE_LAYER_METADATA`, `TABLE_LAYER_CATALOG` constants |
| 1.10 | `repositories/__init__.py` | Export `LayerMetadataRepository` |
| 1.11 | `core/schema/sql_generator.py` | Register both models in `generate_all()` (table + indexes + triggers) |
| 1.12 | `function/blueprints/__init__.py` | Export `metadata_bp` |
| 1.13 | `function_app.py` | Register `metadata_bp` in startup block |
| 1.14 | `handlers/raster/stac.py` | Accept optional `display_name`, `description`, `keywords` in `stac_ensure_collection()` (light touch) |

### Phase 1 Implementation Order

| Step | File(s) | Depends On | Est. Lines |
|------|---------|------------|------------|
| 1 | `core/models/layer_metadata.py` | — | ~110 |
| 2 | `core/models/layer_catalog.py` | — | ~80 |
| 3 | `core/models/__init__.py` | Steps 1-2 | ~5 |
| 4 | `repositories/database.py` | — | ~2 |
| 5 | `repositories/metadata_repo.py` | Steps 1, 4 | ~180 |
| 6 | `repositories/__init__.py` | Step 5 | ~3 |
| 7 | `core/schema/sql_generator.py` | Steps 1-2 | ~15 |
| 8 | `services/metadata_service.py` | Steps 1, 5 | ~100 |
| 9 | `function/repositories/metadata_query_repo.py` | Step 1 | ~180 |
| 10 | `function/blueprints/metadata_bp.py` | Step 9 | ~220 |
| 11 | `function/blueprints/__init__.py` | Step 10 | ~3 |
| 12 | `function_app.py` | Step 10 | ~4 |
| 13 | `handlers/raster/stac.py` | — | ~10 |
| 14 | `tests/test_layer_metadata.py` | Steps 1-7 | ~250 |
| 15 | Run `pytest tests/ -v` | All | — |

**Phase 1 total**: ~9 new files, ~6 modified files, ~1,160 lines.

---

## Phase 2: Projection + B2C Integration

**Goal**: Publish metadata from source of truth into B2C-consumable formats. Admin triggers "publish" → metadata appears in STAC + layer_catalog.

**Prerequisite**: Phase 1 complete + at least one asset with layer_metadata + at least one processed version with STAC collection/item or PostGIS table.

### New Files

#### 2.1 `services/metadata_projection_service.py` — Projection Engine

Core service that reads `layer_metadata` and writes to STAC + `layer_catalog`.

```python
class MetadataProjectionService:
    """Projects dagapp.layer_metadata → B2C targets."""

    def __init__(self, pool: AsyncConnectionPool):
        self._metadata_repo = LayerMetadataRepository(pool)
        self._catalog_repo = LayerCatalogRepository(pool)
        self._pgstac = PgStacRepository()

    async def publish(self, asset_id: str) -> ProjectionResult:
        """
        Project metadata for one asset to all B2C targets.

        Steps:
        1. Read layer_metadata for asset_id
        2. Read asset_version to find stac_collection_id + table_name
        3. Project to STAC collection properties (raster + vector)
        4. Project to layer_catalog (vector + raster discovery)
        5. Update published_at timestamp
        """

    async def unpublish(self, asset_id: str) -> bool:
        """Remove projections (delete layer_catalog row, strip STAC metadata)."""

    async def publish_all(self) -> List[ProjectionResult]:
        """Bulk publish all layer_metadata entries."""
```

**STAC projection** (raster):
```python
def _project_to_stac_collection(self, metadata: LayerMetadata, collection_id: str):
    """Merge presentation metadata into STAC collection properties."""
    update = {
        "title": metadata.display_name,
        "description": metadata.description,
    }
    if metadata.keywords:
        update["keywords"] = metadata.keywords

    # Style → collection-level summaries (TiTiler-native params only for now)
    if metadata.style:
        summaries = {}
        if "colormap" in metadata.style:
            summaries["rmh:colormap"] = metadata.style["colormap"]
        if "rescale" in metadata.style:
            summaries["rmh:rescale"] = metadata.style["rescale"]
        update["summaries"] = summaries

    # Legend → custom namespace
    if metadata.legend:
        update.setdefault("summaries", {})["rmh:legend"] = metadata.legend

    # Use pgstac update_collection_metadata (merges into existing)
    self._pgstac.update_collection_metadata(collection_id, update)
```

**Layer catalog projection** (vector + raster):
```python
def _project_to_layer_catalog(self, metadata: LayerMetadata, table_name: str):
    """Write/update layer_catalog row from layer_metadata."""
    catalog_entry = LayerCatalog(
        table_name=table_name,
        data_type=metadata.data_type,
        display_name=metadata.display_name,
        description=metadata.description,
        attribution=metadata.attribution,
        keywords=metadata.keywords,
        style=metadata.style,
        legend=metadata.legend,
        field_metadata=metadata.field_metadata,
        min_zoom=metadata.min_zoom,
        max_zoom=metadata.max_zoom,
        default_visible=metadata.default_visible,
        sort_order=metadata.sort_order,
        thumbnail_url=metadata.thumbnail_url,
        source_asset_id=metadata.asset_id,
        published_at=datetime.now(timezone.utc),
    )
    # UPSERT by table_name
    await self._catalog_repo.upsert(catalog_entry)
```

---

#### 2.2 `repositories/catalog_repo.py` — Async Repository (LayerCatalog)

| Method | Notes |
|--------|-------|
| `upsert(entry: LayerCatalog) -> LayerCatalog` | INSERT ... ON CONFLICT (table_name) DO UPDATE |
| `get(table_name: str) -> Optional[LayerCatalog]` | SELECT by table_name |
| `delete(table_name: str) -> bool` | DELETE by table_name |
| `list_all(data_type, limit) -> List[LayerCatalog]` | List with filter |
| `get_by_source_asset(asset_id: str) -> List[LayerCatalog]` | Reverse lookup |
| `_row_to_model(row) -> LayerCatalog` | Convert |

---

#### 2.3 `core/models/projection.py` — Projection Result Model

```python
class ProjectionResult(BaseModel):
    """Result of metadata projection operation."""
    asset_id: str
    stac_projected: bool = False
    stac_collection_id: Optional[str] = None
    catalog_projected: bool = False
    catalog_table_name: Optional[str] = None
    errors: List[str] = Field(default_factory=list)
    published_at: Optional[datetime] = None
```

---

#### 2.4 `api/command_routes.py` — Orchestrator Command Endpoints (new)

New FastAPI router for direct commands (ARCHITECTURE.md Section 15). Mounted at `/api/v1/commands/` on the orchestrator.

```python
router = APIRouter(prefix="/commands", tags=["Commands"])

@router.post("/metadata/publish/{asset_id}")
async def publish_metadata(asset_id: str) -> ProjectionResult:
    """Project layer_metadata → STAC + layer_catalog."""

@router.post("/metadata/unpublish/{asset_id}")
async def unpublish_metadata(asset_id: str) -> dict:
    """Remove projections from STAC + layer_catalog."""
```

**Wiring**: Registered in `main.py` alongside the existing `api.routes` router. Receives `MetadataProjectionService` via the same dependency injection pattern (`set_services()`).

**Gateway access**: Admin calls `POST /api/proxy/orchestrator/api/v1/commands/metadata/publish/{asset_id}` via the existing `proxy_bp` — no new gateway code needed for publish.

**Read-only catalog endpoints** (gateway-local, extend `metadata_bp`):

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `admin/layer-catalog` | GET | List all catalog entries |
| `admin/layer-catalog/{table_name}` | GET | Get single catalog entry |

---

#### 2.5 STAC for Vectors — Discovery Catalog

Create STAC collection entries for vector datasets:

```json
{
  "id": "parcels_2024",
  "type": "Collection",
  "title": "Land Parcels 2024",
  "description": "From layer_metadata.description",
  "keywords": ["parcels", "land"],
  "links": [
    {"rel": "ogc-features", "href": "https://titiler.../vector/collections/geo.parcels_2024"}
  ],
  "summaries": {
    "rmh:style": {"fill-color": "#3388ff"},
    "rmh:legend": [...]
  }
}
```

This makes vectors discoverable via STAC search alongside rasters. TiPG still serves the actual data — STAC is just the catalog.

---

#### 2.6 `tests/test_metadata_projection.py` — Projection Tests

| Test Class | Tests |
|------------|-------|
| `TestSTACProjection` | Merges style/legend into collection properties, handles missing collection, preserves existing properties |
| `TestLayerCatalogProjection` | Upsert creates new row, upsert updates existing, maps all fields correctly |
| `TestPublishEndpoint` | Full publish lifecycle, unpublish cleans up, publish with no STAC collection (vector-only) |
| `TestProjectionResult` | Success/error accumulation, partial projection (STAC ok, catalog fails) |

Estimated: ~20-25 tests.

---

### Modified Files (Phase 2)

| # | File | Change |
|---|------|--------|
| 2.7 | `main.py` | Register `command_routes` router at `/api/v1/commands/`, inject `MetadataProjectionService` via `set_services()` |
| 2.8 | `repositories/__init__.py` | Export `LayerCatalogRepository` |
| 2.9 | `repositories/database.py` | Already has `TABLE_LAYER_CATALOG` from Phase 1 |
| 2.10 | `services/__init__.py` | Export `MetadataProjectionService` |
| 2.11 | `function/blueprints/metadata_bp.py` | Add read-only catalog list/get endpoints |
| 2.12 | `handlers/raster/stac.py` | Optionally read `layer_metadata` during collection creation to pre-populate title/description/keywords |

### Phase 2 Implementation Order

| Step | File(s) | Depends On | Est. Lines |
|------|---------|------------|------------|
| 1 | `repositories/catalog_repo.py` | Phase 1 | ~150 |
| 2 | `core/models/projection.py` | — | ~30 |
| 3 | `services/metadata_projection_service.py` | Steps 1-2 | ~200 |
| 4 | `api/command_routes.py` (new) | Step 3 | ~80 |
| 5 | `main.py` (wire command router) | Step 4 | ~15 |
| 6 | `function/blueprints/metadata_bp.py` (catalog reads) | Step 1 | ~60 |
| 7 | `handlers/raster/stac.py` (enhance) | Phase 1 | ~30 |
| 8 | `repositories/__init__.py`, `services/__init__.py` | Steps 1, 3 | ~6 |
| 9 | `tests/test_metadata_projection.py` | Steps 1-5 | ~250 |
| 10 | E2E test in Azure | All | — |

**Phase 2 total**: ~5 new files, ~5 modified files, ~820 lines.

---

### Phase 2 — rmhtitiler Enhancement (Separate Repo)

Not implemented in rmhdagmaster, but designed here for reference.

**Goal**: TiPG (OGC Features) returns metadata alongside vector data.

**Option A (recommended)**: Enrich TiPG collection response.

```python
# rmhtitiler/geotiler/middleware/metadata.py
#
# Middleware that intercepts TiPG /vector/collections/{id} responses
# and joins dagapp.layer_catalog (or geo.layer_catalog in 3-DB mode)
# into the response body.
#
# Before:
#   {"id": "parcels_2024", "title": "parcels_2024", "links": [...]}
#
# After:
#   {"id": "parcels_2024", "title": "Land Parcels 2024",
#    "rmh:style": {...}, "rmh:legend": [...], "rmh:field_metadata": {...}}
```

**Connection**: rmhtitiler reads from `dagapp.layer_catalog` (single-DB) or `geo.layer_catalog` (three-DB). The table structure is identical.

---

## Phase 3: External B2C + ADF Export

**Goal**: When clearance → PUBLIC, ADF export copies metadata alongside data to external B2C.

**Prerequisite**: Phase 2 complete + ADF export pipeline operational.

### 3.1 ADF Export Pipeline Enhancement

When ADF copies data to external B2C:

```
ADF Pipeline copies:
├── COGs                        → external blob storage
├── PostGIS tables (geo.*)      → external database
├── dagapp.layer_catalog rows   → external geo.layer_catalog  ← NEW
├── pgstac items/collections    → external pgstac schema
│   (already carries metadata in properties from Phase 2)
└── (No changes needed for STAC — metadata is already in properties)
```

**For `layer_catalog`**: ADF SELECT from `dagapp.layer_catalog WHERE source_asset_id = ?` → INSERT into external `geo.layer_catalog`. Same schema, different database.

**For pgstac**: No additional work. Phase 2 already embedded metadata into STAC collection/item `properties`. ADF copies pgstac as-is.

### 3.2 External Projection Handler (Optional)

If external B2C has a separate `geo.layer_catalog` table (three-DB mode), the projection service needs a second connection string:

```python
class ExternalProjectionService:
    """Project metadata to external B2C database."""

    def __init__(self, external_conn_string: str):
        self._external_db = PostgreSQLRepository(
            connection_string=external_conn_string,
            schema_name="geo",
        )

    def project_catalog_entry(self, entry: LayerCatalog):
        """Write layer_catalog row to external geo schema."""
        # Same SQL as internal projection, different connection
```

**Config**: `EXTERNAL_B2C_CONNECTION_STRING` env var. Only needed in three-DB mode.

### 3.3 `geo.layer_catalog` Table (External Schema)

In three-DB mode, the external database has `geo.layer_catalog` (not `dagapp.layer_catalog`). The DDL is identical except for schema name. Created by:
- rmhdagmaster's bootstrap endpoint (single-DB mode) → `dagapp.layer_catalog`
- ADF pipeline setup (three-DB mode) → `geo.layer_catalog`

### Modified Files (Phase 3)

| # | File | Change |
|---|------|--------|
| 3.4 | `services/metadata_projection_service.py` | Add `publish_external()` method |
| 3.5 | `function/blueprints/metadata_bp.py` | Add `admin/layer-metadata/{asset_id}/publish-external` endpoint |
| 3.6 | ADF pipeline definition (outside rmhdagmaster) | Add layer_catalog copy step |

### Phase 3 Implementation Order

| Step | File(s) | Depends On | Est. Lines |
|------|---------|------------|------------|
| 1 | `services/metadata_projection_service.py` (extend) | Phase 2 | ~60 |
| 2 | `function/blueprints/metadata_bp.py` (extend) | Step 1 | ~40 |
| 3 | ADF pipeline changes | External | — |
| 4 | E2E: internal → publish → ADF → external → verify | All | — |

**Phase 3 total**: ~100 lines in rmhdagmaster + ADF pipeline changes.

---

## Metadata Lifecycle Summary

```
┌─────────────────────────────────────────────────────────────────────────┐
│  1. Admin creates/updates metadata (Gateway — sync)                      │
│     POST /api/admin/layer-metadata                                       │
│     → gateway writes dagapp.layer_metadata directly                      │
│                                                                          │
│  2. Admin publishes to B2C (Orchestrator — direct command)               │
│     POST /api/proxy/orchestrator/api/v1/commands/metadata/publish/{id}   │
│     → gateway proxies to orchestrator                                    │
│     → orchestrator reads dagapp.layer_metadata                           │
│     → orchestrator writes STAC collection properties                     │
│     → orchestrator writes dagapp.layer_catalog                           │
│     → orchestrator updates published_at + logs audit event               │
│     → returns ProjectionResult synchronously                             │
│                                                                          │
│  3. B2C serves data with metadata                                        │
│     STAC API: collection with title, description, summaries              │
│     TiTiler: uses colormap/rescale from STAC properties                  │
│     TiPG: enriched collection response (via rmhtitiler middleware)        │
│                                                                          │
│  4. On clearance → PUBLIC (Phase 3)                                      │
│     ADF copies layer_catalog + pgstac to external B2C                    │
│     External consumers get identical metadata                            │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Complete File Inventory

### Phase 1 — New Files
| File | Purpose |
|------|---------|
| `core/models/layer_metadata.py` | Source of truth model |
| `core/models/layer_catalog.py` | Projection target model |
| `repositories/metadata_repo.py` | Async CRUD for layer_metadata |
| `function/repositories/metadata_query_repo.py` | Sync gateway CRUD |
| `services/metadata_service.py` | Business logic + validation |
| `function/blueprints/metadata_bp.py` | Admin API endpoints |
| `tests/test_layer_metadata.py` | Unit tests (~25-30) |

### Phase 1 — Modified Files
| File | Change |
|------|--------|
| `core/models/__init__.py` | Export LayerMetadata, LayerCatalog |
| `repositories/database.py` | TABLE_LAYER_METADATA, TABLE_LAYER_CATALOG |
| `repositories/__init__.py` | Export LayerMetadataRepository |
| `core/schema/sql_generator.py` | Register both models for DDL |
| `function/blueprints/__init__.py` | Export metadata_bp |
| `function_app.py` | Register metadata_bp |
| `handlers/raster/stac.py` | Accept optional display_name, description, keywords |

### Phase 2 — New Files
| File | Purpose |
|------|---------|
| `api/command_routes.py` | Orchestrator direct command router (`/api/v1/commands/`) |
| `repositories/catalog_repo.py` | Async UPSERT for layer_catalog |
| `core/models/projection.py` | ProjectionResult model |
| `services/metadata_projection_service.py` | Projection engine |
| `tests/test_metadata_projection.py` | Projection tests (~20-25) |

### Phase 2 — Modified Files
| File | Change |
|------|--------|
| `main.py` | Register command_routes router, inject MetadataProjectionService |
| `function/blueprints/metadata_bp.py` | Add read-only catalog list/get endpoints |
| `repositories/__init__.py` | Export LayerCatalogRepository |
| `services/__init__.py` | Export MetadataProjectionService |
| `handlers/raster/stac.py` | Read layer_metadata during collection creation |

### Phase 3 — Modified Files
| File | Change |
|------|--------|
| `services/metadata_projection_service.py` | Add publish_external() |
| `api/command_routes.py` | Add publish-external command endpoint |
| ADF pipeline (external) | Add layer_catalog copy step |

---

## Verification Checklist

### Phase 1
- [ ] `pytest tests/ -v` — all existing 134 + new ~25-30 tests pass
- [ ] Schema generation includes `layer_metadata` AND `layer_catalog` tables
- [ ] Model validates: max_length, zoom range, required fields
- [ ] JSONB fields serialize correctly via `model_dump()`
- [ ] Optimistic locking: version conflict returns 409 from API
- [ ] Admin API: full CRUD lifecycle (create → get → update → list → delete)
- [ ] No circular imports

### Phase 2
- [ ] Command router registered at `/api/v1/commands/` on orchestrator
- [ ] `POST /api/v1/commands/metadata/publish/{asset_id}` returns `ProjectionResult`
- [ ] Publish writes STAC collection properties (title, description, summaries)
- [ ] Publish writes layer_catalog row (style, legend, field_metadata)
- [ ] Unpublish removes layer_catalog row, strips STAC metadata
- [ ] Re-publish updates existing entries (idempotent)
- [ ] published_at timestamp updated on success
- [ ] Single audit event logged per publish via `EventService`
- [ ] Partial failure: STAC ok + catalog fails → error reported, STAC not rolled back
- [ ] Gateway proxy route works: `POST /api/proxy/orchestrator/api/v1/commands/metadata/publish/{asset_id}`
- [ ] E2E: create metadata (gateway) → publish (orchestrator command) → verify STAC + layer_catalog

### Phase 3
- [ ] ADF copies layer_catalog rows to external database
- [ ] External STAC already has metadata (from Phase 2 pgstac copy)
- [ ] External TiTiler/TiPG reads projected metadata
- [ ] Clearance → PUBLIC triggers export including metadata

---

## Open Design Notes

1. **`rmh:` namespace prefix**: STAC properties use `rmh:style`, `rmh:legend`, `rmh:colormap` to avoid collision with standard STAC fields. This follows the STAC convention for vendor extensions.

2. **STAC render extension**: Phase 2 starts with `rmh:` custom properties in `summaries`. When we adopt the STAC render extension, we add a `renders` object to the collection — this is additive, not breaking. TiTiler will auto-apply render parameters from this extension.

3. **Per-version metadata overrides**: Current design is per-asset. If future need arises, add optional `version_ordinal` to `layer_metadata` PK as a composite key `(asset_id, version_ordinal)`. Default `version_ordinal = NULL` means "all versions". This is a schema migration, not an architecture change.

4. **Metadata change auditing**: Not in scope. If needed later, the existing `JobEvent` system could be extended to log metadata changes, or a separate `layer_metadata_audit` table.
