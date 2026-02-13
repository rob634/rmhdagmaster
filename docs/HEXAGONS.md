# H3 Hexagonal Aggregation Pipeline — Design Document

**Author**: Robert Harrison
**Audience**: Claude(s), engineering
**Created**: 12 FEB 2026
**Status**: Design — high level architecture

---

## 1. Goal

Build an H3 levels 2–7 zonal statistics pipeline that aggregates every raster we can source (Planetary Computer first, then our own hosted rasters). Outputs are Parquet files sorted on H3 index, queryable by columnar engines (DuckDB, Polars) and visualizable by front-end H3 libraries (already in place).

**Core statistics**: mean, sum, median, stdev per H3 cell per raster.

**Geographic scope**: Land and littoral waters only. A land geometry input filters out ~70% of the global H3 grid (open ocean cells are discarded before processing).

---

## 2. Design Decisions

### 2.1 H3 Grid: No Bootstrap — Generate On the Fly

**Decision**: Do NOT pre-build H3 geometries in PostGIS. Generate cells on the fly using `h3-py`.

**Rationale**:
- `h3.cell_to_boundary()` returns a polygon in microseconds — the library *is* the grid
- `h3.cell_to_children(cell, res)` gives the hierarchy deterministically — it's a function, not data
- H3 level 7 has ~98 million cells globally; even land-filtered that's ~30 million rows in PostGIS for zero analytical value
- No spatial joins needed — zonal stats use raster-vector intersection (`exactextract` / `rasterstats`), not PostGIS `ST_Intersects`
- The only spatial query needed is "which L3 cells intersect this raster's bounding box?" — `h3.polygon_to_cells(bbox, res=3)` handles this without a database

**Land filter**: See Section 2.6 — Static L3 Land Cell List.

**H3 aperture-7 hierarchy** (each hex cell has exactly 7 children):

| From L3 | Descendants | Calculation |
|---------|-------------|-------------|
| → L4 | 7 | 7^1 |
| → L5 | 49 | 7^2 |
| → L6 | 343 | 7^3 |
| → L7 | 2,401 | 7^4 |
| **Total (L3–L7)** | **~2,800** | 1 + 7 + 49 + 343 + 2,401 |

### 2.2 Geodesic Distance Everywhere

**Decision**: All distance and area calculations use geodesic math on WGS84. No projected CRS anywhere in the pipeline.

**Rationale**:
- Eliminates UTM zone selection — H3 cells span the globe, no single projection works
- No distortion at zone boundaries (UTM 31N/32N seam, polar regions)
- No reprojection between datasets — everything stays in EPSG:4326
- `pyproj.Geod` (WGS84 ellipsoid) computes geodesic distance and area to millimeter precision
- `geod.geometry_length(line)` → meters, `geod.geometry_area(polygon)` → m², works globally

```python
from pyproj import Geod

geod = Geod(ellps="WGS84")

# Road length in meters — geodesic, no projection needed
length_m = geod.geometry_length(road_linestring)

# Building area in m² — geodesic, accurate at any latitude
area_m2 = abs(geod.geometry_area(building_polygon))

# Distance between cell centroids — geodesic
_, _, dist_m = geod.inv(lon1, lat1, lon2, lat2)
```

This applies to: road length aggregation (Section 10), polygon area aggregation (Section 10), connectivity edge distances (Section 11), and any future measurement. One distance function, global coverage, no CRS management.

### 2.3 Raster Access: Hybrid Strategy

**Own rasters** (Azure Blob Storage): Download via SAS URL (existing `_resolve_blob_path()` pattern from raster_validate handler).

**Planetary Computer**: COG streaming via windowed reads for manageable rasters. Download-first for very large rasters (threshold TBD — likely based on file size or pixel count). STAC API for catalog discovery.

Workers handle both paths behind a unified interface — the DAG task params specify the access method.

### 2.4 Update Model: Batch + Manual

Not event-driven. Analyst triggers a batch run for a raster catalog (or single raster). No automatic reprocessing when new data appears. This keeps the system simple and predictable.

### 2.5 No Database for H3 Stats — Parquet is the Only Data Layer

**Decision**: No OLTP database for computed statistics. Parquet files on blob storage are the sole data layer.

**Rationale**:
- Pure OLAP workload — no concurrent transactional writes, no point updates
- Data is computed, not entered — source rasters are the true source of truth
- PostgreSQL hard limit of ~1,600 columns; thousands of rasters × stats × temporal periods exceeds this
- Columnar queries (DuckDB) read parquet directly — no ETL step from DB to parquet
- Immutable files = simple, no corruption risk, trivially cacheable

**What does live in the database**: The computation catalog (Section 5). Three small tables defining *what* to compute, *what was computed*, and *what files exist*. Recipe book in PostgreSQL, output on blob storage.

### 2.6 Static L3 Land Cell List

**Decision**: Pre-compute the set of L3 cell IDs that cover land and littoral waters. Store as a static artifact (checked into the repo or hosted on blob storage). No runtime geometry intersection.

**Rationale**:
- The land/ocean boundary doesn't change between runs — computing it dynamically at job start is wasted work
- A static list of ~12,000–15,000 L3 cell IDs is trivial to store (~200 KB as a text file or JSON array)
- Eliminates the `land_filter` node entirely — the `discover` node intersects the raster's bounding box against the static set using `h3.polygon_to_cells(bbox, res=3)` ∩ `LAND_L3_CELLS`
- One fewer DAG node = simpler workflow, faster job start, fewer failure modes
- Updating the list (e.g., adding coastal reclamation) is an explicit, versioned change — not a silent runtime recomputation

**Generation** (one-time):
1. Load land + littoral geometry (Natural Earth 10m coastline or equivalent)
2. `h3.polygon_to_cells(land_geometry, res=3)` → set of L3 cell IDs
3. Save to `data/h3_land_l3_cells.json` (or equivalent)
4. Commit to repo or upload to blob storage

**Usage in workflow**: The `discover` node loads the static list, computes the raster's L3 coverage via `h3.polygon_to_cells(bbox, res=3)`, and emits the intersection as the fan-out input. No `land_filter` node needed.

---

## 3. DAG Workflow Design

### 3.1 Core Zonal Stats Workflow

```
┌─────────────┐
│  discover   │  Resolve raster → bbox, CRS, band info
│             │  Intersect bbox L3 cells with static land set → fan-out list
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────────────────┐
│  fan_out: one task per L3 cell                   │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐        │
│  │ zonal_L3 │ │ zonal_L3 │ │ zonal_L3 │  ...   │
│  │ cell_001 │ │ cell_002 │ │ cell_003 │        │
│  └──────────┘ └──────────┘ └──────────┘        │
└──────────────────────┬──────────────────────────┘
                       │
                       ▼
                ┌─────────────┐
                │   fan_in    │  Collect parquet paths → write manifest
                └─────────────┘
```

### 3.2 Zonal Stats Task (per L3 cell)

Each fan-out task receives:
- **raster_source**: STAC URL or blob path
- **h3_l3_cell**: The L3 cell ID to process
- **stats**: `[mean, sum, median, stdev]`
- **levels**: `[3, 4, 5, 6, 7]`

Processing steps:
1. Generate all descendant H3 cells for levels 3–7 (~2,800 cells per L3 chunk)
2. Convert each cell to polygon geometry via `h3.cell_to_boundary()`
3. Run zonal stats against the raster for each cell polygon (`exactextract` preferred — fast, handles partial pixel coverage)
4. Assemble results into a DataFrame: `h3_index | resolution | mean | sum | median | stdev`
5. Write Parquet sorted by `h3_index` to output blob storage

### 3.3 Scale Estimates

| Metric | Value |
|--------|-------|
| L3 cells globally | ~41,000 |
| L3 cells (land-filtered) | ~12,000–15,000 |
| L3 cells per typical raster | 50–500 (depends on extent) |
| H3 cells per L3 chunk (L3–L7) | ~2,800 |
| Task duration (estimate) | 10–60s per chunk depending on raster size |
| 4 workers × 500 chunks | ~125 chunks/worker, ~30–120 min total |

### 3.4 Chunking Level: L3

**L3 is the sweet spot.**

| Chunk Level | Cells per chunk (to L7) | Fan-out tasks (typical raster) | Task granularity |
|-------------|------------------------|-------------------------------|------------------|
| L2 | ~19,600 | 5–20 | Too fat — poor load balance |
| **L3** | **~2,800** | **50–500** | **Right size — good parallelism, amortizes overhead** |
| L4 | ~400 | 350–3,500 | Too many tiny tasks — overhead > compute |

### 3.5 Worker Optimization

Workers are Docker containers (osgeo/gdal base). Key optimizations:
- **Windowed raster reads**: For COGs, read only the window covering the L3 cell's bbox — don't load the full raster
- **Batch cell extraction**: Generate all 2,800 polygons upfront, run zonal stats as a batch operation (not one cell at a time)
- **Memory ceiling**: L3 chunk × raster window is small (~tens of MB) — no memory pressure

With 4 instances processing 500 tasks: tasks are independent with no coordination needed between workers. The existing Service Bus competing-consumer pattern handles load distribution automatically.

---

## 4. Output Storage

### 4.1 Parquet Layout

Per-raster, per-period, partitioned by L3 chunk:

```
h3_stats/
  raster={raster_id}/
    period={period}/                 -- "2025-01", "2024", or "static"
      resolution=7/
        h3_l3={cell_id}.parquet      -- ~2,401 rows × 6 columns
      resolution=6/
        h3_l3={cell_id}.parquet      -- ~343 rows × 6 columns
      ...
```

Each Parquet file contains:

| Column | Type | Description |
|--------|------|-------------|
| `h3_index` | string | H3 cell ID (sort key) |
| `resolution` | int8 | H3 level (3–7) |
| `mean` | float64 | Zonal mean |
| `sum` | float64 | Zonal sum |
| `median` | float64 | Zonal median |
| `stdev` | float64 | Zonal stdev |

### 4.2 Query Patterns (DuckDB)

**Single raster, single period:**
```sql
SELECT h3_index, mean
FROM 'h3_stats/raster=flood_30m/period=static/resolution=7/*.parquet'
WHERE h3_index LIKE '87280e%'
```

**Multi-raster join:**
```sql
SELECT a.h3_index, a.mean AS flood_mean, b.mean AS pop_mean
FROM 'h3_stats/raster=flood_30m/period=static/resolution=7/*.parquet' a
JOIN 'h3_stats/raster=worldpop/period=2024/resolution=7/*.parquet' b
  USING (h3_index)
```

**Temporal comparison:**
```sql
SELECT h3_index,
       a.mean AS flood_jan, b.mean AS flood_feb
FROM 'h3_stats/raster=flood_30m/period=2025-01/resolution=7/*.parquet' a
JOIN 'h3_stats/raster=flood_30m/period=2025-02/resolution=7/*.parquet' b
  USING (h3_index)
```

Per-chunk files are small (~50–100 KB). DuckDB scans glob patterns efficiently. **Compaction is a top priority** — merging per-chunk files into larger regional or global parquet files is essential for production query performance and front-end serving. The parquet schema (columns, types, sort order) will be known ahead of time: a thorough inventory of available data sources, feature engineering decisions, and column mapping will be completed before compute begins. This means file dimensions are close to final from the first run — no ad-hoc column accumulation.

### 4.3 Materialized Wide Views (Analyst-Defined)

If an analyst regularly queries a specific combination of rasters, they can materialize a wide view as a one-off DuckDB operation:

```sql
COPY (
  SELECT a.h3_index,
         a.mean AS flood_mean, a.stdev AS flood_stdev,
         b.mean AS pop_mean,
         c.mean AS elevation_mean
  FROM 'h3_stats/raster=flood_30m/...' a
  JOIN 'h3_stats/raster=worldpop/...' b USING (h3_index)
  JOIN 'h3_stats/raster=dem/...' c USING (h3_index)
) TO 'h3_views/flood_pop_elevation_l7.parquet'
```

This is a **query artifact**, not infrastructure. The source-of-truth remains the per-raster parquet files.

---

## 5. Computation Catalog (Database Schema)

The database defines **what** gets computed. Parquet files are deterministic outputs of `source + config parameters`. Nothing gets computed that isn't explicitly registered.

**Database = recipe book. Parquet = output.**

Two source tables (raster and vector) share a common run/manifest infrastructure.

### 5.1 Schema: `dagapp.h3_raster_sources`

Registry of raster data sources and their processing configuration.

```sql
CREATE TABLE dagapp.h3_raster_sources (
    source_id        TEXT PRIMARY KEY,
    name             TEXT NOT NULL,           -- "FATHOM Flood 30m"
    description      TEXT,
    source_type      TEXT NOT NULL,           -- blob | planetary_computer | url
    source_uri       TEXT NOT NULL,           -- blob path, STAC collection ID, URL
    bands            INT[],                   -- which bands to process [1] or [1,2,3,4]
    stats            TEXT[] NOT NULL,         -- [mean, sum, median, stdev]
    h3_levels        INT[] NOT NULL,          -- [3,4,5,6,7]
    temporal_mode    TEXT NOT NULL DEFAULT 'static',  -- static | monthly | annual
    land_geometry_id TEXT,                    -- which land mask to apply
    active           BOOLEAN NOT NULL DEFAULT true,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    version          INT NOT NULL DEFAULT 1   -- optimistic locking
);
```

### 5.2 Schema: `dagapp.h3_vector_sources`

Registry of vector data sources. See Section 10.5 for full schema and examples.

Separate from raster sources because the config fields are fundamentally different (geometry_op, category_column, aggregations vs bands, stats).

### 5.3 Schema: `dagapp.h3_computation_runs`

Record of each batch computation and its frozen config. Shared by raster and vector runs.

```sql
CREATE TABLE dagapp.h3_computation_runs (
    run_id           TEXT PRIMARY KEY,
    source_id        TEXT NOT NULL,           -- references h3_raster_sources OR h3_vector_sources
    source_type      TEXT NOT NULL,           -- 'raster' | 'vector'
    dag_job_id       TEXT,                    -- FK to dag_jobs (nullable until job created)
    config_snapshot  JSONB NOT NULL,          -- frozen copy of source row at trigger time
    period           TEXT,                    -- "2025-01", "2024", NULL for static
    status           TEXT NOT NULL DEFAULT 'pending',  -- pending | running | completed | failed
    output_prefix    TEXT,                    -- h3_stats/source=overture_roads/period=static/
    chunk_count      INT,
    cell_count       INT,
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    error_message    TEXT
);
```

`config_snapshot` is frozen at trigger time — same pattern as `workflow_snapshot` on `dag_jobs`. If someone changes the source config, previous runs are unaffected. `source_type` discriminates which source table the `source_id` references.

### 5.4 Schema: `dagapp.h3_output_manifest`

What files were produced by a run.

```sql
CREATE TABLE dagapp.h3_output_manifest (
    run_id           TEXT NOT NULL REFERENCES dagapp.h3_computation_runs(run_id),
    h3_l3_cell       TEXT NOT NULL,           -- chunk cell ID
    resolution       INT NOT NULL,
    blob_path        TEXT NOT NULL,
    row_count        INT,
    file_size_bytes  BIGINT,
    PRIMARY KEY (run_id, h3_l3_cell, resolution)
);
```

### 5.5 The Flow

```
1. Analyst registers source            →  INSERT into h3_raster_sources or h3_vector_sources
2. Analyst triggers computation run    →  Freeze config_snapshot, submit DAG job
3. DAG discover (+ static land filter) →  Determine L3 chunks
4. DAG fan-out produces parquet        →  Workers write to output_prefix
5. Fan-in writes manifest              →  INSERT into h3_output_manifest
6. Run marked complete                 →  UPDATE h3_computation_runs SET status = 'completed'
```

**Deterministic**: Given the same `source_id` + `config_snapshot` + `period`, the output parquet files are identical. Rerunning overwrites the same paths.

**Auditable**: "Why does this parquet file exist?" → `manifest → run → config_snapshot`. Always answerable.

**Staleness detection**: Source config changed since last run? Compare source table `updated_at` vs `config_snapshot`. Source registered but never computed? `WHERE source_id NOT IN (SELECT source_id FROM h3_computation_runs WHERE status = 'completed')`.

---

## 6. Complex Aggregation Workflows

This is where the DAG node structure earns its keep. Multi-step analytical workflows compose from reusable task types.

### 6.1 Example: Flood Risk Weighted by Building Count

```yaml
# workflow: flood_building_risk.yaml
nodes:
  - id: flood_zonal
    task_type: h3_zonal_stats
    params:
      raster_source: "{{ inputs.flood_raster }}"
      h3_l3_cell: "{{ inputs.h3_chunk }}"
      stats: [mean, max]

  - id: building_points
    task_type: h3_point_aggregation
    params:
      point_source: "{{ inputs.building_footprints }}"
      h3_l3_cell: "{{ inputs.h3_chunk }}"
      agg: count

  - id: weighted_risk
    task_type: h3_weighted_aggregation
    depends_on: [flood_zonal, building_points]
    params:
      base_data: "{{ nodes.flood_zonal.output.parquet_path }}"
      weight_data: "{{ nodes.building_points.output.parquet_path }}"
      weight_column: building_count
      output_column: weighted_flood_risk
```

### 6.2 Adding Population Density

Add a parallel node — the DAG handles the dependency graph:

```yaml
  - id: population_zonal
    task_type: h3_zonal_stats
    params:
      raster_source: "{{ inputs.population_raster }}"
      h3_l3_cell: "{{ inputs.h3_chunk }}"
      stats: [mean]

  - id: multi_weight_risk
    task_type: h3_multi_weighted_aggregation
    depends_on: [flood_zonal, building_points, population_zonal]
    params:
      base_data: "{{ nodes.flood_zonal.output.parquet_path }}"
      weight_sources:
        - data: "{{ nodes.building_points.output.parquet_path }}"
          column: building_count
          weight: 0.6
        - data: "{{ nodes.population_zonal.output.parquet_path }}"
          column: mean
          weight: 0.4
```

### 6.3 Analyst-Configured Model Profiles

Weights and parameters are analyst-configured. Parameterization as a self-service API is a future goal, not a current priority.

```yaml
# profiles/flood_model_conservative.yaml
model_id: flood_conservative
weights:
  building_count: 0.7
  population_density: 0.3
thresholds:
  high_risk: 0.8
  moderate_risk: 0.5
```

Analysts select a profile when submitting a batch run. The profile values flow into node params via template resolution.

---

## 7. Reusable Task Types

The H3 pipeline introduces these new handler types for DAG workers:

| Task Type | Input | Output | Description |
|-----------|-------|--------|-------------|
| `h3_discover_raster` | STAC URL or blob path | bbox, CRS, band info, L3 cell list | Catalog a raster source + intersect with static land set |
| `h3_zonal_stats` | raster + L3 cell ID | parquet path | Core zonal stats (mean/sum/median/stdev) |
| `h3_point_aggregation` | point source + L3 cell ID | parquet path | Aggregate point/polygon features to H3 cells |
| `h3_weighted_aggregation` | base + weight parquets | parquet path | Weighted combination of H3 datasets |
| `h3_compaction` | list of chunk parquets | consolidated parquet path | Merge per-chunk files into production parquet (top priority) |

All task types are idempotent. Rerunning with the same inputs overwrites the same output path.

---

## 8. Key Dependencies

| Dependency | Purpose | Notes |
|------------|---------|-------|
| `h3-py` | H3 cell generation, hierarchy, polygon conversion | Pure Python bindings to Uber's H3 |
| `exactextract` | Fast zonal statistics (raster x polygon) | C++ core, Python bindings, handles partial pixels |
| `rasterio` | Raster I/O, windowed reads, COG support | Already in worker image (osgeo/gdal base) |
| `pyarrow` | Parquet write | Already available |
| `planetary-computer` | STAC catalog + SAS token signing for PC data | Python SDK |
| `duckdb` | Query engine for parquet (analyst + front-end) | Already in use |

---

## 9. Open Questions

1. **COG streaming threshold**: At what raster size do we switch from windowed COG reads to download-first? Needs benchmarking — likely >1GB or when the network round-trips per window exceed download time.

2. **Land geometry source**: What geometry defines "land + littoral waters"? Natural Earth 10m coastline? Custom geometry? Resolution matters — too coarse and we include ocean cells, too fine and processing the filter is slow.

3. **Blob storage target**: Same storage account (`rmhazuregeo`) or separate? Output volume could be significant at scale.

4. **H3 level range**: Do we need L2 stats, or is L2 just a grouping/discovery level? Coarser levels (L2-L3) can be aggregated from L4+ stats mathematically (mean of means weighted by cell area) rather than running zonal stats directly.

5. **Band handling**: Multi-band rasters — stats per band, or only specific bands? Who specifies which band(s)? (Defined in `h3_raster_sources.bands`.)

6. **Vector aggregation**: How do point and polygon features (building footprints, admin boundaries, infrastructure) aggregate to H3 cells? See Section 10.

---

## 10. Vector Aggregation

### 10.1 Overview

Vector aggregation assigns point, line, and polygon features to H3 cells and computes summary statistics. Data sources are either **Overture Maps GeoParquet** (accessed over HTTP with spatial predicate pushdown) or **internal PostGIS tables** (queried with bbox filters).

No local copy of planet.osm. No PostGIS import of Overture data. GeoParquet over HTTP is faster in every dimension for this use case:
- Spatially partitioned row groups — bbox filter on one L3 cell fetches ~50–200 MB, not the full dataset
- Geometries already constructed (no OSM node→way→geometry reconstruction)
- Schema normalized (`class: "motorway"` not `tags: {highway: motorway}`)
- Each worker reads its own bbox independently — no shared database bottleneck
- Overture includes Microsoft building footprints + Meta places (better global coverage than raw OSM)

Internal PostGIS is used when data isn't in Overture — e.g., millions of ACLED conflict event points, proprietary infrastructure datasets, or curated admin boundaries hosted in our own database.

### 10.2 Aggregation Types

| Type | Geometry | Operation | Example |
|------|----------|-----------|---------|
| **Count** | point | Count features per cell | POI count, ACLED event count |
| **Count by category** | point/line/polygon | Count grouped by attribute | POIs by category, buildings by type |
| **Sum of length** | line | Clip line to cell, sum clipped lengths | Road km by class, waterway km |
| **Sum of area** | polygon | Clip polygon to cell, sum clipped areas | Building footprint area, land use area |
| **Binary intersect** | polygon | Does this cell intersect the feature? | Inside national park? Flood zone? Urban area? |

### 10.3 Geometry Operations

**Points** — trivial assignment:
```python
cell = h3.latlng_to_cell(lat, lng, resolution)
```
O(1) per point, microseconds. No clipping needed.

**Lines** (roads, waterways, pipelines):
1. Build STRtree spatial index of line segments in the L3 chunk
2. For each H3 cell, query index for intersecting segments
3. `shapely.intersection(segment, cell_polygon)` → clipped geometry
4. `geod.geometry_length(clipped)` → length in meters
5. Group by `(h3_index, resolution, category)` → sum length

**Polygons** (buildings, land use):
- **Small polygons** (buildings): centroid → `latlng_to_cell()` for assignment, use original area. At L7 (~5.16 km²) virtually all buildings fit in one cell.
- **Large polygons** (national parks, admin boundaries): clip to cell, measure clipped area. Or use binary intersect (see below).

**Binary intersect** (containment/overlap):
- "Is this H3 cell inside a national park?" = boolean per cell
- Fast path: `h3.polygon_to_cells(park_boundary, resolution)` returns all cells whose centers fall within the polygon — covers 99% of cases
- Boundary precision: for cells on the edge, `shapely.intersects(cell_polygon, park_boundary)` for exact test
- Output: set of cells that intersect. Absence = false. No need to store millions of `false` rows.

```
h3_index | resolution | feature_id         | feature_name
87280e0  | 7          | yellowstone_np     | Yellowstone National Park
87280e0  | 7          | flood_zone_100yr   | FEMA 100-Year Flood Zone
```

Binary outputs are sparse — only cells that intersect are stored. Query pattern:

```sql
-- All L7 cells in national parks for a region
SELECT h3_index, feature_name
FROM 'h3_stats/source=national_parks/period=static/resolution=7/*.parquet'
WHERE h3_index LIKE '87280e%'

-- Join: flood risk where cell is ALSO in a national park
SELECT a.h3_index, a.mean AS flood_risk
FROM 'h3_stats/source=flood_30m/...' a
SEMI JOIN 'h3_stats/source=national_parks/...' b USING (h3_index)
```

### 10.4 Data Sources

#### Overture Maps GeoParquet (primary)

Accessed directly over HTTP. DuckDB spatial extension handles bbox predicate pushdown:

```python
import duckdb

con = duckdb.connect()
con.install_extension("spatial")
con.load_extension("spatial")

# Roads in this L3 cell — only fetches relevant parquet row groups
roads = con.sql(f"""
    SELECT geometry, class, subclass
    FROM 'azure://overture/theme=transportation/type=segment/*.parquet'
    WHERE bbox.xmin < {east} AND bbox.xmax > {west}
      AND bbox.ymin < {north} AND bbox.ymax > {south}
      AND class IN ('motorway','trunk','primary','secondary',
                    'tertiary','residential')
""").df()
```

No import step. No local copy. No PostGIS.

Available Overture themes for H3 aggregation:

| Theme | Type | Geometry | Aggregations |
|-------|------|----------|-------------|
| `transportation` | `segment` | line | length by class (motorway, trunk, primary, ...) |
| `buildings` | `building` | polygon | count, total area, count by type |
| `places` | `place` | point | count, count by category |
| `base` | `land_use` | polygon | area by class, binary intersect |
| `base` | `water` | polygon | area, binary intersect |
| `divisions` | `division_area` | polygon | binary intersect (admin boundaries, parks) |

#### Internal PostGIS (supplemental)

For data not in Overture — proprietary, curated, or sensitive datasets hosted in our own PostgreSQL/PostGIS.

```python
import psycopg
from shapely import wkb

# Worker connects to internal PostGIS, spatial query for L3 bbox
async with pool.connection() as conn:
    rows = await conn.execute("""
        SELECT ST_AsBinary(geom) as geom, event_type, fatalities
        FROM acled.events
        WHERE geom && ST_MakeEnvelope(%s, %s, %s, %s, 4326)
    """, (west, south, east, north))

    for row in rows:
        geom = wkb.loads(row["geom"])
        cell = h3.latlng_to_cell(geom.y, geom.x, resolution)
        # aggregate...
```

The connection string is referenced by name in the source config (`postgis_connection: "acled_db"`), resolved from environment variables at runtime. Workers never store credentials in the catalog.

### 10.5 Vector Source Catalog

Parallel to `h3_raster_sources` with vector-specific config:

```sql
CREATE TABLE dagapp.h3_vector_sources (
    source_id         TEXT PRIMARY KEY,
    name              TEXT NOT NULL,            -- "Overture Roads"
    description       TEXT,

    -- Access
    access_type       TEXT NOT NULL,            -- overture | geoparquet | postgis
    access_uri        TEXT NOT NULL,            -- GeoParquet glob, STAC URI, PostGIS table
    postgis_connection TEXT,                    -- env var name for connection string (postgis only)

    -- Feature config
    feature_type      TEXT NOT NULL,            -- point | line | polygon
    geometry_op       TEXT NOT NULL,            -- assign | clip | binary_intersect
    category_column   TEXT,                     -- "class", "event_type", NULL for no breakdown
    category_filter   TEXT[],                   -- subset of categories to include, NULL for all
    aggregations      JSONB NOT NULL,           -- {"count": true, "length_m": "sum", "area_m2": "sum"}

    -- Processing
    h3_levels         INT[] NOT NULL,           -- [3,4,5,6,7]
    temporal_mode     TEXT NOT NULL DEFAULT 'static',
    land_geometry_id  TEXT,
    active            BOOLEAN NOT NULL DEFAULT true,

    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    version           INT NOT NULL DEFAULT 1
);
```

**Examples:**

```sql
-- Overture roads: length by class
INSERT INTO dagapp.h3_vector_sources VALUES (
    'overture_roads', 'Overture Roads', 'OSM road network via Overture',
    'overture', 'azure://overture/theme=transportation/type=segment/*.parquet', NULL,
    'line', 'clip', 'class', ARRAY['motorway','trunk','primary','secondary','tertiary','residential'],
    '{"length_m": "sum", "segment_count": "count"}',
    ARRAY[3,4,5,6,7], 'static', 'land_default', true,
    now(), now(), 1
);

-- ACLED conflict events: count by type from internal PostGIS
INSERT INTO dagapp.h3_vector_sources VALUES (
    'acled_events', 'ACLED Conflict Events', 'Armed conflict location data',
    'postgis', 'acled.events', 'ACLED_DB_URL',
    'point', 'assign', 'event_type', NULL,
    '{"event_count": "count", "fatalities": "sum"}',
    ARRAY[3,4,5,6,7], 'monthly', 'land_default', true,
    now(), now(), 1
);

-- National parks: binary intersect
INSERT INTO dagapp.h3_vector_sources VALUES (
    'national_parks', 'National Parks & Protected Areas', 'WDPA protected areas',
    'geoparquet', 'azure://rmhazuregeo/reference/wdpa_parks.parquet', NULL,
    'polygon', 'binary_intersect', 'park_name', NULL,
    '{"intersects": true}',
    ARRAY[3,4,5,6,7], 'static', 'land_default', true,
    now(), now(), 1
);
```

`h3_computation_runs` and `h3_output_manifest` work unchanged — they reference `source_id` generically. The `config_snapshot` captures the full vector source row at trigger time.

### 10.6 Output Structure

**Metric aggregations** (count, length, area) — long format with optional category:

```
h3_index | resolution | category    | count | length_m | area_m2
87280e0  | 7          | motorway    | 12    | 3421.5   | NULL
87280e0  | 7          | residential | 847   | 12403.8  | NULL
```

**Binary intersect** — sparse (only intersecting cells stored):

```
h3_index | resolution | feature_id       | feature_name
87280e0  | 7          | yellowstone_np   | Yellowstone National Park
```

Both stored as parquet under the same layout:

```
h3_stats/
  source=overture_roads/
    period=static/
      resolution=7/
        h3_l3={cell_id}.parquet
  source=acled_events/
    period=2025-01/
      resolution=7/
        h3_l3={cell_id}.parquet
  source=national_parks/
    period=static/
      resolution=7/
        h3_l3={cell_id}.parquet
```

### 10.7 Workload Characteristics

Vector aggregation has **wildly uneven** workload per L3 chunk. This is expected and handled naturally by competing consumers:

| L3 cell location | Features (est.) | Task duration |
|------------------|----------------|---------------|
| Sahara, open ocean | 0–100 | <1 second |
| Rural midwest | 1,000–10,000 | 2–10 seconds |
| Suburban metro | 50,000–200,000 | 30–120 seconds |
| Downtown Tokyo | 5,000,000+ | 5–10 minutes |

A batch of 500 L3 chunks with 4 workers: 490 fast chunks clear in minutes, 10 dense urban chunks take a few minutes each. Total wall clock dominated by the densest cells — still under 15 minutes.

No adaptive sub-chunking needed. If profiling later shows extreme outliers (>30 min), we can split dense L3 cells into L4 sub-tasks, but this is an optimization, not a design requirement.

### 10.8 Reusable Task Types (Vector)

| Task Type | Input | Output | Description |
|-----------|-------|--------|-------------|
| `h3_point_aggregation` | point source + L3 cell ID | parquet path | Count/sum points per cell, optional category breakdown |
| `h3_line_aggregation` | line source + L3 cell ID | parquet path | Clip lines, sum length per cell by category |
| `h3_polygon_aggregation` | polygon source + L3 cell ID | parquet path | Count/area of polygons per cell |
| `h3_binary_intersect` | polygon source + L3 cell ID | parquet path | Sparse boolean: which cells intersect which features |

All idempotent. All follow the same fan-out-by-L3-cell pattern as raster zonal stats.

---

## 11. Cell Connectivity Graph (Future Enhancement)

### 11.1 Concept

Model connectivity between **adjacent** H3 cells using road network data. Instead of aggregating GPS trip observations (Uber's approach), we synthesize observations from road infrastructure: **a road segment crossing from one H3 cell into an adjacent cell is a connectivity observation**.

The result is a weighted graph where:
- **Nodes** = H3 cells
- **Edges** = road boundary crossings between adjacent cells only (each hex has 6 neighbors)
- **Edge weight** = connectivity score derived from road class, crossing count, and inter-cell distance

A motorway crossing = strong connectivity signal. A residential street = weak signal. Multiple crossings between the same cell pair = stronger aggregate signal.

### 11.2 Adjacent-Only Graph Model

The graph ONLY has edges between neighboring cells. Long-distance connectivity is an emergent property computed by traversing the graph (Dijkstra), not stored directly.

**Why adjacent-only is correct:**
- Mirrors physical reality — you drive to the next cell, not teleport
- H3 cells have 6 natural neighbors — the graph structure is local
- Direct distant-pair edges are impossible: 30M land cells at L7 → ~10^14 pairs
- Motorway corridors emerge naturally as chains of high-weight adjacent edges
- Dijkstra strongly prefers high-weight chains over parallel low-weight chains

**Example: Istanbul → Ankara (~450 km):**
- ~180 cell hops at L7 (cells ~2.5 km edge-to-edge)
- Motorway edge cost: 2.5 km / 100 km/h × 60 = ~1.5 min/hop
- 180 hops × 1.5 min = ~270 min ≈ 4.5 hours
- Actual driving: ~5–6 hours — reasonable approximation
- Dijkstra finds this path in milliseconds on a 30M-node / 180M-edge graph (igraph, C++ backed)

### 11.3 Detecting Boundary Crossings

For each road segment in an L3 chunk:

1. **Sample points** along the line geometry at regular intervals (~100m)
2. **Convert** each point to its H3 cell: `h3.latlng_to_cell(lat, lng, resolution)`
3. **Deduplicate** consecutive identical cells
4. Each consecutive pair of **different** cells = one boundary crossing

```python
from shapely import LineString
import h3

def detect_crossings(line: LineString, resolution: int, road_class: str):
    """Walk a road segment and yield cell boundary crossings."""
    # Sample points every ~100m along the line
    length_m = geod.geometry_length(line)
    num_points = max(int(length_m / 100), 2)
    fractions = [i / (num_points - 1) for i in range(num_points)]

    cells = []
    for frac in fractions:
        point = line.interpolate(frac, normalized=True)
        cell = h3.latlng_to_cell(point.y, point.x, resolution)
        if not cells or cell != cells[-1]:
            cells.append(cell)

    # Each consecutive pair = one crossing
    for i in range(len(cells) - 1):
        yield {
            "cell_a": min(cells[i], cells[i+1]),  # canonical ordering
            "cell_b": max(cells[i], cells[i+1]),
            "road_class": road_class,
        }
```

A straight motorway passing through 5 L7 cells produces 4 crossings. A winding residential road might cross the same cell boundary multiple times — each crossing is counted.

**Canonical ordering**: `cell_a < cell_b` ensures each edge is stored once regardless of travel direction (roads are bidirectional for connectivity).

### 11.4 Boundary Ownership (No Shuffle)

A road near an L3 chunk boundary is loaded by both adjacent chunks (their bboxes overlap). Both detect the same crossing. This is a shuffle/reduce problem — **unless we avoid producing duplicates in the first place.**

**Deterministic ownership rule**: For any boundary crossing between two L7 cells in different L3 chunks, the chunk with the **lexicographically smaller L3 cell ID** owns the edge. Each worker decides locally — no coordination, no fan-in dedup, no shuffle.

```python
my_l3 = chunk_l3_cell

for crossing in detect_crossings(segment, resolution, road_class):
    cell_a_l3 = h3.cell_to_parent(crossing["cell_a"], 3)
    cell_b_l3 = h3.cell_to_parent(crossing["cell_b"], 3)

    if cell_a_l3 == cell_b_l3:
        # Interior crossing — both cells in my chunk. Always keep.
        emit(crossing)
    else:
        # Boundary crossing — only the smaller L3 chunk keeps it.
        owner_l3 = min(cell_a_l3, cell_b_l3)
        if owner_l3 == my_l3:
            emit(crossing)
        # else: skip — the other chunk owns this edge
```

`h3.cell_to_parent()` is O(1), no network call. The ownership rule is deterministic — given the same boundary crossing, exactly one of the two chunks emits it. Zero duplicates produced. Fan-in is a plain concat, no dedup required. The DAG needs no new capability.

### 11.5 Edge List Output

```
cell_a   | cell_b   | road_class  | crossings | road_length_m
87280e0a | 87280e0b | motorway    | 2         | 4820.3
87280e0a | 87280e0b | residential | 8         | 2156.7
87280e0a | 87280e0c | primary     | 1         | 2510.1
87280e0b | 87280e0c | tertiary    | 3         | 3842.5
```

`road_length_m` = total length of road of that class connecting the two cells (sum of clipped segments that cross that boundary). This is needed for travel time estimation — crossing count alone doesn't encode distance.

Stored as parquet, partitioned by L3 chunk:

```
h3_connectivity/
  resolution=7/
    h3_l3={cell_id}.parquet     # edges owned by this chunk
```

**Raw observations, not scores.** The parquet stores crossing counts and road lengths by class. Scoring and travel-cost models are applied at query time — keeps the data reusable across different weighting models.

### 11.6 Scoring Models (Query-Time)

Different scoring functions for different analyses, all applied to the same underlying edge data:

**Simple connectivity score (relative strength):**
```sql
SELECT cell_a, cell_b,
       SUM(CASE road_class
           WHEN 'motorway'    THEN crossings * 100
           WHEN 'trunk'       THEN crossings * 50
           WHEN 'primary'     THEN crossings * 25
           WHEN 'secondary'   THEN crossings * 10
           WHEN 'tertiary'    THEN crossings * 5
           WHEN 'residential' THEN crossings * 1
       END) AS connectivity_score
FROM 'h3_connectivity/resolution=7/*.parquet'
GROUP BY cell_a, cell_b
```

**Travel time estimate (for Dijkstra):**
```sql
-- Edge cost in seconds — best road class wins (fastest road determines crossing time)
SELECT cell_a, cell_b,
       MIN(CASE road_class
           WHEN 'motorway'    THEN road_length_m / 100000.0 * 3600
           WHEN 'trunk'       THEN road_length_m /  80000.0 * 3600
           WHEN 'primary'     THEN road_length_m /  60000.0 * 3600
           WHEN 'secondary'   THEN road_length_m /  50000.0 * 3600
           WHEN 'tertiary'    THEN road_length_m /  40000.0 * 3600
           WHEN 'residential' THEN road_length_m /  30000.0 * 3600
       END) AS travel_time_seconds
FROM 'h3_connectivity/resolution=7/*.parquet'
GROUP BY cell_a, cell_b
```

`MIN` because if a motorway AND a residential road both connect cells A↔B, you'd take the motorway. Dijkstra uses `travel_time_seconds` as edge weight — shortest path = fastest route.

**Analyst-configured profiles** — same pattern as Section 6.3:

```yaml
# profiles/connectivity_default.yaml
model_id: connectivity_default
class_speeds_kmh:
  motorway: 100
  trunk: 80
  primary: 60
  secondary: 50
  tertiary: 40
  residential: 30
```

### 11.7 Friction Surface Integration (Future)

Replace class-based speeds with physics-informed travel cost by combining road connectivity with raster-derived terrain data:

```
edge_cost_seconds = (road_length_m / base_speed_ms)
                  × slope_factor(from DEM zonal stats)
                  × surface_factor(paved vs unpaved)
                  × congestion_factor(if traffic data available)
```

The raster pipeline (Section 3) has already computed DEM slope stats per H3 cell. The connectivity graph provides the edges. Combining them:

```sql
-- Travel time adjusted for terrain slope at both endpoints
SELECT e.cell_a, e.cell_b,
       e.travel_time_seconds
         * (1.0 + (t1.mean + t2.mean) / 200.0)  -- slope penalty: +1% time per degree
         AS terrain_adjusted_seconds
FROM (
    SELECT cell_a, cell_b,
           MIN(road_length_m / (class_speed_ms) ) AS travel_time_seconds
    FROM 'h3_connectivity/resolution=7/*.parquet'
    GROUP BY cell_a, cell_b
) e
JOIN 'h3_stats/source=dem_slope/resolution=7/*.parquet' t1 ON e.cell_a = t1.h3_index
JOIN 'h3_stats/source=dem_slope/resolution=7/*.parquet' t2 ON e.cell_b = t2.h3_index
```

This is the convergence point: **raster stats + vector aggregation + connectivity graph all join on `h3_index`**. Every data product in the pipeline contributes to the friction surface.

### 11.8 Graph Analysis

The edge list loads directly into graph engines for network analysis:

```python
import igraph as ig
import duckdb

# Load connectivity graph — igraph handles 30M nodes / 180M edges efficiently
edges_df = duckdb.sql("""
    SELECT cell_a, cell_b,
           MIN(road_length_m / (CASE road_class
               WHEN 'motorway' THEN 27.8  -- 100 km/h in m/s
               WHEN 'trunk'    THEN 22.2
               WHEN 'primary'  THEN 16.7
               WHEN 'secondary' THEN 13.9
               WHEN 'tertiary' THEN 11.1
               WHEN 'residential' THEN 8.3
           END)) AS travel_time_seconds
    FROM 'h3_connectivity/resolution=7/*.parquet'
    GROUP BY cell_a, cell_b
""").df()

G = ig.Graph.DataFrame(edges_df, directed=False)

# Reachability: cells within 30-min drive from a starting cell
distances = G.distances(source=source_vertex, weights='travel_time_seconds')[0]
reachable = [v for v, d in enumerate(distances) if d <= 1800]

# Betweenness centrality: critical corridor chokepoints
centrality = G.betweenness(weights='travel_time_seconds')

# Connected components: identify disconnected regions
components = G.connected_components()
isolated = [c for c in components if len(c) < 10]
```

**Applications:**

| Analysis | Method | Value |
|----------|--------|-------|
| **Istanbul → Ankara** | Dijkstra shortest path, travel_time_seconds weight | Estimated drive time from cell graph |
| **30-min service area** | Single-source Dijkstra with cutoff=1800s | "What can a hospital/fire station reach?" |
| **Isolation detection** | Cells with low total edge weight or in small components | Rural access gaps, infrastructure planning |
| **Transport corridors** | High betweenness centrality cells | Critical chokepoints, vulnerability |
| **Equity analysis** | Compare connectivity across demographics/regions | "Underserved areas with poor road access" |
| **Resilience** | Remove high-centrality edges, measure fragmentation | "What if this bridge is destroyed?" |

### 11.9 DAG Workflow

```
┌─────────────────┐
│  load_roads     │  Load Overture road segments for L3 chunk
└───────┬─────────┘
        │
        ▼
┌─────────────────────┐
│  detect_crossings   │  Walk each segment, identify cell boundary crossings
│  + ownership filter │  Skip boundary edges owned by adjacent chunk (no shuffle)
└───────┬─────────────┘
        │
        ▼
┌─────────────────┐
│  aggregate      │  GROUP BY (cell_a, cell_b, road_class) → crossings + road_length_m
└───────┬─────────┘
        │
        ▼  (fan-out per L3 cell, same pattern as all other pipelines)
        │
┌─────────────────┐
│  fan_in_concat  │  Simple concat — no dedup needed (ownership rule guarantees uniqueness)
└─────────────────┘
```

**No shuffle. No dedup. No reduce.** The ownership rule (Section 11.4) ensures each boundary edge is emitted by exactly one chunk. Fan-in is a plain file concat.

### 11.10 Task Type

| Task Type | Input | Output | Description |
|-----------|-------|--------|-------------|
| `h3_connectivity` | road source + L3 cell ID | parquet path (edge list) | Detect boundary crossings with ownership filter, aggregate by road class |

Same computation catalog pattern — registered as a vector source with `geometry_op: "connectivity"`.

---

## 12. Implementation Phases (Rough)

| Phase | What | Depends On |
|-------|------|------------|
| **P1**: Computation catalog | Pydantic models + repos for `h3_raster_sources`, `h3_vector_sources`, `h3_computation_runs`, `h3_output_manifest` | Existing schema patterns |
| **P2**: Core zonal stats handler | `h3_zonal_stats` task type in worker, single raster + single L3 cell | Worker framework (done) |
| **P3**: Raster fan-out workflow | `h3_discover_raster` (static land filter) → fan-out `h3_zonal_stats` → fan-in + manifest | Fan-out/in (done), P1, P2 |
| **P4**: Planetary Computer integration | STAC discovery, COG streaming, SAS token signing | P3 |
| **P5**: Vector point aggregation | `h3_point_aggregation` — Overture places + internal PostGIS (ACLED) | P1, worker framework |
| **P6**: Vector line aggregation | `h3_line_aggregation` — Overture roads (length by class) | P5 (shares access patterns) |
| **P7**: Vector polygon aggregation | `h3_polygon_aggregation` + `h3_binary_intersect` — buildings, parks, admin boundaries | P5 |
| **P8**: Complex aggregation nodes | `h3_weighted_aggregation`, multi-source workflows | P3, P5–P7 |
| **P9**: Compaction + materialized views | **Top priority** — merge per-chunk parquet into production files, analyst-defined wide views. Schema dimensions known ahead of time (data inventory + feature engineering completed before compute). | P3 |
| **P10**: Cell connectivity graph | `h3_connectivity` — road crossing detection + edge list output | P6 (road access pattern) |
| **P11**: Friction surface | Combine connectivity graph + raster terrain stats → travel-cost weights | P10, P3 (DEM stats) |
