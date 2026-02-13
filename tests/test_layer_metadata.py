# ============================================================================
# LAYER METADATA TESTS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Tests - Phase 4D layer metadata models, repo, and service
# PURPOSE: Verify LayerMetadata/LayerCatalog models and MetadataService logic
# CREATED: 13 FEB 2026
# ============================================================================
"""
Layer Metadata Tests

Unit tests for:
- LayerMetadata model (creation, validation, computed fields, serialization)
- LayerCatalog model (creation, serialization)
- LayerMetadataRepository (CRUD with mocked DB)
- MetadataService (business logic with mocked repos)
- Schema generation (includes both tables)

Run with:
    pytest tests/test_layer_metadata.py -v
"""

import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from core.models.layer_metadata import LayerMetadata
from core.models.layer_catalog import LayerCatalog
from services.metadata_service import MetadataService
from core.models.geospatial_asset import GeospatialAsset
from core.contracts import ClearanceState


# ============================================================================
# HELPERS
# ============================================================================

def _make_metadata(
    asset_id="abc123",
    data_type="raster",
    display_name="Flood Risk 2023",
    **kwargs,
):
    """Create a test LayerMetadata."""
    return LayerMetadata(
        asset_id=asset_id,
        data_type=data_type,
        display_name=display_name,
        **kwargs,
    )


def _make_asset(asset_id="abc123", data_type="raster"):
    """Create a test GeospatialAsset."""
    return GeospatialAsset(
        asset_id=asset_id,
        platform_id="ddh",
        platform_refs={"dataset_id": "d1"},
        data_type=data_type,
    )


def _build_service():
    """Build a MetadataService with all repos mocked."""
    pool = MagicMock()
    svc = MetadataService(pool)

    # Replace repos with async mocks
    svc.metadata_repo = AsyncMock()
    svc.asset_repo = AsyncMock()

    return svc


# ============================================================================
# LAYER METADATA MODEL
# ============================================================================

class TestLayerMetadataModel:
    """Tests for LayerMetadata Pydantic model."""

    def test_create_with_defaults(self):
        """Minimal creation: only required fields."""
        m = _make_metadata()
        assert m.asset_id == "abc123"
        assert m.data_type == "raster"
        assert m.display_name == "Flood Risk 2023"
        assert m.description is None
        assert m.attribution is None
        assert m.keywords == []
        assert m.style == {}
        assert m.legend == []
        assert m.field_metadata == {}
        assert m.min_zoom is None
        assert m.max_zoom is None
        assert m.default_visible is True
        assert m.sort_order == 0
        assert m.thumbnail_url is None
        assert m.published_at is None
        assert m.version == 1

    def test_create_with_all_fields(self):
        """Full creation with all optional fields."""
        now = datetime.now(timezone.utc)
        m = LayerMetadata(
            asset_id="abc123",
            data_type="vector",
            display_name="Land Parcels",
            description="Parcel boundaries for 2024",
            attribution="County GIS",
            keywords=["parcels", "land", "boundaries"],
            style={"fill-color": "#3388ff", "stroke-width": 1},
            legend=[{"label": "Residential", "color": "#FF0000"}],
            field_metadata={"population": {"display_name": "Pop", "unit": "people"}},
            min_zoom=5,
            max_zoom=18,
            default_visible=False,
            sort_order=10,
            thumbnail_url="https://example.com/thumb.png",
            published_at=now,
        )
        assert m.data_type == "vector"
        assert m.keywords == ["parcels", "land", "boundaries"]
        assert m.style["fill-color"] == "#3388ff"
        assert len(m.legend) == 1
        assert m.field_metadata["population"]["unit"] == "people"
        assert m.min_zoom == 5
        assert m.max_zoom == 18
        assert m.default_visible is False
        assert m.sort_order == 10
        assert m.published_at == now

    def test_data_type_validation(self):
        """data_type must be 'vector' or 'raster'."""
        with pytest.raises(ValueError, match="data_type must be"):
            _make_metadata(data_type="pointcloud")

    def test_computed_is_published_false(self):
        """is_published is False when published_at is None."""
        m = _make_metadata()
        assert m.is_published is False

    def test_computed_is_published_true(self):
        """is_published is True when published_at is set."""
        m = _make_metadata(published_at=datetime.now(timezone.utc))
        assert m.is_published is True

    def test_computed_has_style(self):
        """has_style reflects whether style dict is non-empty."""
        m1 = _make_metadata()
        assert m1.has_style is False

        m2 = _make_metadata(style={"colormap": "viridis"})
        assert m2.has_style is True

    def test_computed_has_legend(self):
        """has_legend reflects whether legend list is non-empty."""
        m1 = _make_metadata()
        assert m1.has_legend is False

        m2 = _make_metadata(legend=[{"label": "High", "color": "#FF0000"}])
        assert m2.has_legend is True

    def test_model_dump_serialization(self):
        """model_dump produces dict with correct structure."""
        m = _make_metadata(
            style={"colormap": "viridis"},
            keywords=["flood", "hazard"],
        )
        d = m.model_dump(mode="json")
        assert d["asset_id"] == "abc123"
        assert d["data_type"] == "raster"
        assert d["style"] == {"colormap": "viridis"}
        assert d["keywords"] == ["flood", "hazard"]
        assert "is_published" in d
        assert "has_style" in d
        assert "has_legend" in d

    def test_zoom_range_validation(self):
        """Pydantic validates zoom values are 0-24."""
        with pytest.raises(ValueError):
            _make_metadata(min_zoom=-1)

        with pytest.raises(ValueError):
            _make_metadata(max_zoom=25)

    def test_sql_metadata(self):
        """Model has correct SQL DDL metadata."""
        assert LayerMetadata.__sql_table__ == "layer_metadata"
        assert LayerMetadata.__sql_schema__ == "dagapp"
        assert LayerMetadata.__sql_primary_key__ == ["asset_id"]
        assert "asset_id" in LayerMetadata.__sql_foreign_keys__


# ============================================================================
# LAYER CATALOG MODEL
# ============================================================================

class TestLayerCatalogModel:
    """Tests for LayerCatalog Pydantic model."""

    def test_create_minimal(self):
        """Minimal creation with table_name PK."""
        c = LayerCatalog(
            table_name="geo.parcels_2024",
            data_type="vector",
            display_name="Land Parcels 2024",
        )
        assert c.table_name == "geo.parcels_2024"
        assert c.data_type == "vector"
        assert c.display_name == "Land Parcels 2024"
        assert c.source_asset_id is None
        assert c.keywords == []
        assert c.style == {}

    def test_source_asset_traceability(self):
        """source_asset_id is optional for cross-database traceability."""
        c = LayerCatalog(
            table_name="geo.flood_risk",
            data_type="raster",
            display_name="Flood Risk",
            source_asset_id="abc123",
        )
        assert c.source_asset_id == "abc123"

    def test_model_dump_serialization(self):
        """model_dump produces correct dict."""
        c = LayerCatalog(
            table_name="geo.parcels",
            data_type="vector",
            display_name="Parcels",
            style={"fill-color": "#3388ff"},
            legend=[{"label": "Zone A", "color": "#FF0000"}],
        )
        d = c.model_dump(mode="json")
        assert d["table_name"] == "geo.parcels"
        assert d["style"] == {"fill-color": "#3388ff"}
        assert len(d["legend"]) == 1

    def test_sql_metadata(self):
        """Model has correct SQL DDL metadata."""
        assert LayerCatalog.__sql_table__ == "layer_catalog"
        assert LayerCatalog.__sql_schema__ == "dagapp"
        assert LayerCatalog.__sql_primary_key__ == ["table_name"]


# ============================================================================
# METADATA REPOSITORY
# ============================================================================

class TestLayerMetadataRepo:
    """Tests for LayerMetadataRepository with mocked DB connections."""

    def test_create_and_return(self):
        """create() inserts and returns the metadata object."""
        from repositories.metadata_repo import LayerMetadataRepository

        pool = MagicMock()
        conn_mock = AsyncMock()
        pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn_mock)
        pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)
        conn_mock.execute = AsyncMock()

        repo = LayerMetadataRepository(pool)
        metadata = _make_metadata()

        result = asyncio.run(repo.create(metadata))
        assert result.asset_id == "abc123"
        conn_mock.execute.assert_awaited_once()

    def test_get_returns_model(self):
        """get() returns LayerMetadata from DB row."""
        from repositories.metadata_repo import LayerMetadataRepository

        pool = MagicMock()
        conn_mock = AsyncMock()
        pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn_mock)
        pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

        row = {
            "asset_id": "abc123",
            "data_type": "raster",
            "display_name": "Test Layer",
            "description": None,
            "attribution": None,
            "keywords": ["test"],
            "style": {"colormap": "viridis"},
            "legend": [],
            "field_metadata": {},
            "min_zoom": None,
            "max_zoom": None,
            "default_visible": True,
            "sort_order": 0,
            "thumbnail_url": None,
            "published_at": None,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
            "version": 1,
        }

        result_mock = AsyncMock()
        result_mock.fetchone = AsyncMock(return_value=row)
        conn_mock.execute = AsyncMock(return_value=result_mock)
        conn_mock.row_factory = None  # Will be set by repo

        repo = LayerMetadataRepository(pool)
        result = asyncio.run(repo.get("abc123"))

        assert result is not None
        assert result.asset_id == "abc123"
        assert result.display_name == "Test Layer"
        assert result.style == {"colormap": "viridis"}
        assert result.keywords == ["test"]

    def test_get_returns_none_when_not_found(self):
        """get() returns None when no row found."""
        from repositories.metadata_repo import LayerMetadataRepository

        pool = MagicMock()
        conn_mock = AsyncMock()
        pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn_mock)
        pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

        result_mock = AsyncMock()
        result_mock.fetchone = AsyncMock(return_value=None)
        conn_mock.execute = AsyncMock(return_value=result_mock)
        conn_mock.row_factory = None

        repo = LayerMetadataRepository(pool)
        result = asyncio.run(repo.get("nonexistent"))
        assert result is None

    def test_update_success_increments_version(self):
        """update() returns True and increments version on success."""
        from repositories.metadata_repo import LayerMetadataRepository

        pool = MagicMock()
        conn_mock = AsyncMock()
        pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn_mock)
        pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

        result_mock = MagicMock()
        result_mock.rowcount = 1
        conn_mock.execute = AsyncMock(return_value=result_mock)

        repo = LayerMetadataRepository(pool)
        metadata = _make_metadata(version=1)

        result = asyncio.run(repo.update(metadata))
        assert result is True
        assert metadata.version == 2

    def test_update_version_conflict_returns_false(self):
        """update() returns False on version conflict (rowcount=0)."""
        from repositories.metadata_repo import LayerMetadataRepository

        pool = MagicMock()
        conn_mock = AsyncMock()
        pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn_mock)
        pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

        result_mock = MagicMock()
        result_mock.rowcount = 0
        conn_mock.execute = AsyncMock(return_value=result_mock)

        repo = LayerMetadataRepository(pool)
        metadata = _make_metadata(version=1)

        result = asyncio.run(repo.update(metadata))
        assert result is False
        assert metadata.version == 1  # Not incremented

    def test_delete_returns_true_when_found(self):
        """delete() returns True when row exists."""
        from repositories.metadata_repo import LayerMetadataRepository

        pool = MagicMock()
        conn_mock = AsyncMock()
        pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn_mock)
        pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

        result_mock = MagicMock()
        result_mock.rowcount = 1
        conn_mock.execute = AsyncMock(return_value=result_mock)

        repo = LayerMetadataRepository(pool)
        result = asyncio.run(repo.delete("abc123"))
        assert result is True


# ============================================================================
# METADATA SERVICE
# ============================================================================

class TestMetadataService:
    """Tests for MetadataService business logic."""

    def test_create_metadata_happy_path(self):
        """create_metadata validates asset exists and creates metadata."""
        svc = _build_service()
        asset = _make_asset()
        svc.asset_repo.get = AsyncMock(return_value=asset)
        svc.metadata_repo.create = AsyncMock(side_effect=lambda m: m)

        result = asyncio.run(
            svc.create_metadata(
                asset_id="abc123",
                data_type="raster",
                display_name="Flood Risk",
                style={"colormap": "viridis"},
            )
        )

        assert result.asset_id == "abc123"
        assert result.display_name == "Flood Risk"
        assert result.style == {"colormap": "viridis"}
        svc.asset_repo.get.assert_awaited_once_with("abc123")
        svc.metadata_repo.create.assert_awaited_once()

    def test_create_metadata_asset_not_found(self):
        """create_metadata raises ValueError if asset doesn't exist."""
        svc = _build_service()
        svc.asset_repo.get = AsyncMock(return_value=None)

        with pytest.raises(ValueError, match="not found"):
            asyncio.run(
                svc.create_metadata(
                    asset_id="nonexistent",
                    data_type="raster",
                    display_name="Test",
                )
            )

    def test_create_metadata_zoom_constraint(self):
        """create_metadata raises ValueError when min_zoom > max_zoom."""
        svc = _build_service()
        asset = _make_asset()
        svc.asset_repo.get = AsyncMock(return_value=asset)

        with pytest.raises(ValueError, match="min_zoom.*greater than max_zoom"):
            asyncio.run(
                svc.create_metadata(
                    asset_id="abc123",
                    data_type="raster",
                    display_name="Test",
                    min_zoom=15,
                    max_zoom=5,
                )
            )

    def test_get_metadata(self):
        """get_metadata returns metadata from repo."""
        svc = _build_service()
        metadata = _make_metadata()
        svc.metadata_repo.get = AsyncMock(return_value=metadata)

        result = asyncio.run(svc.get_metadata("abc123"))
        assert result.asset_id == "abc123"

    def test_update_metadata_merge_and_lock(self):
        """update_metadata merges fields and uses optimistic locking."""
        svc = _build_service()
        existing = _make_metadata(style={"colormap": "viridis"})
        svc.metadata_repo.get = AsyncMock(return_value=existing)
        svc.metadata_repo.update = AsyncMock(return_value=True)

        result = asyncio.run(
            svc.update_metadata(
                asset_id="abc123",
                expected_version=1,
                style={"colormap": "magma"},
                sort_order=5,
            )
        )

        assert result.style == {"colormap": "magma"}
        assert result.sort_order == 5
        svc.metadata_repo.update.assert_awaited_once()

    def test_update_metadata_version_conflict(self):
        """update_metadata raises ValueError on version mismatch."""
        svc = _build_service()
        existing = _make_metadata(version=2)
        svc.metadata_repo.get = AsyncMock(return_value=existing)

        with pytest.raises(ValueError, match="Version conflict"):
            asyncio.run(
                svc.update_metadata(
                    asset_id="abc123",
                    expected_version=1,
                    display_name="Updated",
                )
            )

    def test_update_metadata_not_found(self):
        """update_metadata raises ValueError if metadata doesn't exist."""
        svc = _build_service()
        svc.metadata_repo.get = AsyncMock(return_value=None)

        with pytest.raises(ValueError, match="not found"):
            asyncio.run(
                svc.update_metadata(
                    asset_id="nonexistent",
                    expected_version=1,
                    display_name="Test",
                )
            )

    def test_update_metadata_zoom_constraint_after_merge(self):
        """update_metadata validates zoom constraints after merging."""
        svc = _build_service()
        existing = _make_metadata(min_zoom=10)
        svc.metadata_repo.get = AsyncMock(return_value=existing)

        with pytest.raises(ValueError, match="min_zoom.*greater than max_zoom"):
            asyncio.run(
                svc.update_metadata(
                    asset_id="abc123",
                    expected_version=1,
                    max_zoom=5,  # 10 > 5 after merge
                )
            )

    def test_delete_metadata(self):
        """delete_metadata hard deletes."""
        svc = _build_service()
        svc.metadata_repo.delete = AsyncMock(return_value=True)

        result = asyncio.run(svc.delete_metadata("abc123"))
        assert result is True
        svc.metadata_repo.delete.assert_awaited_once_with("abc123")

    def test_delete_metadata_not_found(self):
        """delete_metadata raises ValueError if not found."""
        svc = _build_service()
        svc.metadata_repo.delete = AsyncMock(return_value=False)

        with pytest.raises(ValueError, match="not found"):
            asyncio.run(svc.delete_metadata("nonexistent"))

    def test_list_metadata_all(self):
        """list_metadata returns all when no filter."""
        svc = _build_service()
        svc.metadata_repo.list_all = AsyncMock(return_value=[_make_metadata()])

        result = asyncio.run(svc.list_metadata())
        assert len(result) == 1
        svc.metadata_repo.list_all.assert_awaited_once_with(100)

    def test_list_metadata_by_data_type(self):
        """list_metadata filters by data_type when provided."""
        svc = _build_service()
        svc.metadata_repo.list_by_data_type = AsyncMock(return_value=[_make_metadata()])

        result = asyncio.run(svc.list_metadata(data_type="raster"))
        assert len(result) == 1
        svc.metadata_repo.list_by_data_type.assert_awaited_once_with("raster", 100)


# ============================================================================
# SCHEMA GENERATION
# ============================================================================

class TestSchemaGeneration:
    """Tests that DDL generation includes layer_metadata and layer_catalog."""

    def test_generate_all_includes_layer_metadata(self):
        """generate_all() produces DDL statements for layer_metadata table."""
        from core.schema.sql_generator import PydanticToSQL
        from psycopg import sql

        generator = PydanticToSQL(schema_name="dagapp")
        statements = generator.generate_all()

        # Check that at least one statement references each table
        # sql.Composed objects contain Identifier parts — check via repr
        all_reprs = " ".join(repr(stmt) for stmt in statements)
        assert "layer_metadata" in all_reprs
        assert "layer_catalog" in all_reprs

    def test_generate_all_statement_count_increased(self):
        """Adding 2 tables + indexes + trigger increases total statement count."""
        from core.schema.sql_generator import PydanticToSQL

        generator = PydanticToSQL(schema_name="dagapp")
        statements = generator.generate_all()

        # Previously ~45-50 statements. Adding 2 tables + indexes + trigger adds ~10+
        # Just verify it's a reasonable number (more than before)
        assert len(statements) > 50
