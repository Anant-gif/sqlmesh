# SPDX-License-Identifier: Apache-2.0
from pathlib import Path

from sqlmesh.core import dialect as d
from sqlmesh.core.config import Config, ModelDefaultsConfig
from sqlmesh.core.context import Context
from sqlmesh.core.manifest import build_catalog, build_manifest
from sqlmesh.core.model import SeedKind, create_seed_model, load_sql_based_model


def _context(*queries: str) -> Context:
    context = Context(
        config=Config(model_defaults=ModelDefaultsConfig(dialect="duckdb")), load=False
    )
    for query in queries:
        context.upsert_model(load_sql_based_model(d.parse(query), dialect="duckdb"))
    return context


def test_catalog_preserves_column_order():
    context = _context(
        "MODEL (name test.ordered, columns (z INT, a INT), "
        "column_descriptions (a = 'First letter')); SELECT 1 AS z, 2 AS a;"
    )
    manifest = build_manifest(context)
    node = next(iter(manifest["nodes"].values()))
    assert list(node["columns"]) == ["z", "a"]
    columns = build_catalog(manifest)["nodes"][node["unique_id"]]["columns"]
    assert columns["z"]["index"] == 1
    assert columns["a"]["index"] == 2
    assert columns["a"]["comment"] == "First letter"


def test_seed_checksum_tracks_csv_content(tmp_path: Path):
    csv = tmp_path / "data.csv"
    definition = tmp_path / "seed.sql"
    definition.write_text("MODEL (name test.seed, kind SEED (path 'data.csv'));", encoding="utf-8")
    context = _context()

    def export(content: str):
        csv.write_text(content, encoding="utf-8")
        context.upsert_model(
            create_seed_model(
                "test.seed", SeedKind(path="data.csv"), path=definition, dialect="duckdb"
            )
        )
        return next(iter(build_manifest(context)["nodes"].values()))

    before = export("id\n1\n")
    after = export("id\n2\n")
    assert before["unique_id"] == after["unique_id"]
    assert before["checksum"] != after["checksum"]
    assert after["checksum"] == export("id\n2\n")["checksum"]
    assert after["config"]["materialized"] == "seed"


def test_embedded_model_retains_lineage_without_catalog_relation():
    context = _context(
        "MODEL (name test.embedded, kind EMBEDDED); SELECT 1 AS id;",
        "MODEL (name test.downstream); SELECT id FROM test.embedded;",
    )
    manifest = build_manifest(context)
    nodes = {node["name"]: node for node in manifest["nodes"].values()}
    embedded = nodes["embedded"]
    downstream = nodes["downstream"]
    assert embedded["relation_name"] is None
    assert downstream["depends_on"]["nodes"] == [embedded["unique_id"]]
    assert manifest["child_map"][embedded["unique_id"]] == [downstream["unique_id"]]
    assert embedded["unique_id"] not in build_catalog(manifest)["nodes"]


def test_duplicate_names_have_distinct_ids_and_dependencies():
    context = _context(
        "MODEL (name first.orders); SELECT 1 AS id;",
        "MODEL (name second.orders); SELECT id FROM first.orders;",
    )
    manifest = build_manifest(context)
    nodes = {node["schema"]: node for node in manifest["nodes"].values()}
    assert nodes["first"]["unique_id"] != nodes["second"]["unique_id"]
    assert nodes["second"]["depends_on"]["nodes"] == [nodes["first"]["unique_id"]]
