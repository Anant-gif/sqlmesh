"""Export project metadata in the shape of the commonly consumed dbt manifest fields.

This is a metadata interoperability format, not a dbt state artifact. In particular,
the exported nodes cannot be used by dbt's state comparison or execution commands.
"""

from __future__ import annotations

import hashlib
import re
import typing as t
from datetime import datetime, timezone
from pathlib import Path

from sqlglot import exp

from sqlmesh import __version__
from sqlmesh.core.context import Context
from sqlmesh.core.model import Model


def _identifier(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", value) or "sqlmesh"


def _resource_id(resource_type: str, package: str, name: str, fqn: str) -> str:
    # SQLMesh supports identically named tables in different schemas. A short digest
    # keeps dbt-style identifiers distinct without making them depend on file paths.
    digest = hashlib.sha256(fqn.encode("utf-8")).hexdigest()[:12]
    return f"{resource_type}.{package}.{_identifier(name)}_{digest}"


def _source_id(package: str, table: exp.Table, fqn: str) -> str:
    digest = hashlib.sha256(fqn.encode("utf-8")).hexdigest()[:12]
    return (
        f"source.{package}.{_identifier(table.db or 'external')}.{_identifier(table.name)}_{digest}"
    )


def _file_path(model: Model, root: Path) -> str:
    if model._path is None:
        return ""
    try:
        return model._path.relative_to(root).as_posix()
    except ValueError:
        return model._path.as_posix()


def build_manifest(context: Context) -> t.Dict[str, t.Any]:
    """Build a project metadata export without accessing the SQLMesh state store.

    ``relation_name`` refers to the logical production relation. The actual relation
    in a SQLMesh virtual environment can differ from this name.
    """
    project = _identifier(context.config.project or "sqlmesh")
    models = sorted(context.models.values(), key=lambda model: model.fqn)
    model_by_name = {model.fqn: model for model in models}
    ids = {
        model.fqn: (
            _source_id(
                _identifier(model.project or project), model.fully_qualified_table, model.fqn
            )
            if model.kind.is_external
            else _resource_id(
                "seed" if model.kind.is_seed else "model",
                _identifier(model.project or project),
                model.fully_qualified_table.name,
                model.fqn,
            )
        )
        for model in models
    }
    nodes: t.Dict[str, t.Any] = {}
    sources: t.Dict[str, t.Any] = {}
    parent_map: t.Dict[str, t.List[str]] = {}
    child_map: t.Dict[str, t.List[str]] = {}

    def columns_for(model: Model) -> t.Dict[str, t.Dict[str, t.Any]]:
        types = model.columns_to_types or {}
        return {
            column: {
                "name": column,
                "description": model.column_descriptions.get(column, ""),
                "data_type": types[column].sql(dialect=model.dialect) if column in types else None,
            }
            for column in sorted(types.keys() | model.column_descriptions.keys())
        }

    def source_for(name: str, model: t.Optional[Model] = None) -> str:
        table = model.fully_qualified_table if model else exp.to_table(name)
        fqn = model.fqn if model else name
        package = _identifier(model.project or project) if model else project
        source_id = ids[fqn] if model else _source_id(package, table, fqn)
        if source_id not in sources:
            sources[source_id] = {
                "unique_id": source_id,
                "resource_type": "source",
                "package_name": package,
                "source_name": table.db or "external",
                "name": table.name,
                "identifier": table.name,
                "database": table.catalog or None,
                "schema": table.db or "",
                "relation_name": fqn,
                "description": model.description or "" if model else "",
                "columns": columns_for(model) if model else {},
                "original_file_path": _file_path(model, context.path) if model else "",
                "meta": {"sqlmesh": {"owner": model.owner}} if model and model.owner else {},
            }
            parent_map[source_id] = []
            child_map[source_id] = []
        return source_id

    for model in models:
        if model.kind.is_external:
            source_for(model.fqn, model)
            continue

        table = model.fully_qualified_table
        node_id = ids[model.fqn]
        resource_type = "seed" if model.kind.is_seed else "model"
        materialized = (
            "ephemeral"
            if model.kind.is_embedded
            else "view"
            if model.kind.is_view
            else "incremental"
            if model.kind.is_incremental
            else "table"
        )
        file_path = _file_path(model, context.path)
        nodes[node_id] = {
            "unique_id": node_id,
            "resource_type": resource_type,
            "package_name": _identifier(model.project or project),
            "name": table.name,
            "alias": table.name,
            "database": table.catalog or None,
            "schema": table.db or "",
            "relation_name": None if model.kind.is_embedded else model.fqn,
            "description": model.description or "",
            "columns": columns_for(model),
            "tags": list(model.tags),
            "path": file_path,
            "original_file_path": file_path,
            "fqn": [
                _identifier(model.project or project),
                *(table.db or "").split("."),
                table.name,
            ],
            "config": {"materialized": materialized, "enabled": model.enabled},
            "meta": {"sqlmesh": {"kind": model.kind.name, "owner": model.owner}},
            "depends_on": {"nodes": [], "macros": []},
        }
        parent_map[node_id] = []
        child_map[node_id] = []

    for model in models:
        if model.kind.is_external:
            continue
        node_id = ids[model.fqn]
        for dependency in sorted(model.depends_on):
            upstream = model_by_name.get(dependency)
            parent_id = (
                source_for(dependency, upstream)
                if upstream is None or upstream.kind.is_external
                else ids[dependency]
            )
            parent_map[node_id].append(parent_id)
            child_map[parent_id].append(node_id)
        nodes[node_id]["depends_on"]["nodes"] = parent_map[node_id]

    return {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "project_name": project,
            "sqlmesh_version": __version__,
            "format": "sqlmesh-dbt-metadata-v1",
        },
        "nodes": nodes,
        "sources": sources,
        "parent_map": parent_map,
        "child_map": {key: sorted(value) for key, value in child_map.items()},
        "macros": {},
        "docs": {},
        "exposures": {},
        "metrics": {},
        "groups": {},
        "selectors": {},
        "disabled": {},
        "group_map": {},
        "saved_queries": {},
        "semantic_models": {},
        "unit_tests": {},
    }
