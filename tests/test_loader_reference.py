"""Integration tests against ocel-standard.org reference logs.

Reference logs are not vendored. Tests skip when the file is absent.
Download with:
    curl -sL -o tests/fixtures/reference/order-management.json \\
        https://zenodo.org/record/8428112/files/order-management.json
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ocelint.loader import load

REFERENCE_DIR = Path(__file__).parent / "fixtures" / "reference"


def _skip_if_missing(name: str) -> Path:
    p = REFERENCE_DIR / name
    if not p.exists():
        pytest.skip(f"reference log not downloaded: {p.name}")
    return p


def test_load_order_management() -> None:
    """Load the canonical Order Management OCEL 2.0 JSON log."""
    log = load(_skip_if_missing("order-management.json"))

    assert log.source_format == "json"
    assert log.parse_warnings == []
    assert len(log.events) == 21008
    assert len(log.objects) == 10840
    assert len(log.relations_e2o) == 147463
    assert len(log.relations_o2o) == 28391
    assert len(log.event_types) == 11
    assert len(log.object_types) == 6
    assert set(log.event_types["name"]) == {
        "pay order", "package delivered", "place order", "create package",
        "confirm order", "item out of stock", "pick item", "send package",
        "payment reminder", "reorder item", "failed delivery",
    }
    assert set(log.object_types["name"]) == {
        "orders", "items", "packages", "customers", "products", "employees",
    }


def test_load_order_management_sqlite() -> None:
    """Load the Order Management OCEL 2.0 SQLite log; counts must match the JSON.

    Download with:
        curl -sL -o tests/fixtures/reference/order-management.sqlite \\
            https://zenodo.org/record/8428112/files/order-management.sqlite
    """
    log = load(_skip_if_missing("order-management.sqlite"))

    assert log.source_format == "sqlite"
    assert log.parse_warnings == []
    assert len(log.events) == 21008
    assert len(log.objects) == 10840
    assert len(log.relations_e2o) == 147463
    assert len(log.relations_o2o) == 28391
    assert len(log.event_types) == 11
    assert len(log.object_types) == 6
    assert set(log.event_types["name"]) == {
        "pay order", "package delivered", "place order", "create package",
        "confirm order", "item out of stock", "pick item", "send package",
        "payment reminder", "reorder item", "failed delivery",
    }
    assert set(log.object_types["name"]) == {
        "orders", "items", "packages", "customers", "products", "employees",
    }


def test_load_lrms_o2c_xml() -> None:
    """Load the LRMs Order-to-Cash OCEL 2.0 XML log.

    Download with:
        curl -sL -o tests/fixtures/reference/01_o2c.xml \\
            https://zenodo.org/record/13879980/files/01_o2c.xml
    """
    log = load(_skip_if_missing("01_o2c.xml"))

    assert log.source_format == "xml"
    assert log.parse_warnings == []
    assert len(log.events) == 28278
    assert len(log.objects) == 8819
    assert len(log.relations_e2o) == 55360
    assert len(log.relations_o2o) == 0
    assert len(log.event_types) == 22
    assert len(log.object_types) == 9
    assert len(log.attribute_decls) == 18
