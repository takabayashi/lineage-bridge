# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for the Databricks External Lineage + External Metadata client."""

from __future__ import annotations

import httpx
import pytest
import respx

from lineage_bridge.clients.databricks_external_lineage import (
    SYSTEM_TYPE_CONFLUENT,
    ExternalMetadataRef,
    TableRef,
    create_lineage_relationship,
    list_external_metadata,
    upsert_external_metadata,
)

WORKSPACE = "https://acme-prod.cloud.databricks.com"
META_URL = f"{WORKSPACE}/api/2.0/lineage-tracking/external-metadata"
LINEAGE_URL = f"{WORKSPACE}/api/2.0/lineage-tracking/external-lineage"


@pytest.fixture()
async def client():
    async with httpx.AsyncClient(base_url=WORKSPACE) as c:
        yield c


class TestUpsertExternalMetadata:
    @respx.mock
    async def test_creates_with_full_payload(self, client):
        route = respx.post(META_URL).mock(
            return_value=httpx.Response(
                200, json={"name": "confluent_env_x_orders", "system_type": "CONFLUENT"}
            )
        )
        result = await upsert_external_metadata(
            client,
            name="confluent_env_x_orders",
            system_type=SYSTEM_TYPE_CONFLUENT,
            entity_type="TABLE",
            description="topic orders",
            url="https://confluent.cloud/.../orders",
            owner="lineage-bridge",
        )
        assert result is not None
        body = route.calls.last.request.content
        import json as _json

        sent = _json.loads(body)
        assert sent["name"] == "confluent_env_x_orders"
        assert sent["system_type"] == "CONFLUENT"
        assert sent["entity_type"] == "TABLE"
        assert sent["description"] == "topic orders"
        assert sent["url"] == "https://confluent.cloud/.../orders"
        assert sent["owner"] == "lineage-bridge"

    @respx.mock
    async def test_409_falls_back_to_patch(self, client):
        """RESOURCE_ALREADY_EXISTS → PATCH the mutable fields. Idempotent re-runs
        keep the URL / description in sync with the current Confluent state."""
        respx.post(META_URL).mock(
            return_value=httpx.Response(409, json={"error_code": "RESOURCE_ALREADY_EXISTS"})
        )
        patch_route = respx.patch(f"{META_URL}/confluent_orders").mock(
            return_value=httpx.Response(200, json={"name": "confluent_orders"})
        )
        result = await upsert_external_metadata(
            client,
            name="confluent_orders",
            system_type=SYSTEM_TYPE_CONFLUENT,
            description="updated topic orders",
            url="https://new",
        )
        assert result is not None
        assert patch_route.called

    @respx.mock
    async def test_403_returns_none_with_warning(self, client, caplog):
        """Permission denied → log a single warning, return None. Caller
        downgrades to PushResult.skipped instead of erroring."""
        respx.post(META_URL).mock(
            return_value=httpx.Response(
                403,
                json={
                    "error_code": "PERMISSION_DENIED",
                    "message": "User does not have CREATE EXTERNAL METADATA on Metastore 'foo'.",
                },
            )
        )
        with caplog.at_level("WARNING"):
            result = await upsert_external_metadata(
                client,
                name="confluent_orders",
                system_type=SYSTEM_TYPE_CONFLUENT,
            )
        assert result is None
        assert any("CREATE_EXTERNAL_METADATA" in r.message for r in caplog.records)

    @respx.mock
    async def test_network_error_returns_none(self, client):
        respx.post(META_URL).mock(side_effect=httpx.ConnectError("boom"))
        assert (
            await upsert_external_metadata(client, name="x", system_type=SYSTEM_TYPE_CONFLUENT)
            is None
        )


class TestCreateLineageRelationship:
    @respx.mock
    async def test_external_metadata_to_table(self, client):
        route = respx.post(LINEAGE_URL).mock(return_value=httpx.Response(200, json={"id": "rel-1"}))
        result = await create_lineage_relationship(
            client,
            source=ExternalMetadataRef(name="confluent_orders"),
            target=TableRef(name="cat.sch.orders_v2"),
        )
        assert result is not None
        import json as _json

        sent = _json.loads(route.calls.last.request.content)
        assert sent["source"] == {"external_metadata": {"name": "confluent_orders"}}
        assert sent["target"] == {"table": {"name": "cat.sch.orders_v2"}}

    @respx.mock
    async def test_409_already_exists_is_a_noop(self, client):
        """The (source, target) pair already has a relationship — we treat
        it as success (return None, don't bump the error counter)."""
        respx.post(LINEAGE_URL).mock(
            return_value=httpx.Response(409, json={"error_code": "RESOURCE_ALREADY_EXISTS"})
        )
        result = await create_lineage_relationship(
            client,
            source=ExternalMetadataRef(name="x"),
            target=TableRef(name="a.b.c"),
        )
        assert result is None

    @respx.mock
    async def test_403_returns_none(self, client):
        respx.post(LINEAGE_URL).mock(return_value=httpx.Response(403, json={}))
        result = await create_lineage_relationship(
            client,
            source=ExternalMetadataRef(name="x"),
            target=TableRef(name="a.b.c"),
        )
        assert result is None

    async def test_unsupported_ref_type_raises(self, client):
        """Sanity: only ExternalMetadataRef + TableRef are supported. A
        future model_version branch would need an explicit dataclass."""

        class FakeRef:
            pass

        with pytest.raises(TypeError, match="Unsupported lineage reference type"):
            await create_lineage_relationship(
                client, source=FakeRef(), target=TableRef(name="a.b.c")
            )


class TestListExternalMetadata:
    @respx.mock
    async def test_paginates(self, client):
        respx.get(META_URL).mock(
            side_effect=[
                httpx.Response(
                    200,
                    json={
                        "external_metadata": [{"name": "a"}, {"name": "b"}],
                        "next_page_token": "tok",
                    },
                ),
                httpx.Response(200, json={"external_metadata": [{"name": "c"}]}),
            ]
        )
        items = await list_external_metadata(client)
        assert [i["name"] for i in items] == ["a", "b", "c"]

    @respx.mock
    async def test_empty(self, client):
        respx.get(META_URL).mock(return_value=httpx.Response(200, json={}))
        assert await list_external_metadata(client) == []
