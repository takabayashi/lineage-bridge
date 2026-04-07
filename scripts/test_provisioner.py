#!/usr/bin/env python3
"""Test script for key provisioning — exercises SA creation, key provisioning, and revocation."""

import asyncio
import logging
import sys

from lineage_bridge.clients.base import ConfluentClient
from lineage_bridge.config.provisioner import KeyProvisioner
from lineage_bridge.config.settings import Settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

PREFIX = "lb-test"


async def main():
    settings = Settings()  # type: ignore[call-arg]

    cloud = ConfluentClient(
        "https://api.confluent.cloud",
        settings.confluent_cloud_api_key,
        settings.confluent_cloud_api_secret,
    )

    provisioner = KeyProvisioner(cloud, prefix=PREFIX)

    try:
        # ── 1. List environments and clusters ─────────────────────
        print("\n═══ Step 1: Discover environments & clusters ═══")
        envs = await cloud.paginate("/org/v2/environments")
        for env in envs:
            eid = env.get("id", "")
            name = env.get("display_name", "")
            print(f"  Environment: {name} ({eid})")

            clusters = await cloud.paginate("/cmk/v2/clusters", params={"environment": eid})
            for c in clusters:
                cid = c.get("id", "")
                cname = c.get("spec", {}).get("display_name", "")
                print(f"    Cluster: {cname} ({cid})")

            # Check for SR
            try:
                sr_items = await cloud.paginate("/srcm/v2/clusters", params={"environment": eid})
                for sr in sr_items:
                    sr_id = sr.get("id", "")
                    print(f"    Schema Registry: {sr_id}")
            except Exception:
                print("    Schema Registry: not found")

        if not envs:
            print("  No environments found!")
            return

        # Use the first env/cluster for testing
        test_env_id = envs[0].get("id", "")
        test_clusters = await cloud.paginate(
            "/cmk/v2/clusters", params={"environment": test_env_id}
        )
        if not test_clusters:
            print(f"  No clusters in {test_env_id}")
            return
        test_cluster_id = test_clusters[0].get("id", "")
        print(f"\n  Using: env={test_env_id}, cluster={test_cluster_id}")

        # ── 2. Create/find service account ────────────────────────
        print("\n═══ Step 2: Ensure service account ═══")
        sa_id = await provisioner.ensure_service_account()
        print(f"  Service account: {sa_id}")

        # Verify it exists
        sa_id_2 = await provisioner.ensure_service_account()
        assert sa_id == sa_id_2, "Second call should return same SA"
        print("  Idempotent check: OK (same SA returned)")

        # ── 3. Provision a Kafka cluster key ──────────────────────
        print("\n═══ Step 3: Provision Kafka cluster key ═══")
        kafka_key = await provisioner.provision_cluster_key(
            cluster_id=test_cluster_id,
            environment_id=test_env_id,
            owner_id=sa_id,
        )
        print(f"  Key ID: {kafka_key.key_id}")
        print(f"  API Key: {kafka_key.api_key[:8]}...")
        print(f"  Resource: {kafka_key.resource_id}")
        print(f"  Owner: {kafka_key.owner_id}")

        # ── 4. Check cache hit ────────────────────────────────────
        print("\n═══ Step 4: Verify cache hit ═══")
        kafka_key_2 = await provisioner.provision_cluster_key(
            cluster_id=test_cluster_id,
            environment_id=test_env_id,
            owner_id=sa_id,
        )
        assert kafka_key_2.key_id == kafka_key.key_id, "Should return cached key"
        print("  Cache hit: OK (same key returned)")

        # ── 5. Provision SR key if available ──────────────────────
        print("\n═══ Step 5: Provision SR key (if available) ═══")
        try:
            sr_items = await cloud.paginate(
                "/srcm/v2/clusters", params={"environment": test_env_id}
            )
            if sr_items:
                sr_cluster_id = sr_items[0].get("id", "")
                sr_key = await provisioner.provision_sr_key(
                    sr_cluster_id=sr_cluster_id,
                    environment_id=test_env_id,
                    owner_id=sa_id,
                )
                print(f"  SR Key ID: {sr_key.key_id}")
                print(f"  SR API Key: {sr_key.api_key[:8]}...")
            else:
                print("  No Schema Registry found — skipped")
        except Exception as exc:
            print(f"  SR provisioning failed: {exc}")

        # ── 6. List all provisioned keys ──────────────────────────
        print("\n═══ Step 6: List provisioned keys ═══")
        keys = await provisioner.list_provisioned_keys()
        print(f"  Found {len(keys)} key(s) with prefix '{PREFIX}':")
        for k in keys:
            kid = k.get("id", "")
            kname = k.get("spec", {}).get("display_name", "")
            resource = k.get("spec", {}).get("resource", {}).get("id", "")
            print(f"    {kname} ({kid}) → {resource}")

        # ── 7. Check cached keys ─────────────────────────────────
        print("\n═══ Step 7: Check local cache ═══")
        cached = KeyProvisioner.get_all_cached_keys()
        print(f"  {len(cached)} cached key(s):")
        for name, entry in cached.items():
            print(f"    {name}: key_id={entry.get('key_id', '')}")

        # ── 8. Revoke all ─────────────────────────────────────────
        print("\n═══ Step 8: Revoke all provisioned keys ═══")
        await provisioner.revoke_all()
        print("  All test keys revoked")

        # Verify revocation
        remaining = await provisioner.list_provisioned_keys()
        print(f"  Remaining keys with prefix '{PREFIX}': {len(remaining)}")

        # ── 9. Clean up service account ───────────────────────────
        print("\n═══ Step 9: Delete test service account ═══")
        await provisioner.delete_service_account()
        print("  Service account deleted")

        print("\n✓ All provisioning tests passed!")

    except KeyboardInterrupt:
        print("\n  Keeping provisioned keys.")
    except Exception:
        logger.exception("Test failed")
        sys.exit(1)
    finally:
        await cloud.close()


if __name__ == "__main__":
    asyncio.run(main())
