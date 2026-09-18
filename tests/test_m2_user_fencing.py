import asyncio
import logging
import unittest
from unittest.mock import AsyncMock

from PasarGuardNodeBridge.common.service_pb2 import User
from PasarGuardNodeBridge.controller import Controller
from PasarGuardNodeBridge.storage import InMemoryUserSyncStore


class NotifyingLock:
    """An asyncio lock that exposes a deterministic acquisition attempt."""

    def __init__(self):
        self._lock = asyncio.Lock()
        self.attempted = asyncio.Event()

    async def acquire(self):
        self.attempted.set()
        return await self._lock.acquire()

    def release(self):
        self._lock.release()

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.release()


def make_controller(store: InMemoryUserSyncStore, gate=None) -> Controller:
    controller = object.__new__(Controller)
    controller.node_id = "node-m2"
    controller._user_sync_store = store
    controller._user_sync_gate = gate or asyncio.Lock()
    controller._work_available = asyncio.Event()
    controller._internal_timeout = 1
    controller._sync_batch_users = AsyncMock(return_value=[])
    controller.sync_users_chunked = AsyncMock(return_value=[])
    controller._increment_user_sync_failure = AsyncMock()
    controller._reset_user_sync_failure_count = AsyncMock()
    controller.logger = logging.getLogger("m2-user-fencing-test")
    return controller


class M2UserFencingTests(unittest.IsolatedAsyncioTestCase):
    async def test_full_revoke_blocks_and_discards_delayed_claim(self):
        store = InMemoryUserSyncStore()
        await store.enqueue_users("node-m2", [User(email="revoked@example.com", inbounds=["obsolete"])])
        claimed = await store.claim_users("node-m2", "old-writer", 1, 30)
        gate = NotifyingLock()
        controller = make_controller(store, gate)

        await gate.acquire()
        gate.attempted.clear()
        dispatch = asyncio.create_task(controller._dispatch_claimed_users(claimed, False, 0, 1))
        await asyncio.wait_for(gate.attempted.wait(), 1)

        await controller.flush_pending_users()
        gate.release()
        self.assertEqual(await dispatch, 1.0)
        controller._sync_batch_users.assert_not_awaited()
        self.assertEqual(await store.claim_users("node-m2", "new-writer", 10, 30), [])

    async def test_claim_is_re_resolved_to_latest_intent_before_stream_send(self):
        store = InMemoryUserSyncStore()
        await store.enqueue_users("node-m2", [User(email="user@example.com", inbounds=["obsolete"])])
        claimed = await store.claim_users("node-m2", "old-writer", 1, 30)
        await store.enqueue_users("node-m2", [User(email="user@example.com", inbounds=["latest"])])
        controller = make_controller(store)

        self.assertEqual(await controller._dispatch_claimed_users(claimed, False, 0, 1), 1.0)
        sent = controller._sync_batch_users.await_args.args[0]
        self.assertEqual(list(sent[0].inbounds), ["latest"])

    async def test_failed_chunked_send_requeues_current_intent(self):
        store = InMemoryUserSyncStore()
        old_users = [User(email=f"user-{index}@example.com", inbounds=["obsolete"]) for index in range(1000)]
        await store.enqueue_users("node-m2", old_users)
        claimed = await store.claim_users("node-m2", "old-writer", 1000, 30)
        latest = User(email="user-0@example.com", inbounds=["latest"])
        await store.enqueue_users("node-m2", [latest])
        controller = make_controller(store)
        controller.sync_users_chunked = AsyncMock(side_effect=lambda users, **_: users)

        await controller._dispatch_claimed_users(claimed, True, 0, 1)
        retry = await store.claim_users("node-m2", "new-writer", 1001, 30)
        retried = {item.user.email: list(item.user.inbounds) for item in retry}
        self.assertEqual(retried["user-0@example.com"], ["latest"])


if __name__ == "__main__":
    unittest.main()
