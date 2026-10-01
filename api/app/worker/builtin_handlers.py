from __future__ import annotations

from .handlers import HandlerRegistry
from .registry import register_handler
from .server_snapshot_handler import ServerSnapshotActionHandler
from .swift_object_versioning_handler import SwiftObjectVersioningActionHandler
from .volume_backup_full_handler import VolumeBackupFullActionHandler
from .volume_backup_incremental_handler import VolumeBackupIncrementalActionHandler


def register_builtin_handlers(registry: HandlerRegistry) -> None:
    register_handler(registry, action_type="server_snapshot")(ServerSnapshotActionHandler)
    register_handler(registry, action_type="swift_object_versioning")(
        SwiftObjectVersioningActionHandler
    )
    register_handler(registry, action_type="volume_backup_full")(VolumeBackupFullActionHandler)
    register_handler(registry, action_type="volume_backup_incremental")(
        VolumeBackupIncrementalActionHandler
    )
