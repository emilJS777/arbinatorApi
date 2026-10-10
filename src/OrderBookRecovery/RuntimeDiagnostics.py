import os
import socket
from datetime import datetime


def runtime_diagnostics():
    """Identify the responding process, not an assumed shared scanner worker."""
    return {
        "state_contract_version": "paper-lifecycle-state-v2",
        "instance": socket.gethostname(),
        "process_id": os.getpid(),
        "build_revision": os.environ.get("BUILD_REVISION") or None,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "snapshot_scope": "current_process",
        "worker_scope": "current_process",
    }
