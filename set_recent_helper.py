# run set tasks for recentchanges
# flake8: noqa: E402
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.qtparser import dispatch_internal as dispatcher
# 03/13/2026


if __name__ == "__main__":
    dispatcher(sys.argv)
