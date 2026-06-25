import sys
from pathlib import Path

# 将项目根目录加入 sys.path, 使 `from aqa import ...` 可用
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
