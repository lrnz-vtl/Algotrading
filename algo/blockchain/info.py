from dataclasses import dataclass
from datetime import datetime

@dataclass
class BlockInfo:
    block: int
    timestamp: datetime