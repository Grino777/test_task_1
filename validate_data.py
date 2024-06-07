"""Validate Module"""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel


class InputData(BaseModel):
    """Validate class"""

    dt_from: datetime
    dt_upto: datetime
    group_type: Literal["hour", "day", "week", "month"]
