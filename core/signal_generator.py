from dataclasses import dataclass
from enum import Enum
from typing import Optional


class SignalType(Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"
    RESET_UP = "RESET_UP"
    EXPAND_DOWN = "EXPAND_DOWN"


@dataclass
class Signal:
    type: SignalType
    price: float
    reason: str
    metadata: Optional[dict] = None  # Extra info like grid_lines, indicators, etc.


class SignalGenerator:
    """
    Base class for Signal Generators.
    Future implementations (e.g., RSI, MACD) will inherit from this.
    Current Grid logic mainly reacts to price levels, but this structure allows
    us to inject "Why" we are trading.
    """

    def generate_signal(self, current_price: float, context: dict) -> Signal:
        raise NotImplementedError
