"""
Pydantic models for data validation and type safety.

| Symbol | price    | Symbol | price    | Symbol | price    | Symbol | price    |
| :---   | :---     | :---   | :---     | :---   | :---     | :---   | :---     |
| USDCAD | 1.40130  | AUDUSD | 0.65408  | EURUSD | 1.15370  | GBPUSD | 1.31476  |
| NZDUSD | 0.57132  | USDCHF | 0.80459  | USDJPY | 154.015  | EURAUD | 1.76213  |
| EURCAD | 1.61585  | EURCHF | 0.92725  | EURGBP | 0.87720  | EURJPY | 177.616  |
| EURNZD | 2.01384  | GBPAUD | 2.00696  | GBPCAD | 1.84157  | GBPCHF | 1.05649  |
| GBPJPY | 202.336  | GBPNZD | 2.29579  | AUDCAD | 0.91581  | AUDCHF | 0.52578  |
| AUDJPY | 100.676  | AUDNZD | 1.14202  | NZDCAD | 0.80049  | NZDCHF | 0.45899  |
| NZDJPY | 87.958   | CADCHF | 0.57318  | CADJPY | 109.818  | CHFJPY | 191.266  |
| BTCUSD | 110043.50| XAUUSD | 4002.25  | USOUSD | 61.106   |
"""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel
from pydantic import Field
from pydantic import field_serializer
from pydantic import field_validator


def into_pip(price_diff: Decimal) -> int:
    """Get the second last decimal place multiplier for pipstep calculation."""
    num_decimal_places = abs(int(price_diff.as_tuple().exponent))
    if num_decimal_places > 0:
        return price_diff * 10 ** (num_decimal_places - 1)
    return int(price_diff) // 10


class TickData(BaseModel):
    """Tick data model with validation."""

    datetime: datetime
    bid: Decimal = Field(..., gt=0, description="Bid price must be positive")
    ask: Decimal = Field(..., gt=0, description="Ask price must be positive")

    @field_serializer("datetime")
    def serialize_datetime(self, dt: datetime) -> str:
        """Serialize datetime as 'YYYY-MM-DD HH:MM:SS+TZ:TZ' format."""
        return dt.isoformat(sep=" ")

    @field_validator("ask")
    @classmethod
    def ask_must_be_greater_than_bid(cls, v, info):
        """Validate that ask price is greater than bid price."""
        if info.data.get("bid") and v <= info.data["bid"]:
            msg = "Ask price must be greater than bid price"
            raise ValueError(msg)
        return v


class OHLCData(BaseModel):
    """OHLC bar data model with validation."""

    datetime: datetime
    open: Decimal = Field(..., gt=0, description="Open price must be positive")
    high: Decimal = Field(..., gt=0, description="High price must be positive")
    low: Decimal = Field(..., gt=0, description="Low price must be positive")
    close: Decimal = Field(..., gt=0, description="Close price must be positive")
    volume: int = Field(..., ge=0, description="Volume must be non-negative")

    @field_serializer("datetime")
    def serialize_datetime(self, dt: datetime) -> str:
        """Serialize datetime as 'YYYY-MM-DD HH:MM:SS+TZ:TZ' format."""
        return dt.isoformat(sep=" ")

    @field_validator("high")
    @classmethod
    def high_must_be_highest(cls, v, info):
        """Validate that high is the highest price."""
        data = info.data
        if data.get("low") and v < data["low"]:
            msg = "High must be >= low"
            raise ValueError(msg)
        if data.get("open") and v < data["open"]:
            msg = "High must be >= open"
            raise ValueError(msg)
        if data.get("close") and v < data["close"]:
            msg = "High must be >= close"
            raise ValueError(msg)
        return v

    @field_validator("low")
    @classmethod
    def low_must_be_lowest(cls, v, info):
        """Validate that low is the lowest price."""
        data = info.data
        if data.get("high") and v > data["high"]:
            msg = "Low must be <= high"
            raise ValueError(msg)
        if data.get("open") and v > data["open"]:
            msg = "Low must be <= open"
            raise ValueError(msg)
        if data.get("close") and v > data["close"]:
            msg = "Low must be <= close"
            raise ValueError(msg)
        return v


class MarketDataMessage(BaseModel):
    """Wrapper for incoming market data messages."""

    symbol: str = Field(..., min_length=1, max_length=20, description="Trading symbol")
    timeframe: str | None = Field(None, description="Timeframe (None for tick data)")
    data: TickData | OHLCData = Field(..., description="Market data content")

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, v):
        """Validate that symbol is supported."""
        from .config import settings

        if v not in settings.validation.supported_symbols:
            msg = f"Unsupported symbol: {v}"
            raise ValueError(msg)
        return v

    @field_validator("timeframe")
    @classmethod
    def validate_timeframe(cls, v):
        """Validate timeframe format."""
        if v is not None:
            from .config import settings

            if v not in settings.validation.supported_timeframes:
                msg = f"Invalid timeframe: {v}"
                raise ValueError(msg)
        return v
