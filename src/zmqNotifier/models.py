"""Pydantic models for data validation and type safety."""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel
from pydantic import Field
from pydantic import field_validator


class TickData(BaseModel):
    """Tick data model with validation."""

    datetime: datetime
    bid: Decimal = Field(..., gt=0, description="Bid price must be positive")
    ask: Decimal = Field(..., gt=0, description="Ask price must be positive")

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

    # @validator('datetime')
    # def datetime_must_be_recent(cls, v):
    #     """Validate that bar data is not too old."""
    #     from .config import settings
    #     now = datetime.utcnow()
    #     max_age = settings.validation.tick_staleness_seconds * 60
    #     if (now - v).total_seconds() > max_age:  # Allow older for bars
    #         raise ValueError(f'Bar data is too old: {v}')
    #     return v

    # TODO it was found the TF data could be invalid from MT4 server,.
    # INVALID_COUNT_THRESHOLD = 30
    # def is_ohlc_same(self, symbol, ohlc):
    #     is_same = (ohlc["open"] == ohlc["high"] == ohlc["low"] == ohlc["close"])
    #     cnt = self._invalid_count
    #     cnt[symbol] = cnt[symbol] + 1 if is_same else 0
    #     if cnt[symbol] > self.INVALID_COUNT_THRESHOLD:
    #         print(f"Warning: {symbol} has {self._invalid_count[symbol]}"
    #                 "consecutive invalid OHLC data.")
    #         self._proxy.unsubscribe(symbol)



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
