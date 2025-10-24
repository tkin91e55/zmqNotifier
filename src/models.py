"""Pydantic models for data validation and type safety."""

from datetime import datetime
from decimal import Decimal
from typing import Union, Optional
from pydantic import BaseModel, Field, field_validator


class TickData(BaseModel):
    """Tick data model with validation."""

    datetime: datetime
    bid: Decimal = Field(..., gt=0, description="Bid price must be positive")
    ask: Decimal = Field(..., gt=0, description="Ask price must be positive")

    @field_validator('ask')
    @classmethod
    def ask_must_be_greater_than_bid(cls, v, values):
        """Validate that ask price is greater than bid price."""
        if 'bid' in values and v <= values['bid']:
            raise ValueError('Ask price must be greater than bid price')
        return v


class OHLCData(BaseModel):
    """OHLC bar data model with validation."""

    datetime: datetime
    open: Decimal = Field(..., gt=0, description="Open price must be positive")
    high: Decimal = Field(..., gt=0, description="High price must be positive")
    low: Decimal = Field(..., gt=0, description="Low price must be positive")
    close: Decimal = Field(..., gt=0, description="Close price must be positive")
    volume: int = Field(..., ge=0, description="Volume must be non-negative")

    @field_validator('high')
    @classmethod
    def high_must_be_highest(cls, v, values):
        """Validate that high is the highest price."""
        if 'low' in values and v < values['low']:
            raise ValueError('High must be >= low')
        if 'open' in values and v < values['open']:
            raise ValueError('High must be >= open')
        if 'close' in values and v < values['close']:
            raise ValueError('High must be >= close')
        return v

    @field_validator('low')
    @classmethod
    def low_must_be_lowest(cls, v, values):
        """Validate that low is the lowest price."""
        if 'high' in values and v > values['high']:
            raise ValueError('Low must be <= high')
        if 'open' in values and v > values['open']:
            raise ValueError('Low must be <= open')
        if 'close' in values and v > values['close']:
            raise ValueError('Low must be <= close')
        return v

    # @validator('datetime')
    # def datetime_must_be_recent(cls, v):
    #     """Validate that bar data is not too old."""
    #     from .config import settings
    #     now = datetime.utcnow()
    #     if (now - v).total_seconds() > settings.validation.tick_staleness_seconds * 60:  # Allow older for bars
    #         raise ValueError(f'Bar data is too old: {v}')
    #     return v


class MarketDataMessage(BaseModel):
    """Wrapper for incoming market data messages."""

    symbol: str = Field(..., min_length=1, max_length=20, description="Trading symbol")
    timeframe: Optional[str] = Field(None, description="Timeframe (None for tick data)")
    data: Union[TickData, OHLCData] = Field(..., description="Market data content")

    @field_validator('symbol')
    @classmethod
    def validate_symbol(cls, v):
        """Validate that symbol is supported."""
        from .config import settings
        if v not in settings.validation.supported_symbols:
            raise ValueError(f'Unsupported symbol: {v}')
        return v

    @field_validator('timeframe')
    @classmethod
    def validate_timeframe(cls, v):
        """Validate timeframe format."""
        if v is not None:
            from .config import settings
            if v not in settings.validation.supported_timeframes:
                raise ValueError(f'Invalid timeframe: {v}')
        return v
