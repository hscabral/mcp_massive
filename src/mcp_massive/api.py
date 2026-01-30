"""
FastAPI REST wrapper for Massive.com financial data API.
Exposes MCP tools as REST endpoints for integration with other services.
"""
import os
from typing import Optional, List
from datetime import date, datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from massive import RESTClient
from dotenv import load_dotenv

from .formatters import json_to_csv

load_dotenv()

MASSIVE_API_KEY = os.environ.get("MASSIVE_API_KEY", "")
if not MASSIVE_API_KEY:
    print("Warning: MASSIVE_API_KEY environment variable not set.")

massive_client: Optional[RESTClient] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global massive_client
    massive_client = RESTClient(MASSIVE_API_KEY)
    massive_client.headers["User-Agent"] += " MCP-Massive-REST/1.0"
    yield
    massive_client = None


app = FastAPI(
    title="Massive Financial Data API",
    description="REST API wrapper for Massive.com financial market data",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Response models
class ApiResponse(BaseModel):
    success: bool
    data: Optional[str] = None
    error: Optional[str] = None


def success_response(data: str) -> dict:
    return {"success": True, "data": data, "error": None}


def error_response(error: str) -> dict:
    return {"success": False, "data": None, "error": error}


# Health check
@app.get("/health")
async def health_check():
    return {"status": "healthy", "api_key_configured": bool(MASSIVE_API_KEY)}


# ==================== Aggregates ====================

@app.get("/api/aggs/{ticker}", response_model=ApiResponse)
async def get_aggs(
    ticker: str,
    multiplier: int = Query(1, description="Size of the timespan multiplier"),
    timespan: str = Query("day", description="second, minute, hour, day, week, month, quarter, year"),
    from_date: str = Query(..., alias="from", description="Start date (YYYY-MM-DD)"),
    to_date: str = Query(..., alias="to", description="End date (YYYY-MM-DD)"),
    adjusted: Optional[bool] = None,
    sort: Optional[str] = None,
    limit: int = Query(50, le=50000),
):
    """Get aggregate bars for a ticker over a date range."""
    try:
        results = massive_client.get_aggs(
            ticker=ticker,
            multiplier=multiplier,
            timespan=timespan,
            from_=from_date,
            to=to_date,
            adjusted=adjusted,
            sort=sort,
            limit=limit,
            raw=True,
        )
        return success_response(json_to_csv(results.data.decode("utf-8")))
    except Exception as e:
        return error_response(str(e))


@app.get("/api/aggs/grouped/{date}", response_model=ApiResponse)
async def get_grouped_daily_aggs(
    date: str,
    adjusted: Optional[bool] = None,
    include_otc: Optional[bool] = None,
    locale: Optional[str] = None,
    market_type: Optional[str] = None,
):
    """Get grouped daily bars for entire market for a specific date."""
    try:
        results = massive_client.get_grouped_daily_aggs(
            date=date,
            adjusted=adjusted,
            include_otc=include_otc,
            locale=locale,
            market_type=market_type,
            raw=True,
        )
        return success_response(json_to_csv(results.data.decode("utf-8")))
    except Exception as e:
        return error_response(str(e))


@app.get("/api/aggs/{ticker}/open-close/{date}", response_model=ApiResponse)
async def get_daily_open_close(
    ticker: str,
    date: str,
    adjusted: Optional[bool] = None,
):
    """Get daily open, close, high, and low for a ticker and date."""
    try:
        results = massive_client.get_daily_open_close_agg(
            ticker=ticker, date=date, adjusted=adjusted, raw=True
        )
        return success_response(json_to_csv(results.data.decode("utf-8")))
    except Exception as e:
        return error_response(str(e))


@app.get("/api/aggs/{ticker}/prev", response_model=ApiResponse)
async def get_previous_close(
    ticker: str,
    adjusted: Optional[bool] = None,
):
    """Get previous day's OHLC for a ticker."""
    try:
        results = massive_client.get_previous_close_agg(
            ticker=ticker, adjusted=adjusted, raw=True
        )
        return success_response(json_to_csv(results.data.decode("utf-8")))
    except Exception as e:
        return error_response(str(e))


# ==================== Trades ====================

@app.get("/api/trades/{ticker}", response_model=ApiResponse)
async def list_trades(
    ticker: str,
    timestamp: Optional[str] = None,
    timestamp_lt: Optional[str] = None,
    timestamp_lte: Optional[str] = None,
    timestamp_gt: Optional[str] = None,
    timestamp_gte: Optional[str] = None,
    limit: int = Query(10, le=50000),
    sort: Optional[str] = None,
    order: Optional[str] = None,
):
    """Get trades for a ticker symbol."""
    try:
        results = massive_client.list_trades(
            ticker=ticker,
            timestamp=timestamp,
            timestamp_lt=timestamp_lt,
            timestamp_lte=timestamp_lte,
            timestamp_gt=timestamp_gt,
            timestamp_gte=timestamp_gte,
            limit=limit,
            sort=sort,
            order=order,
            raw=True,
        )
        return success_response(json_to_csv(results.data.decode("utf-8")))
    except Exception as e:
        return error_response(str(e))


@app.get("/api/trades/{ticker}/last", response_model=ApiResponse)
async def get_last_trade(ticker: str):
    """Get the most recent trade for a ticker."""
    try:
        results = massive_client.get_last_trade(ticker=ticker, raw=True)
        return success_response(json_to_csv(results.data.decode("utf-8")))
    except Exception as e:
        return error_response(str(e))


# ==================== Quotes ====================

@app.get("/api/quotes/{ticker}", response_model=ApiResponse)
async def list_quotes(
    ticker: str,
    timestamp: Optional[str] = None,
    timestamp_lt: Optional[str] = None,
    timestamp_lte: Optional[str] = None,
    timestamp_gt: Optional[str] = None,
    timestamp_gte: Optional[str] = None,
    limit: int = Query(10, le=50000),
    sort: Optional[str] = None,
    order: Optional[str] = None,
):
    """Get quotes for a ticker symbol."""
    try:
        results = massive_client.list_quotes(
            ticker=ticker,
            timestamp=timestamp,
            timestamp_lt=timestamp_lt,
            timestamp_lte=timestamp_lte,
            timestamp_gt=timestamp_gt,
            timestamp_gte=timestamp_gte,
            limit=limit,
            sort=sort,
            order=order,
            raw=True,
        )
        return success_response(json_to_csv(results.data.decode("utf-8")))
    except Exception as e:
        return error_response(str(e))


@app.get("/api/quotes/{ticker}/last", response_model=ApiResponse)
async def get_last_quote(ticker: str):
    """Get the most recent quote for a ticker."""
    try:
        results = massive_client.get_last_quote(ticker=ticker, raw=True)
        return success_response(json_to_csv(results.data.decode("utf-8")))
    except Exception as e:
        return error_response(str(e))


# ==================== Snapshots ====================

@app.get("/api/snapshot/{market_type}/all", response_model=ApiResponse)
async def get_snapshot_all(
    market_type: str,
    tickers: Optional[str] = Query(None, description="Comma-separated tickers"),
    include_otc: Optional[bool] = None,
):
    """Get snapshot of all tickers in a market."""
    try:
        ticker_list = tickers.split(",") if tickers else None
        results = massive_client.get_snapshot_all(
            market_type=market_type,
            tickers=ticker_list,
            include_otc=include_otc,
            raw=True,
        )
        return success_response(json_to_csv(results.data.decode("utf-8")))
    except Exception as e:
        return error_response(str(e))


@app.get("/api/snapshot/{market_type}/{direction}", response_model=ApiResponse)
async def get_snapshot_direction(
    market_type: str,
    direction: str,
    include_otc: Optional[bool] = None,
):
    """Get gainers or losers for a market."""
    try:
        results = massive_client.get_snapshot_direction(
            market_type=market_type,
            direction=direction,
            include_otc=include_otc,
            raw=True,
        )
        return success_response(json_to_csv(results.data.decode("utf-8")))
    except Exception as e:
        return error_response(str(e))


@app.get("/api/snapshot/{market_type}/ticker/{ticker}", response_model=ApiResponse)
async def get_snapshot_ticker(
    market_type: str,
    ticker: str,
):
    """Get snapshot for a specific ticker."""
    try:
        results = massive_client.get_snapshot_ticker(
            market_type=market_type, ticker=ticker, raw=True
        )
        return success_response(json_to_csv(results.data.decode("utf-8")))
    except Exception as e:
        return error_response(str(e))


# ==================== Market Info ====================

@app.get("/api/market/holidays", response_model=ApiResponse)
async def get_market_holidays():
    """Get upcoming market holidays."""
    try:
        results = massive_client.get_market_holidays(raw=True)
        return success_response(json_to_csv(results.data.decode("utf-8")))
    except Exception as e:
        return error_response(str(e))


@app.get("/api/market/status", response_model=ApiResponse)
async def get_market_status():
    """Get current trading status of exchanges."""
    try:
        results = massive_client.get_market_status(raw=True)
        return success_response(json_to_csv(results.data.decode("utf-8")))
    except Exception as e:
        return error_response(str(e))


# ==================== Tickers ====================

@app.get("/api/tickers", response_model=ApiResponse)
async def list_tickers(
    ticker: Optional[str] = None,
    type: Optional[str] = None,
    market: Optional[str] = None,
    exchange: Optional[str] = None,
    cusip: Optional[str] = None,
    cik: Optional[str] = None,
    date: Optional[str] = None,
    search: Optional[str] = None,
    active: Optional[bool] = None,
    sort: Optional[str] = None,
    order: Optional[str] = None,
    limit: int = Query(10, le=1000),
):
    """Query supported ticker symbols."""
    try:
        results = massive_client.list_tickers(
            ticker=ticker,
            type=type,
            market=market,
            exchange=exchange,
            cusip=cusip,
            cik=cik,
            date=date,
            search=search,
            active=active,
            sort=sort,
            order=order,
            limit=limit,
            raw=True,
        )
        return success_response(json_to_csv(results.data.decode("utf-8")))
    except Exception as e:
        return error_response(str(e))


@app.get("/api/tickers/{ticker}", response_model=ApiResponse)
async def get_ticker_details(
    ticker: str,
    date: Optional[str] = None,
):
    """Get detailed information about a ticker."""
    try:
        results = massive_client.get_ticker_details(
            ticker=ticker, date=date, raw=True
        )
        return success_response(json_to_csv(results.data.decode("utf-8")))
    except Exception as e:
        return error_response(str(e))


@app.get("/api/tickers/{ticker}/news", response_model=ApiResponse)
async def list_ticker_news(
    ticker: str,
    published_utc: Optional[str] = None,
    limit: int = Query(10, le=1000),
    sort: Optional[str] = None,
    order: Optional[str] = None,
):
    """Get recent news articles for a ticker."""
    try:
        results = massive_client.list_ticker_news(
            ticker=ticker,
            published_utc=published_utc,
            limit=limit,
            sort=sort,
            order=order,
            raw=True,
        )
        return success_response(json_to_csv(results.data.decode("utf-8")))
    except Exception as e:
        return error_response(str(e))


# ==================== Corporate Actions ====================

@app.get("/api/dividends", response_model=ApiResponse)
async def list_dividends(
    ticker: Optional[str] = None,
    ex_dividend_date: Optional[str] = None,
    frequency: Optional[int] = None,
    dividend_type: Optional[str] = None,
    limit: int = Query(10, le=1000),
):
    """Get historical cash dividends."""
    try:
        results = massive_client.list_dividends(
            ticker=ticker,
            ex_dividend_date=ex_dividend_date,
            frequency=frequency,
            dividend_type=dividend_type,
            limit=limit,
            raw=True,
        )
        return success_response(json_to_csv(results.data.decode("utf-8")))
    except Exception as e:
        return error_response(str(e))


@app.get("/api/splits", response_model=ApiResponse)
async def list_splits(
    ticker: Optional[str] = None,
    execution_date: Optional[str] = None,
    reverse_split: Optional[bool] = None,
    limit: int = Query(10, le=1000),
):
    """Get historical stock splits."""
    try:
        results = massive_client.list_splits(
            ticker=ticker,
            execution_date=execution_date,
            reverse_split=reverse_split,
            limit=limit,
            raw=True,
        )
        return success_response(json_to_csv(results.data.decode("utf-8")))
    except Exception as e:
        return error_response(str(e))


# ==================== Short Data ====================

@app.get("/api/short-interest", response_model=ApiResponse)
async def list_short_interest(
    ticker: Optional[str] = None,
    settlement_date: Optional[str] = None,
    settlement_date_lt: Optional[str] = None,
    settlement_date_lte: Optional[str] = None,
    settlement_date_gt: Optional[str] = None,
    settlement_date_gte: Optional[str] = None,
    limit: int = Query(10, le=1000),
    sort: Optional[str] = None,
    order: Optional[str] = None,
):
    """Retrieve short interest data for stocks."""
    try:
        results = massive_client.list_short_interest(
            ticker=ticker,
            settlement_date=settlement_date,
            settlement_date_lt=settlement_date_lt,
            settlement_date_lte=settlement_date_lte,
            settlement_date_gt=settlement_date_gt,
            settlement_date_gte=settlement_date_gte,
            limit=limit,
            sort=sort,
            order=order,
            raw=True,
        )
        return success_response(json_to_csv(results.data.decode("utf-8")))
    except Exception as e:
        return error_response(str(e))


@app.get("/api/short-volume", response_model=ApiResponse)
async def list_short_volume(
    ticker: Optional[str] = None,
    date: Optional[str] = None,
    date_lt: Optional[str] = None,
    date_lte: Optional[str] = None,
    date_gt: Optional[str] = None,
    date_gte: Optional[str] = None,
    limit: int = Query(10, le=1000),
    sort: Optional[str] = None,
    order: Optional[str] = None,
):
    """Retrieve short volume data for stocks."""
    try:
        results = massive_client.list_short_volume(
            ticker=ticker,
            date=date,
            date_lt=date_lt,
            date_lte=date_lte,
            date_gt=date_gt,
            date_gte=date_gte,
            limit=limit,
            sort=sort,
            order=order,
            raw=True,
        )
        return success_response(json_to_csv(results.data.decode("utf-8")))
    except Exception as e:
        return error_response(str(e))


# ==================== Financials ====================

@app.get("/api/financials", response_model=ApiResponse)
async def list_stock_financials(
    ticker: Optional[str] = None,
    cik: Optional[str] = None,
    company_name: Optional[str] = None,
    timeframe: Optional[str] = None,
    include_sources: Optional[bool] = None,
    limit: int = Query(10, le=100),
    sort: Optional[str] = None,
    order: Optional[str] = None,
):
    """Get fundamental financial data for companies."""
    try:
        results = massive_client.vx.list_stock_financials(
            ticker=ticker,
            cik=cik,
            company_name=company_name,
            timeframe=timeframe,
            include_sources=include_sources,
            limit=limit,
            sort=sort,
            order=order,
            raw=True,
        )
        return success_response(json_to_csv(results.data.decode("utf-8")))
    except Exception as e:
        return error_response(str(e))


# ==================== Economic Data ====================

@app.get("/api/treasury-yields", response_model=ApiResponse)
async def list_treasury_yields(
    date: Optional[str] = None,
    date_lt: Optional[str] = None,
    date_lte: Optional[str] = None,
    date_gt: Optional[str] = None,
    date_gte: Optional[str] = None,
    limit: int = Query(10, le=1000),
    sort: Optional[str] = None,
    order: Optional[str] = None,
):
    """Retrieve treasury yield data."""
    try:
        results = massive_client.list_treasury_yields(
            date=date,
            date_lt=date_lt,
            date_lte=date_lte,
            date_gt=date_gt,
            date_gte=date_gte,
            limit=limit,
            sort=sort,
            order=order,
            raw=True,
        )
        return success_response(json_to_csv(results.data.decode("utf-8")))
    except Exception as e:
        return error_response(str(e))


@app.get("/api/inflation", response_model=ApiResponse)
async def list_inflation(
    date: Optional[str] = None,
    date_lt: Optional[str] = None,
    date_lte: Optional[str] = None,
    date_gt: Optional[str] = None,
    date_gte: Optional[str] = None,
    limit: int = Query(10, le=1000),
    sort: Optional[str] = None,
):
    """Get inflation data from the Federal Reserve."""
    try:
        results = massive_client.list_inflation(
            date=date,
            date_gt=date_gt,
            date_gte=date_gte,
            date_lt=date_lt,
            date_lte=date_lte,
            limit=limit,
            sort=sort,
            raw=True,
        )
        return success_response(json_to_csv(results.data.decode("utf-8")))
    except Exception as e:
        return error_response(str(e))


# ==================== Ratios ====================

@app.get("/api/ratios", response_model=ApiResponse)
async def list_ratios(
    ticker: Optional[str] = None,
    cik: Optional[str] = None,
    price_gt: Optional[float] = None,
    price_gte: Optional[float] = None,
    price_lt: Optional[float] = None,
    price_lte: Optional[float] = None,
    market_cap_gt: Optional[float] = None,
    market_cap_gte: Optional[float] = None,
    market_cap_lt: Optional[float] = None,
    market_cap_lte: Optional[float] = None,
    price_to_earnings_gt: Optional[float] = None,
    price_to_earnings_gte: Optional[float] = None,
    price_to_earnings_lt: Optional[float] = None,
    price_to_earnings_lte: Optional[float] = None,
    price_to_book_gt: Optional[float] = None,
    price_to_book_gte: Optional[float] = None,
    price_to_book_lt: Optional[float] = None,
    price_to_book_lte: Optional[float] = None,
    dividend_yield_gt: Optional[float] = None,
    dividend_yield_gte: Optional[float] = None,
    dividend_yield_lt: Optional[float] = None,
    dividend_yield_lte: Optional[float] = None,
    return_on_equity_gt: Optional[float] = None,
    return_on_equity_gte: Optional[float] = None,
    return_on_equity_lt: Optional[float] = None,
    return_on_equity_lte: Optional[float] = None,
    debt_to_equity_gt: Optional[float] = None,
    debt_to_equity_gte: Optional[float] = None,
    debt_to_equity_lt: Optional[float] = None,
    debt_to_equity_lte: Optional[float] = None,
    limit: int = Query(10, le=1000),
    sort: Optional[str] = None,
):
    """
    Retrieve financial ratios for stocks including P/E, P/B, dividend yield, ROE, debt-to-equity.
    Use comparison operators to filter by ratio values.
    """
    try:
        results = massive_client.list_ratios(
            ticker=ticker,
            cik=cik,
            price_gt=price_gt,
            price_gte=price_gte,
            price_lt=price_lt,
            price_lte=price_lte,
            market_cap_gt=market_cap_gt,
            market_cap_gte=market_cap_gte,
            market_cap_lt=market_cap_lt,
            market_cap_lte=market_cap_lte,
            price_to_earnings_gt=price_to_earnings_gt,
            price_to_earnings_gte=price_to_earnings_gte,
            price_to_earnings_lt=price_to_earnings_lt,
            price_to_earnings_lte=price_to_earnings_lte,
            price_to_book_gt=price_to_book_gt,
            price_to_book_gte=price_to_book_gte,
            price_to_book_lt=price_to_book_lt,
            price_to_book_lte=price_to_book_lte,
            dividend_yield_gt=dividend_yield_gt,
            dividend_yield_gte=dividend_yield_gte,
            dividend_yield_lt=dividend_yield_lt,
            dividend_yield_lte=dividend_yield_lte,
            return_on_equity_gt=return_on_equity_gt,
            return_on_equity_gte=return_on_equity_gte,
            return_on_equity_lt=return_on_equity_lt,
            return_on_equity_lte=return_on_equity_lte,
            debt_to_equity_gt=debt_to_equity_gt,
            debt_to_equity_gte=debt_to_equity_gte,
            debt_to_equity_lt=debt_to_equity_lt,
            debt_to_equity_lte=debt_to_equity_lte,
            limit=limit,
            sort=sort,
            raw=True,
        )
        return success_response(json_to_csv(results.data.decode("utf-8")))
    except Exception as e:
        return error_response(str(e))


def run_api(host: str = "0.0.0.0", port: int = 8000):
    """Run the FastAPI server."""
    import uvicorn
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    run_api()
