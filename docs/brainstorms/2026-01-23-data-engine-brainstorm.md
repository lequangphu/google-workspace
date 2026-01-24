# Data Engine for Nhan Thanh Tam Tire Shop

**Date:** 2026-01-23
**Status:** Brainstorm Complete

## What We're Building

A data engine that powers a full business dashboard for a tire shop, sourcing data from:
- **Google Drive** (historical 2020-2025 data, one-time fetch)
- **KiotViet ERP** (current 2026+ data, daily sync)

The dashboard covers: Sales analytics, Inventory health, Receivables, Payables, and Financial metrics.

## Why This Approach

**Chosen Architecture: Modern Data Stack Lite**

```
[Google Drive] → [Python ETL] → [DuckDB] → [FastAPI] → [React Dashboard]
               (pandas)
```

### Rationale
- **DuckDB** chosen over PostgreSQL for local deployment (zero config, columnar for analytics)
- **Raw + Aggregates** storage pattern for fast dashboard queries with drill-down capability
- **React + FastAPI** provides flexibility for multi-user access and interactive dashboard
- **Daily refresh** is sufficient for operational dashboards
- **Local deployment** keeps costs at zero during prototype phase

## Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Deployment | Local | Zero cost, full control, prototype phase |
| Database | DuckDB | Analytics-optimized, handles 500K+ rows easily |
| Data granularity | Raw + Aggregates | Fast dashboard + drill-down capability |
| Backend | FastAPI | Modern Python API, good for multi-user |
| Frontend | React | Interactive dashboard, scalable UI |
| Refresh frequency | Daily | Sufficient for operational visibility |
| Users | Multiple (role-based) | Staff need access for operations |

## Data Sources

### Google Drive (Historical - Fetch Once)
- Import/Export receipts (2020-2025)
- Customer data, supplier data
- ~450K transaction rows, ~1.3K products

### KiotViet API (Current - Daily Sync)
- Products, inventory
- Sales transactions
- Customer/supplier records
- Requires API credential setup

## Data Model (Star Schema)

### Dimensions
- `dim_products`: Products, categories, brands, tire specs
- `dim_customers`: Customers, contact, credit limits
- `dim_suppliers`: Suppliers, contact info
- `dim_dates`: Date dimension (daily grain)
- `dim_warehouses`: Store locations

### Facts
- `fact_sales`: Sales transactions (aggregated daily)
- `fact_purchases`: Purchase transactions (aggregated daily)
- `fact_inventory`: Inventory snapshots (daily)
- `fact_receivables`: Customer payment tracking (daily)
- `fact_payables`: Supplier payment tracking (daily)

### Aggregation Tables
- `agg_sales_daily`: Revenue, units, margin by day
- `agg_sales_monthly`: Monthly rollups by product/category
- `agg_inventory_daily`: Stock levels, turnover
- `agg_receivables_aging`: AR aging buckets
- `agg_payables_aging`: AP aging buckets

## Dashboard Metrics

### Sales Analytics
- Daily/monthly revenue
- Product performance (units, margin)
- Category trends
- Growth rates

### Inventory Health
- Current stock levels
- Turnover rates
- Reorder alerts
- Slow-moving items

### Receivables
- Outstanding balances
- Aging analysis (30/60/90+ days)
- Collection trends
- Customer credit utilization

### Payables
- Outstanding payments
- Aging analysis
- Supplier performance
- Cash flow timing

### Financial
- Gross margin analysis
- Cash position
- P&L by category
- Working capital metrics

## Open Questions

1. **KiotViet API**: Need to set up credentials and understand rate limits
2. **Authentication**: Simple password or full auth system?
3. **Dashboard refresh**: Manual button or auto-refresh interval?
4. **Alerting**: Email/SMS alerts for critical metrics?

## Next Steps

1. Set up KiotViet API credentials
2. Design detailed DuckDB schema (tables, indexes)
3. Build ETL pipeline for historical data ingestion
4. Create aggregation scripts
5. Set up FastAPI backend
6. Build React dashboard skeleton
7. Deploy locally and iterate
