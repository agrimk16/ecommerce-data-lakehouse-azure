# Power BI Dashboard Design

## E-Commerce Analytics Dashboard

This document describes the Power BI dashboard design for the Gold layer data.

### Data Source Connection

Connect Power BI to **Synapse Serverless SQL** (not directly to Delta files):

1. Open Power BI Desktop
2. Get Data → Azure → Azure Synapse Analytics SQL
3. Server: `<synapse-workspace-name>-ondemand.sql.azuresynapse.net`
4. Database: `ecommerce_gold`
5. Authentication: Microsoft Account (your Azure AD)

### Dashboard Pages

#### Page 1: Executive Summary

| Visual            | Type        | Data                                       |
| ----------------- | ----------- | ------------------------------------------ |
| Total Revenue     | Card        | `SUM(fact_orders.total_payment_value)`     |
| Total Orders      | Card        | `COUNT(fact_orders.order_id)`              |
| Avg Order Value   | Card        | `AVERAGE(fact_orders.total_payment_value)` |
| Avg Delivery Days | Card        | `AVERAGE(fact_orders.delivery_days)`       |
| Revenue Over Time | Line Chart  | X: `dim_date.year_month`, Y: Revenue       |
| Orders by Status  | Donut Chart | `order_status`, Count                      |
| Top 10 Categories | Bar Chart   | `dim_products.category`, Revenue           |

#### Page 2: Customer Analytics

| Visual            | Type       | Data                                            |
| ----------------- | ---------- | ----------------------------------------------- |
| Customer Segments | Pie Chart  | Value segment from `vw_customer_lifetime_value` |
| CLV Distribution  | Histogram  | `lifetime_value` from CLV view                  |
| Top Customers     | Table      | Top 20 by lifetime value                        |
| New vs Returning  | Line Chart | First-time vs repeat by month                   |
| Customer Map      | Map        | `dim_geography.latitude`, `longitude`, Revenue  |

#### Page 3: Delivery Performance

| Visual                 | Type       | Data                              |
| ---------------------- | ---------- | --------------------------------- |
| On-Time Rate           | Gauge      | % of `is_late_delivery = false`   |
| Avg Delivery by Region | Bar Chart  | Region vs delivery days           |
| Late Deliveries Trend  | Line Chart | Late delivery % over time         |
| Delivery Heatmap       | Matrix     | State × Month → Avg delivery days |

#### Page 4: Real-Time Metrics (if streaming is active)

| Visual               | Type        | Data                               |
| -------------------- | ----------- | ---------------------------------- |
| Events/Minute        | Line Chart  | Streaming metrics from Silver      |
| Event Type Breakdown | Stacked Bar | Event types over time              |
| Active Users         | Card        | Distinct users in last 5 min       |
| Conversion Funnel    | Funnel      | page_view → add_to_cart → purchase |

### Key DAX Measures

```dax
// Total Revenue
Total Revenue = SUM(fact_orders[total_payment_value])

// Month-over-Month Growth
MoM Growth % =
VAR CurrentMonth = [Total Revenue]
VAR PreviousMonth = CALCULATE([Total Revenue], DATEADD(dim_date[date], -1, MONTH))
RETURN DIVIDE(CurrentMonth - PreviousMonth, PreviousMonth, 0)

// Conversion Rate (if streaming data available)
Conversion Rate =
DIVIDE(
    COUNTROWS(FILTER(clickstream_metrics, clickstream_metrics[event_type] = "purchase")),
    COUNTROWS(FILTER(clickstream_metrics, clickstream_metrics[event_type] = "page_view")),
    0
)

// YTD Revenue
YTD Revenue = TOTALYTD([Total Revenue], dim_date[date])
```

### Screenshots to Capture

After building the dashboard, take screenshots for your portfolio:

1. Executive summary with all KPIs
2. A filter applied (e.g., specific date range)
3. Customer map visualization
4. Delivery performance gauge
