# Ecolane Power BI Quick Reference Guide

This document provides a quick reference for understanding and querying the Ecolane Power BI model effectively.

## Data Model Overview

### Primary Fact Tables
1. **`ArchiveTrip`** - Granular trip-level data (one row per trip)
2. **`Fact_Revenue_provider`** - Aggregated operational performance metrics

### Key Dimension Tables
- **`Date_time`** - Time dimension with multiple relationships
- **`k3_customer`** - Customer/rider information
- **`ServerDetails`** - Agency/server context
- **`archive_route`** - Route information

## Quick Measure Reference

### Operational Efficiency (from Fact_Revenue_provider)
| Measure | Description | Use Case |
|---------|-------------|----------|
| `[RevenueHours]` | Total actual revenue hours | Operational capacity |
| `[RevenueMiles]` | Total actual revenue miles | Distance traveled with passengers |
| `[ServiceHours]` | Total actual service hours | Total operational time |
| `[ServiceMiles]` | Total actual service miles | Total distance traveled |
| `[RidesPerHour]` | Rides ÷ Revenue hours | Efficiency metric |
| `[AverageDailyRevenueMiles]` | Avg revenue miles per day | Daily performance baseline |

### Trip Metrics (from ArchiveTrip)
| Measure | Description | Use Case |
|---------|-------------|----------|
| `[Total Trips]` | Count of all trip records | Overall volume |
| `[PassengerTrips_completed]` | Completed trips only | Successful service delivery |
| `[PassengerTrips_noshow]` | No-show trips | Service disruption tracking |
| `[Passenger Trips]` | Sum of passenger_count | Total passenger volume |
| `[AvgDailyTrips]` | Average trips per day | Daily volume patterns |

### On-Time Performance (from ArchiveTrip)
| Measure | Description | Key Fields |
|---------|-------------|------------|
| `[PickUp_OTP%]` | Pickup OTP percentage | Uses `pickup_ontime` field |
| `[Dropoff_OTP%]` | Drop-off OTP percentage | Uses `dropoff_ontime` field |

### Stop Validation (from Archive_Stops)
| Measure | Description | Use Case |
|---------|-------------|----------|
| `[ValidStops]` | Stops within distance threshold | Stop accuracy |
| `[InValidStops]` | Stops outside threshold | Quality issues |

## Critical Columns and Usage

### Trip Identification & Status
- **`ArchiveTrip.id`** - Unique trip identifier (primary counting field)
- **`ArchiveTrip.status`** - Trip status: "comp" (completed), "noshow", "cancel"
- **`ArchiveTrip.passenger_count`** - Number of passengers (for weighting)

### Date Fields (Multiple Relationships!)
| Column | Relationship | When to Use |
|--------|--------------|-------------|
| `reporting_date` | ACTIVE | Default time filtering |
| `promised_pickup` | Inactive | OTP calculations, schedule analysis |
| `requested_pickup` | Inactive | Booking/demand analysis |
| `actual_pickup_arrival` | Inactive | Actual performance analysis |

**Important**: Use `USERELATIONSHIP()` in DAX to activate inactive date relationships.

### On-Time Performance Fields
- **`ArchiveTrip.pickup_ontime`** - Values: "On-Time", "Late"
- **`ArchiveTrip.dropoff_ontime`** - Values: "On-Time", "Late"

### Entity Identifiers
- **`ArchiveTrip.[Driver name]`** - For driver-level analysis
- **`ArchiveTrip.[physical_vehicle_public_id]`** - Vehicle ID (synonym: "vehicle")
- **`ArchiveTrip.passenger_name`** - Customer name

### Location Fields
- **`start_location_name`** / **`end_location_name`** - Named locations (POIs)
- **`start_latitude`**, **`start_longitude`** - Pickup coordinates
- **`end_latitude`**, **`end_longitude`** - Drop-off coordinates

## Common Query Patterns

### 1. Basic KPI Query
```
Question: "What is the average daily revenue miles?"
Measure: [AverageDailyRevenueMiles]
Filter: Date range (optional)
```

### 2. Filtered Trip Count
```
Question: "How many completed trips last month?"
Measure: [PassengerTrips_completed]
Filter: Date_time[L_Date] = last month
```

### 3. On-Time Performance
```
Question: "What is the pickup OTP for driver John?"
Measure: [PickUp_OTP%]
Filter: ArchiveTrip[Driver name] = "John"
Note: Uses promised_pickup date relationship
```

### 4. Top N Ranking
```
Question: "Top 5 vehicles by revenue miles"
Measure: [RevenueMiles]
Group By: Fact_Revenue_provider[Vehicle ID]
Order: Descending, Limit: 5
```

### 5. Status-Based Analysis
```
Question: "No-show rate last week"
Measures: [PassengerTrips_noshow], [Total Trips]
Calculation: NoShow / Total
Filter: Date = last week
```

## Report Page Context

Understanding which page a question relates to helps determine the right measures:

- **"dashboard" / "KPI"** → Daily averages, gauges, efficiency metrics
- **"OTP" / "on-time"** → Pickup/dropoff OTP%, early/late analysis
- **"driver" / "performance"** → Driver-level metrics, stop validation
- **"rider" / "passenger" / "customer"** → Trip counts by age, new riders
- **"map" / "location"** → Geospatial analysis, POI tables
- **"capacity"** → Vehicle utilization metrics
- **"booking"** → Subscription vs demand, overbooking

## Common Synonyms and Aliases

### Measures
- "OTP" → On-Time Performance ([PickUp_OTP%] or [Dropoff_OTP%])
- "rides" → [Total Trips] or [PassengerTrips_completed]
- "efficiency" → [RidesPerHour]
- "no-shows" → [PassengerTrips_noshow]

### Filters
- "vehicle" / "vehicleid" → physical_vehicle_public_id
- "driver" → Driver name
- "passenger" / "rider" / "customer" → passenger_name
- "date" → Date_time[L_Date]
- "status" → ArchiveTrip[status]
- "company" → run_company or physical_vehicle_company

## Important Considerations

### 1. Status Filtering
Most operational metrics should filter for completed trips:
```
ArchiveTrip[status] = "comp"
```

### 2. Parameter Tables
- **`NoshowFilter`** - Controls whether no-shows are included
- **`IncludeRuns`** - Controls whether runs are included
- **`Trip Types`** - Filters trip types

### 3. Row-Level Security
The model enforces RLS through `UserAccess` table. Queries automatically filter to user's assigned server/agency.

### 4. Date Relationship Selection
Choose the right date field:
- **Default queries** → Use `reporting_date` (active)
- **"What was scheduled/promised?"** → Use `promised_pickup`
- **"What actually happened?"** → Use `actual_pickup_arrival`
- **"When was it booked?"** → Use `requested_pickup`

### 5. Multiple Date Meanings in Questions
- "trips today" → Filter by `reporting_date`
- "trips scheduled for today" → Filter by `promised_pickup`
- "trips that actually occurred today" → Filter by `actual_pickup_arrival`

## Query Construction Tips

1. **Identify the measure** - What metric is being asked about?
2. **Identify filters** - What constraints are mentioned?
3. **Identify grouping** - Is it by driver, vehicle, date, location?
4. **Determine query type** - KPI, TopN, Chart, Table?
5. **Select date field** - Which date relationship is appropriate?
6. **Apply status filter** - Should it be completed trips only?

## Example Query Resolutions

| User Question | Measure | Filters | Group By | Notes |
|--------------|---------|---------|----------|-------|
| "Average daily trips last week" | [AvgDailyTrips] | Date = last week | None | Simple KPI |
| "Top 5 drivers by completed trips" | [PassengerTrips_completed] | None | Driver name | TopN, DESC, limit 5 |
| "OTP by vehicle" | [PickUp_OTP%] | None | Vehicle ID | Chart data |
| "No-shows for driver John" | [PassengerTrips_noshow] | Driver = "John" | None | Filtered KPI |
| "Trips by status" | [Total Trips] | None | status | Chart breakdown |

## Visual Type Suggestions

Based on query type, suggest appropriate visualizations:

- **Single KPI** → Card or Gauge
- **Trend over time** → Line chart
- **Top N ranking** → Bar chart (horizontal)
- **Category breakdown** → Pie/Donut or Column chart
- **Detailed records** → Table
- **Location analysis** → Map with bubble size

---

**For detailed measure definitions and column descriptions, see [`docs/data_model.md`](docs/data_model.md)**
