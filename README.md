# ✈️ Flight Delay Analysis with Apache Spark

Distributed analysis of **1,000 flight records** using PySpark and Spark SQL to uncover delay patterns across airlines, routes, and airports.

---

## Analyses

| # | Analysis | Method |
|---|----------|--------|
| 1 | Average delay by airline | Spark SQL + DataFrame API |
| 2 | Top routes with highest delays | Spark SQL |
| 3 | On-time performance per airline | Spark SQL |
| 4 | Delay hotspots by origin & destination city | Spark SQL |
| 5 | Delay severity breakdown (none / minor / moderate / major) | Spark SQL |

---

## Dataset

**`flight_delays.csv`** — 1,000 records

| Column | Type | Description |
|--------|------|-------------|
| `flight_id` | String | Unique identifier (FL00001 …) |
| `airline` | String | Carrier name |
| `origin` | String | Departure airport (IATA code) |
| `destination` | String | Arrival airport (IATA code) |
| `delay_minutes` | Integer | Delay in minutes (0 = on time) |

---

## Key Findings

### Airline Performance
| Airline | Avg Delay | On-Time % |
|---------|-----------|-----------|
| Southwest | 8.06 min | 85.83% ✅ |
| Alaska | 7.19 min | 80.88% |
| Delta | 9.60 min | 76.52% |
| American | 11.81 min | 73.11% |
| JetBlue | 14.49 min | 62.22% |
| United | 16.24 min | 61.54% |
| Frontier | 20.98 min | 53.28% |
| Spirit | 23.69 min | 52.94% ❌ |

> On-time = delay ≤ 15 minutes

### Worst Routes
| Route | Avg Delay |
|-------|-----------|
| DEN → BNA | 68 min |
| LAS → JFK | 61 min |
| PHX → ORD | 61 min |
| PHX → ATL | 60 min |

### Worst Departure Cities
| City | Avg Delay |
|------|-----------|
| DFW | 23.44 min |
| DEN | 19.21 min |
| MSP | 18.43 min |
| ATL | 17.32 min |

---

## Run It

```bash
python coding.py
```

Results are saved to `output/`:
```
output/
├── avg_delay_by_airline.csv
├── route_delays.csv
├── ontime_performance.csv
├── origin_delays.csv
├── destination_delays.csv
└── delay_categories.csv
```

---

## Stack

`PySpark` · `Spark SQL` · `Pandas` · `Python 3`
