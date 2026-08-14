# Deutsche Bahn Historical Delay Data — Profiling Conclusion

**Profiling scope:** January–July 2026  
**Source files:** `data-2026-01.parquet` … `data-2026-07.parquet`  
**Rows profiled:** 101,702,091  
**Source notebook:** `03_data_profiling.ipynb`

---

## 1. Purpose

This conclusion summarizes the evidence produced by the executed January–July 2026 profiling notebook.

It is intentionally limited to findings supported by the profiling outputs.

The purpose of the profiling phase is to:

- understand the real structure and behavior of the monthly dataset;
- identify data-quality patterns;
- confirm or reject the grain hypothesis;
- understand delay, timestamp, station, line, and cancellation semantics;
- make unresolved modeling questions explicit before later transformation work.

No production transformation rule is implemented in this profiling phase.

---

## 2. Dataset scope and structural stability

The January–July 2026 profiling window contains **101,702,091 rows**.

| Month | File size | Row count |
|---|---:|---:|
| 2026-01 | 620.54 MB | 15,582,748 |
| 2026-02 | 547.28 MB | 13,721,520 |
| 2026-03 | 597.84 MB | 15,016,329 |
| 2026-04 | 564.63 MB | 14,149,788 |
| 2026-05 | 574.70 MB | 14,427,217 |
| 2026-06 | 589.73 MB | 14,752,336 |
| 2026-07 | 563.05 MB | 14,052,153 |
| **Total** | — | **101,702,091** |

All seven files use the same **17-column schema**.

For every profiled month:

- the schema matches the January 2026 reference schema;
- the physical column order matches the cataloged column order;
- no field-level schema differences were found.

### Conclusion

There is no evidence of schema drift within the January–July 2026 profiling window.

This does not prove that the entire historical archive is schema-stable, so schema validation should remain part of future data-quality checks.

---

## 3. Temporal coverage

The profiling confirms that the source `time` field provides complete month-level temporal coverage.

For every month:

- `service_date_null_rows = 0`;
- `service_date_outside_source_month_rows = 0`;
- no missing service dates were detected;
- all seven weekdays are represented.

The combined profiling period covers:

```text
2026-01-01 00:00
to
2026-07-31 23:59
```

with **212 distinct service dates**.

### Conclusion

There is no evidence of missing days or out-of-month records in the January–July 2026 data.

---

## 4. Column completeness

Only a subset of the 17 columns contains null values.

Observed null patterns include:

| Column | Approximate null-rate range |
|---|---:|
| `arrival_planned_time` | 7.33%–7.84% |
| `departure_planned_time` | 7.33%–7.84% |
| `arrival_change_time` | 7.33%–7.83% |
| `departure_change_time` | 7.33%–7.83% |
| `line_number` | 1.69%–1.81% |
| `station_name` | 0.013%–0.034% |
| `final_destination_station` | very low; appears from May onward |

The following columns are fully populated in the profiled window:

```text
id
eva
train_number
train_type
train_line_ride_id
train_line_station_num
time
is_canceled
xml_station_name
delay_in_min
```

### Conclusion

Null values in this dataset are not uniformly data-quality failures.

Several null patterns are structural and must be interpreted in their business context.

---

## 5. Arrival and departure null semantics

Arrival and departure timestamp columns are each null in approximately 7%–8% of observations.

However:

```text
both_planned_events_null = 0
```

for every profiled month.

This means every row contains at least one planned event:

- an arrival;
- a departure;
- or both.

This is consistent with railway-stop structure:

- origin stops can have departure information without arrival information;
- terminal stops can have arrival information without departure information;
- intermediate stops can have both.

### Conclusion

The arrival/departure null pattern is largely structural rather than random missing data.

Blanket non-null assumptions for every arrival and departure timestamp would therefore be incorrect.

---

## 6. `line_number` completeness depends on `train_type`

The `line_number` null pattern is strongly associated with `train_type`.

Many regional and local services have line numbers almost all the time, while several long-distance or international categories have `line_number` missing systematically.

Examples observed with systematic null `line_number` include categories such as:

```text
ICE
IC
EC
RJ
NJ
TGV
EN
IR
```

Regional/local categories such as:

```text
S
RB
RE
HLB
Bus
BRB
NWB
ERB
```

generally contain line numbers.

### Conclusion

A null `line_number` is not universally a data-quality problem.

It reflects source semantics for some train categories.

---

## 7. Station identity

The EVA code is highly stable across the profiled data.

Only a very small number of EVA identifiers show multiple station-name representations.

At the same time, `station_name` and `xml_station_name` disagree on approximately **27.4%–28.3% of rows** every month.

This mismatch is large and consistent across the entire profiling window.

### Conclusion

The station-name mismatch is a structural naming difference rather than an occasional dirty-data issue.

`eva` is the strongest candidate for a durable station identifier.

Both station-name fields should remain available until a clear naming-precedence rule is documented.

---

# 8. Grain and repeated observations — central profiling finding

This is the most important result of the profiling phase.

## 8.1 `id`

The `id` field is:

- fully non-null;
- unique for every row;
- never duplicated within a monthly file.

This was additionally verified across the **whole table, all months combined**, after loading the monthly raw data from S3 into Redshift:

```sql
-- Is id unique across the WHOLE table (all months combined)?
SELECT
    COUNT(*)           AS total_rows,
    COUNT(DISTINCT id) AS total_distinct_ids
FROM db_monthly.raw_observations;
-- total_rows = total_distinct_ids for the whole table
```

`total_rows` equaled `total_distinct_ids`, confirming `id` is unique not just within a single monthly file but across the entire loaded dataset.

Therefore:

> `id` is a reliable technical row identifier for one captured observation.

However, uniqueness does not mean that `id` represents the final business event.

---

## 8.2 Repeated ride-stop observations

The combination:

```text
train_line_ride_id
+ train_line_station_num
```

is highly repetitive.

Per month:

- approximately **798,555–863,139** ride-stop groups contain multiple rows;
- those repeated groups contain approximately **13.6M–15.5M rows**;
- one ride-stop can appear up to **35 times**.

The repeated rows are not exact copies.

Within duplicate ride-stop groups:

- 100% show different `time` values;
- approximately 92% show changing delay values;
- approximately 92% show changing arrival/departure change timestamps;
- a substantial share show changes in cancellation status;
- 100% contain multiple distinct `id` values.

### Conclusion

The monthly processed dataset behaves like a **poll-and-snapshot history**.

The same train-stop is captured repeatedly while its state evolves over time.

Therefore two grains must be distinguished:

### Source observation grain

> One row = one captured snapshot of a train stop.

### Intended reporting grain

> One row = one selected final state for a train ride at one station sequence position.

This distinction must be resolved during later transformation work.

---

# 9. Timestamp semantics

The generic `time` column is not exclusively an arrival or departure timestamp.

Across the seven months:

- approximately 64.6%–67.6% of rows match `departure_change_time` only;
- approximately 24.5%–28.0% match both arrival and departure change times;
- approximately 7.3%–7.8% match `arrival_change_time` only;
- no row has a null `time`;
- no row has a `time` matching neither event.

### Conclusion

The `time` field represents an effective event timestamp selected by the upstream processing logic.

It should not be interpreted as simply “departure time.”

---

## 9.1 Timestamp integrity

The profiling found:

- no planned arrival-after-departure inversions;
- a small number of effective arrival-after-departure cases;
- meaningful numbers of early arrivals and early departures;
- all calculated arrival/departure time differences occur in whole minutes.

### Conclusion

Timestamp integrity is generally strong, but a small number of effective-time edge cases deserve explicit monitoring rather than silent removal.

---

# 10. Delay distribution

Delay values are strongly right-skewed.

Across January–July 2026:

- median delay is approximately **1 minute**;
- p90 is approximately **12–15 minutes**;
- p99 is approximately **29–39 minutes**;
- maximum delays reach approximately **802–1,439 minutes**;
- negative delay values occur every month.

Negative-delay counts are substantial enough to show that early arrivals/departures are a real source behavior rather than isolated errors.

Very large delays also occur, but they are rare relative to the total dataset volume.

### Conclusion

Average delay alone is insufficient to describe performance.

The delay distribution contains:

- many zero/low-delay observations;
- legitimate negative values;
- a long positive tail;
- rare extreme values.

Extreme delays should be investigated before any hard production sanity range is defined.

---

# 11. Delay recomputation

The profiling compared published `delay_in_min` with delays recomputed from planned and effective timestamps.

## Departure-based recomputation

For all departure-eligible observations:

> **Departure-based recomputation matches the published `delay_in_min` 100% in every profiled month.**

## Arrival-based recomputation

Arrival-based recomputation matches only approximately:

```text
69.6%–72.0%
```

of arrival-eligible observations.

No event-comparable row remained unexplained by both arrival and departure calculations.

### Conclusion

Departure timestamps provide the strongest reproducible explanation of the published delay field when departure information exists.

Arrival-based delay still matters for arrival-only observations and as a supplementary diagnostic measure.

The profiling therefore supports keeping arrival and departure delay concepts separate rather than assuming that one event type explains every row.

---

# 12. Cancellations

Cancellation rates vary across the profiling window from approximately:

```text
3.09% to 6.08%
```

Canceled observations still contain non-null `delay_in_min` values.

They also show a consistently higher average delay than non-canceled observations.

Approximate observed averages:

- canceled observations: **5.0–6.9 minutes**;
- non-canceled observations: **2.8–3.6 minutes**.

### Conclusion

Cancellation and delay are not independent in the source data.

Including or excluding canceled observations from delay/on-time KPIs will materially change reporting results.

Profiling identifies this as an important business-definition decision, but does not decide the denominator by itself.

---

# 13. Coverage-era limitation

The entire profiling period occurs after the documented station-coverage transition on:

```text
2025-11-02
```

Therefore this notebook cannot compare:

- the earlier approximately 100-largest-stations period;
- the later all-available-stations period.

### Conclusion

The profiling window cannot validate the historical coverage break itself.

However, later historical trend analysis must preserve a coverage-era indicator so that pre- and post-transition station populations are not compared silently.


---

# 14. Key profiling findings

The January–July 2026 profiling establishes the following evidence-backed conclusions:

1. **The seven profiled months are structurally consistent.**
   - Same 17-column schema.
   - No observed schema drift.

2. **Temporal coverage is complete.**
   - No null service dates.
   - No service dates outside the source month.
   - No missing service days detected.

3. **`id` is a technical snapshot identifier.**
   - Fully non-null and unique.
   - It does not represent the final reporting grain.

4. **Ride-stop repetition is structural.**
   - The same `train_line_ride_id + train_line_station_num` appears repeatedly.
   - Repeated records carry evolving operational state.

5. **The source is effectively a snapshot-history dataset.**
   - Reporting later requires choosing one final state per ride-stop.

6. **Arrival/departure nulls are structural.**
   - Every row has at least one planned event.

7. **`line_number` nullability depends strongly on train type.**
   - Null does not automatically mean bad data.

8. **EVA is the strongest station identifier.**
   - Station-name representations differ frequently.

9. **The generic `time` field blends arrival and departure event semantics.**
   - It should not be interpreted as departure-only.

10. **Delay values include legitimate negatives and a long positive tail.**
    - Hard sanity thresholds should not be guessed from observed extrema alone.

11. **Departure-based delay is fully reproducible where departure timestamps exist.**
    - Arrival-based recomputation is less consistent.

12. **Cancellations materially affect delay statistics.**
    - KPI denominator treatment must be explicitly defined later.

13. **The profiling period cannot validate the pre/post-November-2025 station-coverage difference.**
    - Historical comparisons will need a coverage-era rule.


