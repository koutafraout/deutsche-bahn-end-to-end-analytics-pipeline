
# Dataset context

The capstone project combines:

- **Deutsche Bahn historical delay data** for historical analysis and possible ML training;
- **VBB static GTFS data** for route, trip, stop, and schedule topology;
- **VBB GTFS-Realtime data** for the future streaming path;
- **weather data** for later enrichment.

## Dataset source:

| | |
|---|---|
| **Sources** | [VBB GTFS-Realtime](https://production.gtfsrt.vbb.de/) (live positions/trip updates/alerts, no auth), [VBB static GTFS](https://daten.berlin.de/datensaetze/vbb-fahrplandaten-via-gtfs) (routes/stops/trips — join target), [Deutsche Bahn historical delay data](https://github.com/piebro/deutsche-bahn-data) (community-archived), [DWD weather via Open-Meteo](https://open-meteo.com/en/docs/dwd-api) (free, no key) |
| **Type** | Streaming/API (GTFS-RT protobuf, ~30s cadence), batch CSV (static GTFS republished ~2x/week, historical delay data), API (weather) |
| **Update frequency** | GTFS-RT: near real-time. Static GTFS: twice weekly. Historical delay data: static/batch archive. Weather: on-demand/hourly via Open-Meteo. |