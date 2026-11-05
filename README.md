# CAPA · PTO Performance Dashboard v2

Streamlit web app — all features from Excel v13.

---

## Features
- **3 tabs**: CARs, PTOs, Combined
- **Region filter** → auto-filters location dropdown to that region's labs only
- **Location filter** — ALL shows region aggregate, or drill to individual lab
- **Running weighted avg days closed** — resets each January, 2025 YE KPI + 2026 YTD in scorecard
- **Top 20 offenders** per tab, ranked by Feb 2026 open ≥90 days
- **Interactive Plotly charts** — hover, zoom, year boundary marker
- **Trend indicators** — ▲▼→ with color coding
- **Conditional color coding** — red/yellow/green on overdue counts
- **Export to Excel** — all 3 tabs with scorecard

---

## Quick Start — Mac Studio

```bash
pip install -r requirements.txt
streamlit run app.py
```

Update the file path in the sidebar to point at your Excel file.

---

## Unraid Deployment

1. Copy folder to `/mnt/user/appdata/capa_dashboard/`
2. Edit `docker-compose.yml` — update the data volume:
   ```yaml
   - /mnt/user/data/QA:/data:ro   # wherever your Excel file lives
   ```
3. In the app sidebar set path to `/data/your-filename.xlsx`
4. Run:
   ```bash
   cd /mnt/user/appdata/capa_dashboard
   docker-compose up -d
   ```
5. Access at `http://<unraid-ip>:8501` or via Tailscale from anywhere

---

## Updating Data

Drop a new Excel export in your mapped `/data` folder, update the filename
in the sidebar if it changed, then click **⟳ Load / Refresh Data**.

## Separate Canada from USNE

Turn on **Show Canada as a separate region** in the sidebar to move Halifax,
Hamilton, Levis, Montreal, Newfoundland, and St John from USNE into Canada.
The region filter, dashboard metrics, location rankings, and regional Excel
summary all use the selected grouping. Turn it off to use the region assignments
in the uploaded workbook. The overall NAM totals do not change.

The regional Excel summary opens with the Explanation sheet, followed by CARs,
PTOs, and Combined. The Completed Records and Open Over 90 Days sheets list the
individual CARs and PTOs behind columns D and C. They reflect the selected JN
and Canada settings, and use the same snapshot time and deduplicated records as
the summary. The regional export covers all locations, regardless of the
dashboard's region and location selections.

---

## Adding / Editing Regions

Regions are read directly from the **'List source'** sheet in your Excel file.
The `Area` column defines which region each location belongs to.
No code changes needed — just update the source file.

---

## File Structure

```
capa_dashboard/
├── app.py              # Streamlit UI
├── data_engine.py      # All computation logic
├── export_utils.py     # Excel export
├── requirements.txt
├── docker-compose.yml
└── README.md
```
