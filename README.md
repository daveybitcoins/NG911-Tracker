# NG911 Filing Tracker
## Tracking PSAP filings under FCC PS Docket 25-143

This toolkit monitors which 911 Authorities and PSAPs have filed NG911 Phase 1 and Phase 2 service requests with the FCC, and cross-references them against the full Master PSAP Registry to identify who has and hasn't filed.

---

## Components

### 1. `fetch_ng911_data.py` — Data Fetcher
Pulls data from two FCC APIs:
- **ECFS API** (docket 25-143) — all NG911 valid request filings
- **FCC Master PSAP Registry** (Socrata/SODA API) — ~7,000+ PSAPs nationwide

### 2. `index.html` — Interactive Dashboard
A self-contained HTML dashboard with:
- State-by-state filing heatmap (click any state to drill down)
- Searchable/sortable ECFS filings table
- Full PSAP registry with NG911 filing status
- CSV export for all views

---

## Quick Start

### Step 1: Get your free ECFS API key
Register at: https://www.fcc.gov/ecfs/help/public_api

### Step 2: Fetch the data
```bash
python3 fetch_ng911_data.py --api-key YOUR_API_KEY
```

This creates three files:
- `ecfs_filings.json` — raw ECFS filing data
- `psap_registry.json` — full PSAP registry
- `ng911_tracker_data.json` — merged dataset for the dashboard

### Step 3: Open the dashboard
Open `index.html` in any browser, then click **"Load JSON File"** and select `ng911_tracker_data.json`.

Or click **"Load Demo Data"** to preview the dashboard with sample data.

### Subsequent runs
```bash
# Re-fetch just ECFS filings (skip re-downloading the PSAP registry)
python3 fetch_ng911_data.py --api-key YOUR_API_KEY --skip-psap

# Re-run with cached ECFS data (just rebuild the tracker)
python3 fetch_ng911_data.py --api-key YOUR_API_KEY --skip-ecfs
```

---

## Data Architecture

### What's available automatically (structured via API):
| Field | Source | Notes |
|-------|--------|-------|
| Who filed | ECFS API | `filers[].name` |
| Filing date | ECFS API | `date_submission` |
| Filing type | ECFS API | Usually "REQUEST" |
| Filer state | ECFS API | `addressentity.address_state` |
| Document links | ECFS API | URLs to attached PDFs |
| All PSAPs nationwide | SODA API | PSAP ID, name, state, county, city, type |

### What requires PDF parsing (inside attachments):
| Field | Location | Extraction Method |
|-------|----------|-------------------|
| Phase requested (1 or 2) | PDF form Question 5 | Needs PDF text extraction |
| Specific PSAP IDs | PDF form table | Structured table in the form |
| Delivery Point ZIPs | PDF form Question 7 | Needs PDF text extraction |
| Certifications | PDF form Questions 6/8 | Checkbox fields |

### Enhancing with PDF extraction
The FCC's NG911 Valid Request Form is a consistent template. To extract phase and
PSAP-level data from the PDF attachments, you could:

1. Download PDFs using the `document_urls` from each filing
2. Use `pdfplumber` or `PyMuPDF` to extract text
3. Parse the structured form fields (the template is standardized)
4. Alternatively, use an LLM (Claude API) to extract structured data from each PDF

---

## API Reference

### ECFS Public API
- **Base:** `https://publicapi.fcc.gov/ecfs/filings`
- **Key params:** `proceedings.name`, `sort`, `limit`, `offset`, `api_key`
- **Docs:** https://www.fcc.gov/ecfs/public-api-docs.html
- **Rate limit:** Reasonable use; no published hard limit

### FCC PSAP Registry (Socrata/SODA)
- **Base:** `https://opendata.fcc.gov/resource/dpq5-ta9j.json`
- **Key params:** `$limit`, `$offset`, `$order`, `$where`
- **No API key required** (but rate-limited without app token)
- **Dataset page:** https://opendata.fcc.gov/Public-Safety/911-Master-PSAP-Registry/dpq5-ta9j

### Related dockets
- **25-143** — NG911 valid requests (this tracker)
- **25-144** — OSP petitions challenging requests
- **25-145** — Mutual agreement notifications
