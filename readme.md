# Script (IDX) Suspension Scraper

This repo is a Python-based scraper designed to automatically fetch, process, and store stock suspension announcements from the Indonesia Stock Exchange (IDX). It parses PDF announcements to extract structured data and saves it to a Supabase (PostgreSQL) database, and saved to csv if there is any incomplete data.

## Getting Started

### 1. Clone the Repository

```bash
git clone )
cd repo
```

### 2. Set Up a Virtual Environment

```bash
python -m venv .venv
source .venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

```bash
PROXY="PROXY"
SUPABASE_URL="SUPABASE_PROJECT_URL"
SUPABASE_KEY="SUPABASE_API_KEY"
```

### 5. Usage
```bash
python pipeline.py <start_date> <end_date>
```

## idx_suspension payload

This table stores the structured suspension data scraped by the script.

| Column          | Type | Description                                          |
|-----------------|------|------------------------------------------------------|
| symbol          | text | Foreign Key to `idx_company_profile.symbol`.         |
| suspension_date | date | The date the suspension was announced.               |
| reason          | text | The reason for the suspension, extracted from the PDF. |
| pdf_url         | text | The direct URL to the official announcement PDF.     |
