# ✈️ Flight Data Pipeline — France ↔ Tunisia

![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?logo=apacheairflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-003B57?logo=sqlite&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?logo=pandas&logoColor=white)
![BeautifulSoup](https://img.shields.io/badge/BeautifulSoup-4-brightgreen)

---

## 📌 Project Overview

An end-to-end automated **ETL pipeline** that collects, cleans, and stores real flight data between **France 🇫🇷 and Tunisia 🇹🇳**.

The pipeline scrapes live flight comparison websites, processes and enriches the data, stores it in a structured database, and produces analytical visualizations — all orchestrated automatically via **Apache Airflow** and containerized with **Docker**.

---

## ⚡ Tech Stack

| Layer | Tool |
|---|---|
| **Data Collection** | BeautifulSoup, Requests |
| **Data Processing** | Pandas, NumPy |
| **Orchestration** | Apache Airflow |
| **Storage** | SQLite |
| **Containerization** | Docker |
| **Visualization** | Matplotlib, Seaborn |
| **Version Control** | Git |

---

## ✅ Key Features

- Automated scraping of **real flight data** (prices, airlines, schedules, duration, airports)
- Robust **data cleaning** pipeline with deduplication, type normalization, and outlier detection
- **Data enrichment** with derived features (e.g. weekend flight indicator)
- Fully automated **ETL orchestration** with Apache Airflow DAGs
- Lightweight and efficient **SQLite storage**
- Statistical **visualizations** for trend analysis

---

## 📂 Repository Structure

```text
flight-data-pipeline/
├── scraper/
│   └── scraper.py            # BeautifulSoup web scraping scripts
├── transformation/
│   └── clean.py              # Pandas cleaning & enrichment logic
├── dags/
│   └── flight_pipeline_dag.py  # Airflow DAG definition
├── db/
│   └── flights.db            # SQLite database
├── visualizations/
│   └── analysis.py           # Matplotlib & Seaborn charts
├── docker-compose.yml        # Docker services configuration
├── requirements.txt
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites

- Docker & Docker Compose installed
- Python 3.8+

### Run the Pipeline

```bash
# 1. Clone the repository
git clone https://github.com/your-username/flight-data-pipeline.git
cd flight-data-pipeline

# 2. Start all services (Airflow + dependencies)
docker-compose up -d

# 3. Access the Airflow UI
# Navigate to http://localhost:8080
# Default credentials: admin / admin

# 4. Trigger the DAG manually or let it run on schedule
```

---

## ⚙️ Pipeline Steps

### 1. 🔍 Data Extraction

Flight data is scraped from flight comparison websites using **BeautifulSoup**. The following fields are extracted:

- Ticket prices
- Airline names
- Departure & arrival times
- Flight duration
- Airport codes (origin & destination)

---

### 2. 🧹 Data Cleaning & Transformation

Raw data is processed with **Pandas** and **NumPy**:

- Remove duplicates and handle missing values
- Normalize data types (prices, airline names, timestamps)
- Detect and remove price outliers using **standard deviation filtering**
- Enrich data with derived columns (e.g. `is_weekend_flight`)

---

### 3. 🔄 ETL Orchestration with Airflow

The pipeline is orchestrated with **Apache Airflow**:

- DAG schedules automated scraping runs
- Tasks run in the correct order: `scrape → clean → store`
- Airflow handles retries, logging, and failure alerts

---

### 4. 🗄️ Storage

Cleaned data is stored in **SQLite**, chosen for its simplicity and zero-configuration setup.  
Docker ensures the environment is isolated, reproducible, and portable across machines.

---

### 5. 📊 Visualization

Analytical charts generated with **Matplotlib** and **Seaborn**:

- Price distribution across airlines
- Price trends over time
- Flight duration vs. price correlation
- Weekend vs. weekday pricing comparison

---

## 📊 Sample Insights

- Price trends and seasonal patterns on the France–Tunisia route
- Cheapest airlines and optimal booking windows
- Impact of departure time on ticket price

---

## 🔗 Connect

**LinkedIn**: [Chahine Bouslahi](https://www.linkedin.com/in/chahine-bouslahi-b41880278/)
