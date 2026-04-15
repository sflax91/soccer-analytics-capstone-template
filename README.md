# Soccer Analytics Capstone Template

**Project (Trilemma Foundation): “Delivering Elite European Football (Soccer) Analytics”**

## Project Overview
This project aims to build an **MIT-licensed, open-source** pipeline that ingests **public match event data** and produces **interactive player/team analytics dashboards**. The goal is to create actionable insights (e.g., possession chains, xG flow, pressure heatmaps) from raw event data.

> [!IMPORTANT]
> **License Notice**: The code in this repository is licensed under MIT. However, the data sources (StatsBomb and Polymarket) are not covered by the MIT license and have their own licensing terms. See the [Data Licensing](#data-licensing) section below.

> [!IMPORTANT]
> **Batch Files**: To conveniently kick off all necessary Python files sequentially, this process utilizes batch files and may not work on Mac computers. Process will still work if Python files are run in the same order in the batch script.

## Getting Started
1. **Fork this repository** to your own GitHub account.
2. **Clone your fork** locally.
3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
4. **Download the data**:
   ```bash
   data/download_data.py
   ```
   *Note: This will download both StatsBomb (required) and Polymarket (optional) data.*
5. **Run the ETL Scripts**:
   See potential limitations above for Mac users.
   ```bash
   build_analysis_data.bat
   ```

6. **Launch the dashboard**:
   Start the interactive dashboard:
   ```bash
   launch_streamlit_app.bat
   ```
   `http://localhost:8501/` should automatically run in your default browser.

   The dashboard features:
   - Interactive head to head comparison of any two teams included in the 2026 FIFA World Cup.
   - Breakdown of team's playing style based on possession analysis we did on the entire events dataset. 
   - Plotting of xG values from shots in events data, bucketed in two square meter bins. Compares the two teams side-by-side to show potential opportunities.


## Data Licensing
This project uses data from multiple sources, each with their own licensing terms:

### StatsBomb Data
- **License**: [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) (Creative Commons Attribution-NonCommercial 4.0 International)
- **Usage**: Non-commercial use only, attribution required
- **Citation**: "StatsBomb Open Data"
- **Source**: Publicly available match event data

### Polymarket Data
- **Copyright**: © 2026 Polymarket
- **Usage**: Subject to [Polymarket Terms of Service](https://polymarket.com/terms)
- **Restrictions**: For analytical and research purposes only; users responsible for compliance with local laws and regulations
- **Source**: Historical prediction market data provided through Polymarket APIs

### Sports Reference / FBRef Data
- **Citation**: "FBRef Open Data"
- **Usage**: Non-commercial, local use only. No automated data pulls. No aggregating data to produce a competing product. ([Sports Reference Terms of Service](https://www.sports-reference.com/data_use.html))
- **Source**: Publicly available match event and team-level data

### 2026 World Cup Data from Wikipeida
- **Citation**: "2026 FIFA World Cup Qualification Information"
- **Usage**: [Wikipedia Terms of Use](https://foundation.wikimedia.org/wiki/Policy:Terms_of_Use#Overview)
- **Source**: Publicly available tournament information


> [!WARNING]
> The data in this project is **not covered by the MIT license**. Users must comply with the licensing terms of each respective data provider when using the data for their own projects or analyses.
