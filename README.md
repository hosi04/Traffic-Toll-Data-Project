# Traffic Toll Data Dashboard

A real-time traffic toll data pipeline and dashboard using Kafka, MySQL, and Power BI to analyze vehicle traffic and payment methods at toll stations.

![Dashboard Screenshot](docs/dashboard_screenshot.png)

## Overview
This project builds a data pipeline to process traffic toll data in real-time:
- **Data Streaming**: Kafka for ingesting and processing toll data.
- **Storage**: MySQL for persistent storage.
- **Visualization**: Power BI dashboard to display traffic trends, vehicle types, and payment methods.

## Features
- Interactive dashboard with filters for toll plaza and vehicle type.
- Real-time data processing with Kafka.
- Visualizations: Vehicle traffic by type, trading trends over time, payment method ratios, and transactions per toll station.

## Tech Stack
- **Python**: Data processing (kafka-python, mysql-connector-python).
- **Kafka**: Real-time data streaming.
- **MySQL**: Data storage.
- **Power BI**: Data visualization.

## Setup and Installation
1. **Prerequisites**:
   - Python 3.8+
   - Kafka (localhost:9092)
   - MySQL (localhost:3307)
   - Power BI Desktop

2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
