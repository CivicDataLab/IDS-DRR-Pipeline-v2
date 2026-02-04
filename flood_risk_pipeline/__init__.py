"""
IDS-DRR Flood Risk Pipeline

A Dagster-based data pipeline for automating multi-state flood risk
data collection, transformation, and risk score calculation.

States covered:
- Himachal Pradesh
- Assam
- Odisha
- Bihar
- Uttar Pradesh

Risk Model Components:
- Hazard Factor: Flood probability and intensity
- Vulnerability Factor: Population and infrastructure exposure
- Exposure Factor: Assets at risk
- Government Response: Disaster preparedness indicators
"""

__version__ = "0.1.0"
