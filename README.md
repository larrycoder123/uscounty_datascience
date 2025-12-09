# US County Income Analysis & Prediction

A data science project that builds a comprehensive county-level dataset from U.S. Census Bureau, Bureau of Labor Statistics, and Zillow sources, then applies machine learning to predict mean household income and identify key economic and social drivers.

## Overview

This project aggregates data from multiple authoritative sources to create a unified dataset covering all 3,143 U.S. counties. Through exploratory analysis and predictive modeling, it examines how factors like education, housing costs, employment composition, poverty rates, and digital infrastructure relate to county-level household income patterns.

**Dataset**: 3,143 U.S. counties with 40+ features including demographics, education levels, employment by industry, housing costs, poverty rates, internet access, and geographic coordinates.

## Key Features

- **Multi-Source Data Pipeline**: Automated U.S. Census API integration and data enrichment from BLS and Zillow
- **Custom Imputation Method**: Geographically-weighted k-nearest neighbors algorithm for missing values using haversine distance
- **Comprehensive EDA**: Correlation analysis, geospatial visualizations, and multivariate relationship exploration
- **Predictive Modeling**: Linear Regression baseline with Random Forest comparison to predict income patterns

## Tech Stack

**Languages & Core Libraries**
- Python, Pandas, NumPy

**Data Collection & APIs**
- U.S. Census Bureau API (ACS 5-Year Estimates)
- Custom API wrapper functions for Census data series

**Machine Learning**
- scikit-learn (Linear Regression, Random Forest, preprocessing, model selection)
- Custom geospatial imputation algorithm

**Visualization**
- Matplotlib, Seaborn, GeoPandas

**Development**
- Jupyter Notebooks

## Project Structure

```
code/
├── project.ipynb                    # Main analysis & modeling notebook
├── data_import_modify.ipynb         # Data collection & preprocessing
├── uscensus_functions.py            # Census API helper functions
├── othersources_functions.py        # Data integration utilities
├── data/
│   ├── county_data.csv              # Intermediate dataset
│   └── merged_county_data.csv       # Final processed dataset
├── data_uscensus/                   # Census data by topic (12 files)
│   ├── demographics_county_2023.csv
│   ├── education_county_2023.csv
│   ├── healthinsurance_county_2023.csv
│   └── ...
├── data_othersources/               # External sources (BLS, Zillow)
│   ├── poverty_county_2023.csv
│   ├── unemployment_county_2023.csv
│   ├── zillowhousevalue_county_2023.csv
│   └── ...
└── plots/                           # Generated visualizations
```

## Key Findings

- **Model Performance**: Random Forest achieved 84% R² and $8,500 RMSE, reducing prediction error by 11% compared to Linear Regression (80% R², $9,556 RMSE)

- **Top Predictors**: Internet access emerged as the strongest predictor, followed by poverty rate, home values, knowledge-based employment, and education levels—highlighting digital infrastructure's critical role in economic prosperity

- **Education-Income Relationship**: Strong positive correlation (0.79) between bachelor's degree attainment and household income, though other factors like housing costs (0.88) and professional employment (0.69) also show significant influence

- **Geographic Patterns**: Coastal and urban counties consistently show higher incomes, with clear regional disparities visible in geospatial analysis

- **Housing-Income Connection**: Exceptionally strong correlation (0.88) between monthly housing costs and income, reflecting bidirectional relationship between local wages and cost of living

- **Custom Imputation Success**: Geographically-weighted k-nearest neighbors method effectively handled missing values by leveraging spatial proximity and inverse distance weighting

## Methodology Highlights

**Data Collection**
- Integrated 12 Census Bureau datasets via API automation
- Enriched with BLS unemployment, poverty rates, and Zillow home values
- Standardized FIPS codes across all sources for seamless merging

**Feature Engineering**
- Grouped 14 detailed industry categories into 6 broader economic sectors
- Calculated derived metrics from raw Census variables
- Maintained geographic coordinates for spatial analysis

**Missing Value Treatment**
- Developed custom imputation algorithm using haversine distance formula
- Computed inverse distance-weighted averages from 5 nearest counties
- Preserved spatial patterns while filling gaps in data coverage

**Model Development**
- Established Linear Regression baseline with standardized features
- Applied Random Forest for non-linear relationship capture
- Used 5-fold cross-validation for robust performance estimation
- Employed 80/20 train-test split with random seed for reproducibility

## Documentation

See `project_overview.pdf` for detailed methodology, data dictionary, and comprehensive analysis results.
