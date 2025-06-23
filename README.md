# Fenics Market Data - Data Quality Assurance (DQA) Tool

## Overview

A Streamlit application for comprehensive data quality analysis of FMD financial datasets. Supports CSV and Excel files with intelligent caching, user-controlled column selection, and multi-method anomaly detection.

---

## Core Capabilities

- Smart File Processing: CSV and Excel files with automatic header detection and cached loading  
- Single & Multi-File Support: Individual file analysis or combined datasets with cross-file anomaly detection  
- User-Controlled Quality Checks: Manual column selection for all quality validations  
- Multi-Method Anomaly Detection: Three detection methods (Log Return, Z-Score, Percentage Change)  
- Enhanced Summary Grid: Multi-column grouping with business intelligence features  
- Session State Management: Configuration persistence without page reloads  

---

## Quality Checks

- Data Completeness: User-selected columns for sufficient data validation  
- Data Integrity: Manual column selection for blank/null value detection  
- Expected Values: Validates against user-defined value lists  
- File Compatibility: Duplicate detection with content hashing  
- Anomaly Detection: Three statistical methods for spike identification (multi-file only)  

---

## Business Features

- Multi-Column Summary Grid: Group by multiple attributes (brand, ccyPair, priceType, convention, etc.)  
- Smart Quality Scoring: Pass/fail status with quality percentages per group  
- Enhanced Export Options: JSON reports and method-specific CSV downloads  
- Performance Optimization: Smart caching eliminates file reloading on configuration changes  

---

## Getting Started

### Prerequisites

```bash
pip install streamlit pandas numpy plotly
```

### Launch

```bash
streamlit run QA_notebook.py
```

**Supported Files:** CSV Files (.csv) and Excel Files (.xlsx, .xls)

---

## Main Components

### Enhanced Column Detection

```python
def smart_column_detection(self, df):
```

- Identifies column types (price, currency, datetime, reference, grouping, quality_check)  
- Provides smart suggestions for quality checks  
- Supports multi-column grouping for business analysis  

---

### Optimized File Processing

```python
@st.cache_data
def load_and_process_files(file_data_list):
```

- Smart caching eliminates file reloading on setting changes  
- Multi-format support (CSV, Excel) with header detection  
- Content-based duplicate detection using MD5 hashing  

---

## User-Controlled Quality Checks

### Data Completeness

```python
def check_non_empty(self, df, selected_columns=None):
```

- Manual column selection for targeted validation  
- Data density analysis with sparse row detection  
- Empty row identification in selected columns only  

### Data Integrity

```python
def check_non_blank(self, df, selected_columns=None):
```

- User-defined column selection prevents false positives  
- Excludes optional fields like 'priceType'  
- Quality scoring per selected column with blank value analysis  

### Expected Values

```python
def check_expected_values(self, df, expected_values_config):
```

- User-defined comma-separated value lists  
- Compliance rate calculation with unexpected value reporting  
- Severity classification (HIGH/MEDIUM/LOW)  

---

## Three Anomaly Detection Methods

### Log Return Analysis

```python
def run_log_return_analysis(self, df, value_columns, time_col, threshold):
```

- Financial price movement analysis using logarithmic calculations  
- Multiple price column support with rolling statistics  
- Severity classification and context analysis  

### Z-Score Movement Analysis

```python
def run_z_score_analysis(self, df, value_columns, time_col, threshold):
```

- Enhanced to analyze price movements rather than absolute values  
- Curve-by-curve analysis when grouping columns available  
- Cross-file symbol tracking for multi-file datasets  

### Percentage Change Analysis

```python
def run_percentage_change_analysis(self, df, value_columns, time_col, threshold):
```

- Simple percentage change detection for business users  
- Configurable thresholds (1-100%)  
- Intuitive interpretation for non-technical users  

---

## Multi-Column Summary Grid

```python
def generate_summary_grid(self, df, results, pivot_columns):
```

- Single or multiple column grouping with string concatenation  
- Business attribute combinations (brand + priceType + convention)  
- Quality scoring per group with comprehensive issue breakdown  

---

## Technical Implementation

### Session State Management

```python
def initialize_session_state():
```

- All configuration preserved across interactions  
- Results persistence until manual clearing  
- No page reloads during analysis setup  

---

## Performance Features

- Files cached with `@st.cache_data` - no reloading on configuration changes  
- Session state maintains all settings and results  
- Memory-efficient processing for large datasets  
- Optimized multi-column grouping algorithms  

---

## Statistical Methods

- Log Return: `log(current) - log(previous)`  
- Z-Score Movement: `(movement - mean_movement) / std_movement`  
- Percentage Change: `((current - previous) / previous) * 100`  

---

## Tab Display Logic

- Single File: Shows only "Summary & Quality Checks" tab  
- Multi-File: Shows all three tabs including anomaly detection and charts  

---

## Architecture

```
QA Notebook
├── Smart Column Detection & Classification
├── Cached File Loading & Processing  
├── User-Controlled Quality Checks
├── Multi-Method Anomaly Detection Engine
├── Multi-Column Summary Grid Generator
├── Interactive Visualization & Charts
└── Session State Results Management
```
