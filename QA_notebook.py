import streamlit as st
import pandas as pd
import numpy as np
import io
import json
import hashlib
from datetime import datetime
import plotly.graph_objects as go
import warnings
warnings.filterwarnings('ignore')
import base64

st.set_page_config(
    page_title="FMD DQA Tool", 
    page_icon="🔍", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Open the image and convert to base64
with open("fmd.png", "rb") as f:
    data = f.read()
    encoded = base64.b64encode(data).decode()

# Render as centered and resized
st.markdown(
    f"""
    <div style="text-align: center;">
        <img src="data:image/png;base64,{encoded}" width="700"/>
    </div>
    """,
    unsafe_allow_html=True
)

class EnhancedDQATool:
    def __init__(self):
        self.results = {}
        self.uploaded_files = []
        
    def smart_column_detection(self, df):
        """Enhanced automatic column detection for dropdown population ONLY"""
        columns = {
            'numeric': [],
            'datetime': [],
            'reference': [],
            'price': [],
            'currency': [],
            'identifier': [],
            'grouping': [],
            'quality_check': []
        }
        
        for col in df.columns:
            col_lower = col.lower()
            
            # Price columns (highest priority for numeric analysis)
            if any(keyword in col_lower for keyword in ['mid', 'bid', 'ask', 'price', 'rate', 'yield']):
                if pd.api.types.is_numeric_dtype(df[col]):
                    columns['price'].append(col)
                    columns['numeric'].append(col)
                    columns['quality_check'].append(col)  # Important for quality checks
                continue
            
            # Currency columns
            if any(keyword in col_lower for keyword in ['ccy', 'currency', 'curr']):
                columns['currency'].append(col)
                columns['reference'].append(col)
                columns['grouping'].append(col)
                columns['quality_check'].append(col)
                continue
            
            # grouping columns for summary grid (addressing feedback #2)
            grouping_keywords = ['brand', 'ccypair', 'ccy1', 'ccy2', 'convention', 'subassetclass', 'assetclass', 
                               'pricetype', 'settle', 'skew', 'expiry', 'tenor', 'maturity']
            if any(keyword in col_lower for keyword in grouping_keywords):
                columns['grouping'].append(col)
                columns['reference'].append(col)
                # Don't auto-add optional fields like priceType to quality_check
                if not any(optional in col_lower for optional in ['pricetype', 'skew', 'expiry']):
                    columns['quality_check'].append(col)
                continue
            
            # Datetime columns
            if any(keyword in col_lower for keyword in ['time', 'date', 'timestamp']):
                columns['datetime'].append(col)
                continue
            
            # Identifier columns
            if any(keyword in col_lower for keyword in ['sym', 'id', 'name', 'code']):
                columns['identifier'].append(col)
                columns['reference'].append(col)
                columns['quality_check'].append(col)  # IDs are important for quality
                if col_lower in ['sym', 'symbol']:
                    columns['grouping'].append(col)
                continue
            
            # Generic numeric columns
            if pd.api.types.is_numeric_dtype(df[col]) and df[col].isna().mean() <= 0.5:
                columns['numeric'].append(col)
                columns['quality_check'].append(col)
                continue
            
            # Default to reference for non-numeric
            if not pd.api.types.is_numeric_dtype(df[col]):
                columns['reference'].append(col)
                # Check if it could be a grouping column
                if df[col].nunique() <= 50:  # Reasonable number of unique values for grouping
                    columns['grouping'].append(col)
                # Add to quality check if it seems important
                if df[col].count() / len(df) > 0.8:  # Most rows have data
                    columns['quality_check'].append(col)
        
        return columns

    def detect_header_and_load(self, file_content, filename):
        """Smart header detection and file loading (CSV and Excel support)"""
        try:
            file_extension = filename.lower().split('.')[-1]
            
            if file_extension in ['xlsx', 'xls']:
                # Handle Excel files
                try:
                    if isinstance(file_content, bytes):
                        excel_file = io.BytesIO(file_content)
                    else:
                        excel_file = io.StringIO(file_content)
                    
                    df = pd.read_excel(excel_file, sheet_name=0, engine='openpyxl' if file_extension == 'xlsx' else 'xlrd')
                    df.columns = df.columns.astype(str).str.strip().str.replace(' ', '_')
                    
                    if 'sym' not in df.columns:
                        sym_candidates = [col for col in df.columns if any(keyword in col.lower() 
                                         for keyword in ['sym', 'symbol', 'recordname', 'record_name'])]
                        if sym_candidates:
                            df.rename(columns={sym_candidates[0]: 'sym'}, inplace=True)
                        elif len(df.columns) > 0:
                            df.rename(columns={df.columns[0]: 'sym'}, inplace=True)
                    
                    df.reset_index(drop=True, inplace=True)
                    return df
                    
                except Exception as e:
                    st.error(f"Error reading Excel file {filename}: {str(e)}")
                    return None
            
            else:
                # Handle CSV files
                encodings = ['utf-8', 'latin1', 'cp1252']
                df = None
                
                for encoding in encodings:
                    try:
                        if isinstance(file_content, bytes):
                            content_str = file_content.decode(encoding)
                        else:
                            content_str = file_content
                        
                        preview = pd.read_csv(io.StringIO(content_str), header=None, nrows=3)
                        
                        if preview.empty:
                            continue
                        
                        first_row = preview.iloc[0].astype(str).str.lower()
                        keywords = {
                            'sym', 'symbol', 'mid', 'bid', 'ask', 'time', 'price', 'tenor', 
                            'brand', 'ccy', 'currency', 'ccypair', 'assetclass', 'subassetclass'
                        }
                        
                        header_match_count = sum(1 for item in first_row if any(keyword in str(item) for keyword in keywords))
                        header = 0 if header_match_count >= 2 else 1
                        
                        df = pd.read_csv(io.StringIO(content_str), header=header)
                        df.columns = df.columns.astype(str).str.strip().str.replace(' ', '_')
                        
                        if 'sym' not in df.columns:
                            sym_candidates = [col for col in df.columns if any(keyword in col.lower() 
                                             for keyword in ['sym', 'symbol', 'recordname', 'record_name'])]
                            if sym_candidates:
                                df.rename(columns={sym_candidates[0]: 'sym'}, inplace=True)
                            elif len(df.columns) > 0:
                                df.rename(columns={df.columns[0]: 'sym'}, inplace=True)
                        
                        df.reset_index(drop=True, inplace=True)
                        break
                        
                    except Exception:
                        continue
                
                if df is None:
                    st.error(f"Failed to load file {filename}")
                    return None
                    
                return df
            
        except Exception as e:
            st.error(f"Error loading file {filename}: {str(e)}")
            return None

    def analyze_single_file_anomaly_capability(self, df):
        """Analyze single file for anomaly detection capability with detailed feedback"""
        if 'sym' not in df.columns:
            return False, "❌ No symbol column found. Anomaly detection requires symbol grouping."
        
        symbol_counts = df['sym'].value_counts()
        symbols_with_multiple_records = symbol_counts[symbol_counts > 1]
        single_record_symbols = symbol_counts[symbol_counts == 1]
        
        feedback_parts = []
        
        if len(symbols_with_multiple_records) > 0:
            feedback_parts.append(f"✅ **Anomaly Detection Available**: {len(symbols_with_multiple_records)} symbols have multiple records")
            feedback_parts.append(f"   - Symbols with multiple records: {list(symbols_with_multiple_records.index[:5])}{'...' if len(symbols_with_multiple_records) > 5 else ''}")
            
            if len(single_record_symbols) > 0:
                feedback_parts.append(f"ℹ️ **Note**: {len(single_record_symbols)} symbols have single records (excluded from anomaly analysis)")
            
            return True, "\n\n".join(feedback_parts)
        else:
            feedback_parts.append(f"❌ **Anomaly Detection Not Available**: All symbols have single records only.")
            return False, "\n\n".join(feedback_parts)

    def check_non_empty(self, df, selected_columns=None):
        """Enhanced non-empty validation with user column selection"""
        # Use selected columns or all columns
        check_columns = selected_columns if selected_columns else df.columns.tolist()
        
        # Filter to only check columns that exist in the dataframe
        check_columns = [col for col in check_columns if col in df.columns]
        
        if not check_columns:
            return {
                'status': 'FAIL',
                'message': 'No valid columns selected for non-empty check',
                'total_rows': len(df),
                'data_rows': 0,
                'empty_rows': len(df),
                'sparse_rows': 0,
                'data_density': 0,
                'has_sufficient_data': False,
                'columns_checked': []
            }
        
        # Check only the selected columns
        subset_df = df[check_columns]
        
        total_rows = len(subset_df)
        empty_rows = subset_df.isnull().all(axis=1).sum()
        data_rows = total_rows - empty_rows
        
        # Calculate sparse rows (rows with minimal data in selected columns)
        min_data_threshold = max(1, len(check_columns) * 0.3)
        sparse_rows = ((subset_df.count(axis=1) < min_data_threshold) & (subset_df.count(axis=1) > 0)).sum()
        
        return {
            'total_rows': total_rows,
            'data_rows': data_rows,
            'empty_rows': empty_rows,
            'sparse_rows': sparse_rows,
            'data_density': round((data_rows / total_rows * 100), 2) if total_rows > 0 else 0,
            'has_sufficient_data': data_rows >= 10,
            'status': 'PASS' if data_rows >= 10 else 'FAIL',
            'message': f"Found {data_rows} data rows, {empty_rows} empty rows, {sparse_rows} sparse rows in {len(check_columns)} selected columns",
            'columns_checked': check_columns
        }

    def check_non_blank(self, df, selected_columns=None):
        """Enhanced non-blank validation with user column selection"""
        # Use selected columns or fall back to smart detection
        if selected_columns:
            check_columns = [col for col in selected_columns if col in df.columns]
        else:
            column_analysis = self.smart_column_detection(df)
            check_columns = column_analysis['quality_check']
        
        if not check_columns:
            return {
                'status': 'FAIL',
                'message': 'No valid columns selected for non-blank check',
                'columns_checked': [],
                'total_blank_fields': 0,
                'column_results': {}
            }
        
        results = {
            'columns_checked': check_columns,
            'total_blank_fields': 0,
            'column_results': {}
        }
        
        for col in check_columns:
            if col in df.columns:
                total_count = len(df)
                null_count = df[col].isna().sum()
                empty_str_count = (df[col].astype(str).str.strip() == '').sum()
                na_str_count = (df[col].astype(str).str.lower() == 'nan').sum()
                
                total_blank_count = null_count + empty_str_count + na_str_count
                
                results['column_results'][col] = {
                    'total': total_count,
                    'null_values': null_count,
                    'empty_strings': empty_str_count,
                    'na_strings': na_str_count,
                    'total_blanks': total_blank_count,
                    'blank_percentage': round((total_blank_count / total_count * 100), 2) if total_count > 0 else 0,
                    'data_quality_score': round(((total_count - total_blank_count) / total_count * 100), 1) if total_count > 0 else 0
                }
                
                results['total_blank_fields'] += total_blank_count
        
        results['status'] = 'PASS' if results['total_blank_fields'] == 0 else 'FAIL'
        results['message'] = f"Checked {len(check_columns)} selected columns, found {results['total_blank_fields']} blank fields"
        
        return results

    def check_expected_values(self, df, expected_values_config):
        """Enhanced expected values validation with PDF-style table format"""
        results = {}
        overall_compliance = []
        
        for column, expected_list in expected_values_config.items():
            if column in df.columns and expected_list:
                expected_set = set(str(val).strip().upper() for val in expected_list if str(val).strip())
                actual_values = set(df[column].dropna().astype(str).str.strip().str.upper().unique())
                
                unexpected = actual_values - expected_set
                missing = expected_set - actual_values
                compliant = actual_values & expected_set
                
                compliant_records = df[df[column].astype(str).str.strip().str.upper().isin(expected_set)].shape[0] if not df.empty else 0
                total_records = df[column].count()
                
                compliance_rate = (compliant_records / total_records * 100) if total_records > 0 else 0
                overall_compliance.append(compliance_rate)
                
                status = 'PASS' if len(unexpected) == 0 else 'FAIL'
                
                results[column] = {
                    'expected_count': len(expected_set),
                    'actual_count': len(actual_values),
                    'compliant_values': list(compliant),
                    'unexpected_values': list(unexpected),
                    'missing_values': list(missing),
                    'compliant_records': compliant_records,
                    'total_records': total_records,
                    'compliance_rate': round(compliance_rate, 2),
                    'coverage_rate': round((len(compliant) / len(expected_set) * 100), 2) if expected_set else 0,
                    'status': status,
                    'severity': 'HIGH' if compliance_rate < 50 else 'MEDIUM' if compliance_rate < 90 else 'LOW'
                }
        
        overall_status = 'PASS' if all(r['status'] == 'PASS' for r in results.values()) else 'FAIL'
        avg_compliance = round(np.mean(overall_compliance), 2) if overall_compliance else 100
        
        return {
            'column_results': results,
            'overall_compliance_rate': avg_compliance,
            'overall_status': overall_status,
            'columns_checked': len(results),
            'columns_passed': sum(1 for r in results.values() if r['status'] == 'PASS')
        }

    def validate_file_compatibility(self, uploaded_files):
        """Enhanced file compatibility validation with FIXED duplicate detection"""
        if len(uploaded_files) <= 1:
            return {"is_compatible": True, "message": "Single file analysis", "duplicates": []}
        
        file_hashes = {}
        duplicates = []
        
        for file in uploaded_files:
            try:
                file.seek(0)
                content = file.read()
                file.seek(0)
                
                content_hash = hashlib.md5(content).hexdigest()
                
                if content_hash in file_hashes:
                    duplicates.append({
                        'file1': file_hashes[content_hash],
                        'file2': file.name,
                        'hash': content_hash[:8]
                    })
                else:
                    file_hashes[content_hash] = file.name
                    
            except Exception as e:
                st.warning(f"Could not check {file.name} for duplicates: {str(e)}")
        
        if duplicates:
            st.warning(f"⚠️ **Duplicate files detected**: {len(duplicates)} duplicate(s) found")
            for dup in duplicates:
                st.error(f"🔴 **Duplicate**: '{dup['file1']}' and '{dup['file2']}' are identical (hash: {dup['hash']})")
        
        file_names = [f.name.lower() for f in uploaded_files]
        
        rates_indicators = ['rate', 'irs', 'swap', 'bond', 'yield', 'curve']
        fx_indicators = ['fx', 'spot', 'forward', 'currency', 'exchange']
        equity_indicators = ['equity', 'stock', 'share']
        
        file_types = set()
        for name in file_names:
            if any(indicator in name for indicator in rates_indicators):
                file_types.add('rates')
            elif any(indicator in name for indicator in fx_indicators):
                file_types.add('fx')
            elif any(indicator in name for indicator in equity_indicators):
                file_types.add('equity')
            else:
                file_types.add('unknown')
        
        known_types = file_types - {'unknown'}
        is_compatible = len(known_types) <= 1 and len(duplicates) == 0
        
        message_parts = []
        if len(known_types) > 1:
            message_parts.append(f"Mixed file types detected: {', '.join(known_types)}")
        if duplicates:
            message_parts.append(f"{len(duplicates)} duplicate file(s) detected")
        
        if not message_parts:
            message_parts.append("Files appear compatible")
        
        return {
            "is_compatible": is_compatible,
            "message": "; ".join(message_parts),
            "duplicates": duplicates,
            "file_types": list(known_types)
        }

    def generate_summary_grid(self, df, results, pivot_columns):
        """ENHANCED: Generate multi-column summary pivot table for business analysis"""
        try:
            # Handle both single column (string) and multiple columns (list)
            if isinstance(pivot_columns, str):
                pivot_columns = [pivot_columns]
            
            # Validate all pivot columns exist
            valid_pivot_columns = [col for col in pivot_columns if col in df.columns]
            
            if not valid_pivot_columns:
                # Fallback to first available grouping column
                column_analysis = self.smart_column_detection(df)
                available_grouping = [col for col in column_analysis['grouping'] if col in df.columns]
                if available_grouping:
                    valid_pivot_columns = [available_grouping[0]]
                else:
                    valid_pivot_columns = ['sym']
            
            # Create grouping strategy
            if len(valid_pivot_columns) == 1:
                grouping_key = valid_pivot_columns[0]
                df_copy = df.copy()
            else:
                # Multi-column grouping: combine values with separator
                df_copy = df.copy()
                df_copy['_group_key'] = df_copy[valid_pivot_columns].astype(str).agg(' | '.join, axis=1)
                grouping_key = '_group_key'
            
            summary_data = []
            
            # Get anomaly data if available
            anomaly_data = {}
            if 'anomaly_detection' in results and 'error' not in results['anomaly_detection']:
                for anomaly_key in ['spikes_summary', 'anomalies_summary']:
                    if anomaly_key in results['anomaly_detection'] and not results['anomaly_detection'][anomaly_key].empty:
                        anomaly_df = results['anomaly_detection'][anomaly_key].copy()
                        
                        # Create matching group key for anomaly data
                        if len(valid_pivot_columns) == 1 and valid_pivot_columns[0] in anomaly_df.columns:
                            anomaly_group_key = valid_pivot_columns[0]
                        elif len(valid_pivot_columns) > 1 and all(col in anomaly_df.columns for col in valid_pivot_columns):
                            anomaly_df['_group_key'] = anomaly_df[valid_pivot_columns].astype(str).agg(' | '.join, axis=1)
                            anomaly_group_key = '_group_key'
                        else:
                            continue
                        
                        anomaly_data = anomaly_df.groupby(anomaly_group_key).size().to_dict()
                        break
            
            # Generate summary for each group
            for group_name, group_df in df_copy.groupby(grouping_key):
                total_records = len(group_df)
                anomalies = anomaly_data.get(group_name, 0)
                
                # Calculate quality issues for this group
                non_empty_issues = 0
                non_blank_issues = 0
                expected_values_issues = 0
                
                # Non-empty issues (only if checked)
                if 'non_empty' in results and 'columns_checked' in results['non_empty']:
                    checked_cols = results['non_empty']['columns_checked']
                    if checked_cols:
                        group_subset = group_df[checked_cols]
                        empty_rows_in_group = group_subset.isnull().all(axis=1).sum()
                        non_empty_issues = empty_rows_in_group
                
                # Non-blank issues (only for checked columns)
                if 'non_blank' in results and 'column_results' in results['non_blank']:
                    for col in results['non_blank']['columns_checked']:
                        if col in group_df.columns:
                            group_null_count = group_df[col].isna().sum()
                            group_empty_str_count = (group_df[col].astype(str).str.strip() == '').sum()
                            group_na_str_count = (group_df[col].astype(str).str.lower() == 'nan').sum()
                            non_blank_issues += group_null_count + group_empty_str_count + group_na_str_count
                
                # Expected values issues (for this group)
                if 'expected_values' in results and 'column_results' in results['expected_values']:
                    for col, col_result in results['expected_values']['column_results'].items():
                        if col in group_df.columns:
                            expected_values = col_result.get('compliant_values', [])
                            if expected_values:
                                non_compliant_mask = ~group_df[col].astype(str).str.strip().str.upper().isin(expected_values)
                                expected_values_issues += non_compliant_mask.sum()
                
                # Calculate quality score
                total_issues = anomalies + non_empty_issues + non_blank_issues + expected_values_issues
                quality_score = max(0, round(((total_records - total_issues) / total_records * 100), 1)) if total_records > 0 else 100
                
                # Determine status
                if total_issues == 0:
                    status = "✅ PASS"
                elif total_issues <= total_records * 0.05:
                    status = "⚠️ WARNING"
                else:
                    status = "❌ FAIL"
                
                # Create row data
                row_data = {
                    'Group': group_name,
                    'Total_Records': total_records,
                    'Anomalies': anomalies,
                    'Non_Empty_Issues': non_empty_issues,
                    'Non_Blank_Issues': non_blank_issues,
                    'Expected_Values_Issues': expected_values_issues,
                    'Total_Issues': total_issues,
                    'Quality_Score_%': quality_score,
                    'Status': status
                }
                
                # Add individual pivot column values for multi-column grouping
                if len(valid_pivot_columns) > 1:
                    for i, col in enumerate(valid_pivot_columns):
                        if col in group_df.columns:
                            # Extract value from the combined group name
                            try:
                                values = str(group_name).split(' | ')
                                if i < len(values):
                                    row_data[f'{col}'] = values[i]
                                else:
                                    row_data[f'{col}'] = 'N/A'
                            except:
                                row_data[f'{col}'] = 'N/A'
                
                summary_data.append(row_data)
            
            summary_df = pd.DataFrame(summary_data)
            
            # Sort by total issues (worst first)
            if not summary_df.empty:
                summary_df = summary_df.sort_values('Total_Issues', ascending=False)
            
            return summary_df, None
            
        except Exception as e:
            return pd.DataFrame(), f"Error generating summary grid: {str(e)}"

    def run_percentage_change_analysis(self, df, value_columns, time_col, threshold=10.0):
        """NEW: Simple percentage change spike detection"""
        try:
            data = df.copy()
            
            # Clean datetime column
            if not pd.api.types.is_datetime64_any_dtype(data[time_col]):
                series = data[time_col].astype(str)
                series = series.str.replace('D', ' ', regex=False)
                series = series.str.replace(r'(\.\d{6})\d+', r'\1', regex=True)
                data[time_col] = pd.to_datetime(series, errors='coerce')
            
            data = data.dropna(subset=[time_col])
            
            # Check valid price columns
            valid_price_columns = []
            for col in value_columns:
                if col in data.columns and data[col].count() > 0:
                    valid_price_columns.append(col)
            
            if not valid_price_columns:
                return {'error': 'No valid price columns found', 'details': f'None of the selected price columns {value_columns} have valid data'}
            
            all_spikes = []
            
            # Process each price column
            for value_col in valid_price_columns:
                col_data = data.dropna(subset=[value_col])
                col_data = col_data[col_data[value_col] > 0]  # Avoid division by zero
                
                if len(col_data) < 2:
                    continue
                
                # Sort data properly
                col_data = col_data.sort_values(by=['sym', time_col])
                
                # Calculate context values
                col_data['prev_val'] = col_data.groupby('sym')[value_col].shift(1)
                col_data['next_val'] = col_data.groupby('sym')[value_col].shift(-1)
                
                # Calculate percentage change
                col_data['pct_change'] = col_data.groupby('sym')[value_col].transform(
                    lambda x: ((x - x.shift(1)) / x.shift(1) * 100)
                )
                
                # Calculate additional context
                col_data['rolling_mean'] = col_data.groupby('sym')[value_col].transform(
                    lambda x: x.rolling(window=min(10, len(x)), min_periods=1).mean()
                )
                col_data['price_column'] = value_col
                
                # Identify spikes based on threshold
                spikes = col_data[col_data['pct_change'].abs() > threshold].copy()
                
                if len(spikes) > 0:
                    all_spikes.append(spikes)
            
            if not all_spikes:
                return {
                    'spikes_summary': pd.DataFrame(),
                    'total_spikes': 0,
                    'affected_symbols': 0,
                    'method': 'Percentage Change Analysis',
                    'threshold_used': threshold,
                    'message': f'No spikes found above {threshold}% threshold in any price column'
                }
            
            # Combine all spikes
            combined_spikes = pd.concat(all_spikes, ignore_index=True)
            
            # Prepare summary
            summary_columns = [
                'sym', 'price_column', time_col, 'prev_val', value_columns[0], 'next_val',
                'rolling_mean', 'pct_change'
            ]
            
            available_columns = [col for col in summary_columns if col in combined_spikes.columns]
            
            if 'source_file' in combined_spikes.columns:
                available_columns.insert(1, 'source_file')
            
            # Add grouping columns
            column_analysis = self.smart_column_detection(df)
            for col in column_analysis['grouping']:
                if col in combined_spikes.columns and col not in available_columns:
                    available_columns.append(col)
            
            summary = combined_spikes[available_columns].copy()
            
            # Rename columns
            rename_dict = {
                time_col: 'Spike_Date',
                'prev_val': 'Prev_Value',
                'next_val': 'Next_Value',
                'rolling_mean': 'Rolling_Mean',
                'pct_change': 'Pct_Change',
                'price_column': 'Price_Column'
            }
            
            if value_columns[0] in summary.columns:
                rename_dict[value_columns[0]] = 'Spike_Value'
            
            summary.rename(columns=rename_dict, inplace=True)
            
            # Add severity classification
            summary['Severity'] = pd.cut(
                summary['Pct_Change'].abs(), 
                bins=[0, threshold, threshold*2, threshold*5, float('inf')], 
                labels=['Low', 'Medium', 'High', 'Critical']
            )
            
            return {
                'spikes_summary': summary,
                'total_spikes': len(summary),
                'affected_symbols': summary['sym'].nunique(),
                'method': 'Percentage Change Analysis',
                'threshold_used': threshold,
                'analysis_summary': {
                    'total_records_analyzed': len(data),
                    'symbols_analyzed': data['sym'].nunique(),
                    'price_columns_analyzed': valid_price_columns,
                    'date_range': f"{data[time_col].min()} to {data[time_col].max()}",
                    'avg_spike_magnitude': summary['Pct_Change'].abs().mean(),
                    'max_spike_magnitude': summary['Pct_Change'].abs().max()
                }
            }
            
        except Exception as e:
            return {'error': f'Percentage change analysis failed: {str(e)}'}

    def run_log_return_analysis(self, df, value_columns, time_col, threshold=0.25):
        """Enhanced log return spike detection with multiple price columns support"""
        try:
            data = df.copy()
            
            if not pd.api.types.is_datetime64_any_dtype(data[time_col]):
                series = data[time_col].astype(str)
                series = series.str.replace('D', ' ', regex=False)
                series = series.str.replace(r'(\.\d{6})\d+', r'\1', regex=True)
                data[time_col] = pd.to_datetime(series, errors='coerce')
            
            data = data.dropna(subset=[time_col])
            
            valid_price_columns = []
            for col in value_columns:
                if col in data.columns and data[col].count() > 0:
                    valid_price_columns.append(col)
            
            if not valid_price_columns:
                return {'error': 'No valid price columns found', 'details': f'None of the selected price columns {value_columns} have valid data'}
            
            all_spikes = []
            
            for value_col in valid_price_columns:
                col_data = data.dropna(subset=[value_col])
                col_data = col_data[col_data[value_col] > 0]
                
                if len(col_data) < 2:
                    continue
                
                col_data = col_data.sort_values(by=['sym', time_col])
                
                col_data['prev_val'] = col_data.groupby('sym')[value_col].shift(1)
                col_data['next_val'] = col_data.groupby('sym')[value_col].shift(-1)
                
                col_data['log_return'] = col_data.groupby('sym')[value_col].transform(
                    lambda x: np.log(x) - np.log(x.shift(1))
                )
                
                col_data['rolling_mean'] = col_data.groupby('sym')[value_col].transform(
                    lambda x: x.rolling(window=min(10, len(x)), min_periods=1).mean()
                )
                col_data['rolling_std'] = col_data.groupby('sym')[value_col].transform(
                    lambda x: x.rolling(window=min(10, len(x)), min_periods=1).std()
                )
                
                col_data['pct_change'] = col_data['log_return'] * 100
                col_data['price_column'] = value_col
                
                spikes = col_data[col_data['log_return'].abs() > threshold].copy()
                
                if len(spikes) > 0:
                    all_spikes.append(spikes)
            
            if not all_spikes:
                return {
                    'spikes_summary': pd.DataFrame(),
                    'total_spikes': 0,
                    'affected_symbols': 0,
                    'method': 'Log Return Analysis',
                    'threshold_used': threshold,
                    'message': f'No spikes found above threshold {threshold} in any price column'
                }
            
            combined_spikes = pd.concat(all_spikes, ignore_index=True)
            
            summary_columns = [
                'sym', 'price_column', time_col, 'prev_val', value_columns[0], 'next_val',
                'rolling_mean', 'rolling_std', 'log_return', 'pct_change'
            ]
            
            available_columns = [col for col in summary_columns if col in combined_spikes.columns]
            
            if 'source_file' in combined_spikes.columns:
                available_columns.insert(1, 'source_file')
            
            column_analysis = self.smart_column_detection(df)
            for col in column_analysis['grouping']:
                if col in combined_spikes.columns and col not in available_columns:
                    available_columns.append(col)
            
            summary = combined_spikes[available_columns].copy()
            
            rename_dict = {
                time_col: 'Spike_Date',
                'prev_val': 'Prev_Value',
                'next_val': 'Next_Value',
                'rolling_mean': 'Rolling_Mean',
                'rolling_std': 'Rolling_Std',
                'log_return': 'Log_Return',
                'pct_change': 'Pct_Change',
                'price_column': 'Price_Column'
            }
            
            if value_columns[0] in summary.columns:
                rename_dict[value_columns[0]] = 'Spike_Value'
            
            summary.rename(columns=rename_dict, inplace=True)
            
            summary['Severity'] = pd.cut(
                summary['Log_Return'].abs(), 
                bins=[0, threshold, threshold*2, threshold*5, float('inf')], 
                labels=['Low', 'Medium', 'High', 'Critical']
            )
            
            return {
                'spikes_summary': summary,
                'total_spikes': len(summary),
                'affected_symbols': summary['sym'].nunique(),
                'method': 'Log Return Analysis',
                'threshold_used': threshold,
                'analysis_summary': {
                    'total_records_analyzed': len(data),
                    'symbols_analyzed': data['sym'].nunique(),
                    'price_columns_analyzed': valid_price_columns,
                    'date_range': f"{data[time_col].min()} to {data[time_col].max()}",
                    'avg_spike_magnitude': summary['Log_Return'].abs().mean(),
                    'max_spike_magnitude': summary['Log_Return'].abs().max()
                }
            }
            
        except Exception as e:
            return {'error': f'Log return analysis failed: {str(e)}'}

    def run_z_score_analysis(self, df, value_columns, time_col, threshold=2.0):
        """REVISED: Z-score analysis based on price movements rather than price levels"""
        try:
            data = df.copy()
            
            # Clean datetime column
            if not pd.api.types.is_datetime64_any_dtype(data[time_col]):
                series = data[time_col].astype(str)
                series = series.str.replace('D', ' ', regex=False)
                series = series.str.replace(r'(\.\d{6})\d+', r'\1', regex=True)
                data[time_col] = pd.to_datetime(series, errors='coerce')
            
            data = data.dropna(subset=[time_col])
            
            # Check valid price columns
            valid_price_columns = []
            for col in value_columns:
                if col in data.columns and data[col].count() > 2:
                    valid_price_columns.append(col)
            
            if not valid_price_columns:
                return {'error': 'Insufficient data for Z-score analysis', 'details': f'Need at least 3 records per price column'}
            
            # Check if files follow similar datetime pattern (for multi-file analysis)
            file_pattern_check = True
            if 'source_file' in data.columns:
                files = data['source_file'].unique()
                if len(files) > 1:
                    # Check if file names have datetime patterns
                    import re
                    datetime_pattern = re.compile(r'\d{4}-?\d{2}-?\d{2}|\d{8}|\d{2}-?\d{2}-?\d{4}')
                    files_with_dates = [f for f in files if datetime_pattern.search(f)]
                    if len(files_with_dates) != len(files):
                        file_pattern_check = False
                        st.info("💡 **Z-Score Analysis Note**: File names don't follow datetime pattern. Results may be less accurate for cross-file anomaly detection.")
            
            all_anomalies = []
            
            # Process each price column separately
            for value_col in valid_price_columns:
                col_data = data.dropna(subset=[value_col])
                
                if len(col_data) < 3:
                    continue
                
                col_data = col_data.sort_values(by=['sym', time_col])
                
                # Skip symbols that only exist in one file (for multi-file analysis)
                if 'source_file' in col_data.columns and len(data['source_file'].unique()) > 1:
                    sym_file_counts = col_data.groupby('sym')['source_file'].nunique()
                    symbols_multi_files = sym_file_counts[sym_file_counts > 1].index
                    if len(symbols_multi_files) == 0:
                        continue
                    col_data = col_data[col_data['sym'].isin(symbols_multi_files)]
                
                if len(col_data) < 3:
                    continue
                
                # Calculate day-to-day absolute price movements for all symbols
                col_data['prev_val'] = col_data.groupby('sym')[value_col].shift(1)
                col_data['next_val'] = col_data.groupby('sym')[value_col].shift(-1)
                
                # Calculate absolute price movements (not percentage)
                col_data['price_movement'] = col_data.groupby('sym')[value_col].transform(
                    lambda x: (x - x.shift(1)).abs()
                )
                
                # Remove the first record of each symbol (no previous value)
                col_data = col_data.dropna(subset=['price_movement'])
                
                if len(col_data) == 0:
                    continue
                
                # REVISED APPROACH: Curve-by-curve analysis
                # If we have grouping columns that can define "curves", use them
                column_analysis = self.smart_column_detection(data)
                curve_columns = []
                
                # Look for curve-defining columns
                for col in column_analysis['grouping']:
                    if col in col_data.columns and col not in ['sym', 'source_file']:
                        # Check if this column creates meaningful groups for curve analysis
                        group_sizes = col_data.groupby(col).size()
                        if len(group_sizes) > 1 and group_sizes.min() >= 3:  # At least 3 records per curve
                            curve_columns.append(col)
                
                # If no suitable curve columns, analyze movements across all data
                if not curve_columns:
                    # Global movement analysis
                    movement_mean = col_data['price_movement'].mean()
                    movement_std = col_data['price_movement'].std(ddof=0)
                    
                    if movement_std == 0:
                        continue
                    
                    # Calculate Z-score based on movement deviation from mean movement
                    col_data['movement_z_score'] = (col_data['price_movement'] - movement_mean) / movement_std
                    col_data['curve_type'] = 'Global'
                else:
                    # Curve-by-curve movement analysis
                    curve_results = []
                    
                    for curve_col in curve_columns[:1]:  # Use the first suitable curve column
                        curve_data = col_data.copy()
                        
                        # Calculate movement statistics for each curve
                        curve_stats = curve_data.groupby(curve_col)['price_movement'].agg(['mean', 'std']).reset_index()
                        curve_stats['std'] = curve_stats['std'].fillna(0)
                        curve_stats = curve_stats[curve_stats['std'] > 0]  # Remove curves with zero std
                        
                        if len(curve_stats) == 0:
                            continue
                        
                        # Merge stats back to data
                        curve_data = curve_data.merge(
                            curve_stats.rename(columns={'mean': 'curve_movement_mean', 'std': 'curve_movement_std'}),
                            on=curve_col,
                            how='inner'
                        )
                        
                        # Calculate Z-score for movements within each curve
                        curve_data['movement_z_score'] = (
                            (curve_data['price_movement'] - curve_data['curve_movement_mean']) / 
                            curve_data['curve_movement_std']
                        )
                        curve_data['curve_type'] = f'{curve_col}_based'
                        
                        curve_results.append(curve_data)
                    
                    if curve_results:
                        col_data = pd.concat(curve_results, ignore_index=True)
                    else:
                        # Fallback to global analysis
                        movement_mean = col_data['price_movement'].mean()
                        movement_std = col_data['price_movement'].std(ddof=0)
                        
                        if movement_std == 0:
                            continue
                        
                        col_data['movement_z_score'] = (col_data['price_movement'] - movement_mean) / movement_std
                        col_data['curve_type'] = 'Global_Fallback'
                
                # Additional context calculations
                col_data['group_mean'] = col_data.groupby('sym')[value_col].transform('mean')
                col_data['group_std'] = col_data.groupby('sym')[value_col].transform(lambda x: x.std(ddof=0))
                col_data['group_median'] = col_data.groupby('sym')[value_col].transform('median')
                col_data['price_column'] = value_col
                
                # Identify anomalies based on movement Z-score threshold
                anomalies = col_data[col_data['movement_z_score'].abs() > threshold].copy()
                
                if len(anomalies) > 0:
                    all_anomalies.append(anomalies)
            
            if not all_anomalies:
                return {
                    'anomalies_summary': pd.DataFrame(),
                    'total_anomalies': 0,
                    'affected_symbols': 0,
                    'method': 'Z-Score Movement Analysis',
                    'threshold_used': threshold,
                    'message': f'No movement anomalies found above threshold {threshold} in any price column'
                }
            
            # Combine all anomalies
            combined_anomalies = pd.concat(all_anomalies, ignore_index=True)
            
            # Prepare summary
            summary_columns = [
                'sym', 'price_column', time_col, 'prev_val', value_columns[0], 'next_val',
                'price_movement', 'movement_z_score', 'curve_type', 'group_mean', 'group_std', 'group_median'
            ]
            
            available_columns = [col for col in summary_columns if col in combined_anomalies.columns]
            
            if 'source_file' in combined_anomalies.columns:
                available_columns.insert(1, 'source_file')
            
            # Add grouping columns for summary grid
            column_analysis = self.smart_column_detection(df)
            for col in column_analysis['grouping']:
                if col in combined_anomalies.columns and col not in available_columns:
                    available_columns.append(col)
            
            summary = combined_anomalies[available_columns].copy()
            
            # Rename columns for clarity
            rename_dict = {
                time_col: 'Anomaly_Date',
                'prev_val': 'Prev_Value',
                'next_val': 'Next_Value',
                'price_movement': 'Price_Movement',
                'movement_z_score': 'Movement_Z_Score',
                'curve_type': 'Analysis_Type',
                'group_mean': 'Symbol_Mean',
                'group_std': 'Symbol_Std',
                'group_median': 'Symbol_Median',
                'price_column': 'Price_Column'
            }
            
            if value_columns[0] in summary.columns:
                rename_dict[value_columns[0]] = 'Anomalous_Value'
            
            summary.rename(columns=rename_dict, inplace=True)
            
            # Add severity classification based on movement Z-score
            summary['Severity'] = pd.cut(
                summary['Movement_Z_Score'].abs(), 
                bins=[0, threshold, threshold*1.5, threshold*2.5, float('inf')], 
                labels=['Low', 'Medium', 'High', 'Critical']
            )
            
            # Add analysis details
            analysis_details = {
                'total_records_analyzed': len(data),
                'symbols_analyzed': data['sym'].nunique(),
                'price_columns_analyzed': valid_price_columns,
                'date_range': f"{data[time_col].min()} to {data[time_col].max()}",
                'avg_movement_z_score': summary['Movement_Z_Score'].abs().mean(),
                'max_movement_z_score': summary['Movement_Z_Score'].abs().max(),
                'analysis_approach': 'Movement-based Z-Score (curve-by-curve when possible)',
                'file_pattern_check': file_pattern_check
            }
            
            if 'source_file' in data.columns:
                analysis_details['files_analyzed'] = len(data['source_file'].unique())
                if len(data['source_file'].unique()) > 1:
                    # Check how many symbols span multiple files
                    multi_file_symbols = data.groupby('sym')['source_file'].nunique()
                    analysis_details['symbols_across_files'] = (multi_file_symbols > 1).sum()
            
            return {
                'anomalies_summary': summary,
                'total_anomalies': len(summary),
                'affected_symbols': summary['sym'].nunique(),
                'method': 'Z-Score Movement Analysis',
                'threshold_used': threshold,
                'analysis_summary': analysis_details
            }
            
        except Exception as e:
            return {'error': f'Z-score movement analysis failed: {str(e)}'}

    def create_notable_spike_charts(self, anomaly_results):
        """Create notable spike charts using Plotly"""
        charts = []
        
        if 'error' in anomaly_results:
            return charts
        
        if 'spikes_summary' in anomaly_results and not anomaly_results['spikes_summary'].empty:
            data = anomaly_results['spikes_summary']
            value_col = 'Spike_Value'
            date_col = 'Spike_Date'
            change_col = 'Log_Return' if 'Log_Return' in data.columns else 'Pct_Change'
            title_prefix = 'Spike'
        elif 'anomalies_summary' in anomaly_results and not anomaly_results['anomalies_summary'].empty:
            data = anomaly_results['anomalies_summary']
            value_col = 'Anomalous_Value'
            date_col = 'Anomaly_Date'
            change_col = 'Movement_Z_Score' if 'Movement_Z_Score' in data.columns else 'Z_Score'
            title_prefix = 'Anomaly'
        else:
            return charts
        
        unique_symbols = data['sym'].value_counts().head(10).index.tolist()
        
        for symbol in unique_symbols:
            symbol_data = data[data['sym'] == symbol].copy()
            
            if len(symbol_data) == 0:
                continue
            
            symbol_data[date_col] = pd.to_datetime(symbol_data[date_col])
            symbol_data = symbol_data.sort_values(date_col)
            
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=symbol_data[date_col],
                y=symbol_data[value_col],
                mode='lines+markers',
                name=f'{title_prefix} Values',
                line=dict(color='red', width=2),
                marker=dict(size=8, color='red')
            ))
            
            fig.add_trace(go.Scatter(
                x=symbol_data[date_col],
                y=symbol_data[value_col],
                mode='markers',
                name=f'{title_prefix} Points',
                marker=dict(size=12, color='darkred', symbol='circle-open', line=dict(width=2))
            ))
            
            fig.update_layout(
                title=f'{title_prefix} Detection for {symbol}',
                xaxis_title='Date',
                yaxis_title=f'{title_prefix} Value',
                height=400,
                showlegend=True,
                template='plotly_white'
            )
            
            fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
            fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
            
            charts.append((f'{symbol}_{title_prefix}_Chart', fig))
        
        return charts


# Cached file loading function
@st.cache_data
def load_and_process_files(file_data_list):
    """Cached file loading to prevent reloading on configuration changes"""
    tool = EnhancedDQATool()
    dfs = []
    file_info = []
    
    for name, content in file_data_list:
        try:
            df = tool.detect_header_and_load(content, name)
            if df is not None:
                df['source_file'] = name
                dfs.append(df)
                file_info.append({
                    'name': name,
                    'rows': len(df),
                    'columns': len(df.columns),
                    'type': 'Excel' if name.lower().endswith(('.xlsx', '.xls')) else 'CSV'
                })
        except Exception as e:
            st.error(f"Error loading {name}: {str(e)}")
            
    return dfs, file_info


def json_serializer(obj):
    """Custom JSON serializer for pandas and numpy objects"""
    if isinstance(obj, (pd.Timestamp, pd.DatetimeIndex)):
        return obj.isoformat()
    elif isinstance(obj, (pd.DataFrame, pd.Series)):
        return obj.to_dict()
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif pd.isna(obj):
        return None
    else:
        return str(obj)


def initialize_session_state():
    """Initialize all session state variables to prevent page reloads"""
    # Analysis results storage
    if 'analysis_results' not in st.session_state:
        st.session_state.analysis_results = None
    if 'analysis_file_info' not in st.session_state:
        st.session_state.analysis_file_info = None
    if 'combined_dataframe' not in st.session_state:
        st.session_state.combined_dataframe = None
    
    # Configuration state - FIXED: Initialize all config to prevent reloads
    if 'config_check_non_empty' not in st.session_state:
        st.session_state.config_check_non_empty = True
    if 'config_check_non_blank' not in st.session_state:
        st.session_state.config_check_non_blank = True
    if 'config_check_expected_values' not in st.session_state:
        st.session_state.config_check_expected_values = True
    if 'config_run_anomaly_detection' not in st.session_state:
        st.session_state.config_run_anomaly_detection = True
    if 'config_detection_method' not in st.session_state:
        st.session_state.config_detection_method = "Log Return Spike Detection"
    if 'config_threshold' not in st.session_state:
        st.session_state.config_threshold = 0.25
    
    # Column selections
    if 'config_non_empty_columns' not in st.session_state:
        st.session_state.config_non_empty_columns = []
    if 'config_non_blank_columns' not in st.session_state:
        st.session_state.config_non_blank_columns = []
    if 'config_value_columns' not in st.session_state:
        st.session_state.config_value_columns = []
    if 'config_time_column' not in st.session_state:
        st.session_state.config_time_column = ""
    if 'config_expected_values_columns' not in st.session_state:
        st.session_state.config_expected_values_columns = []
    
    # Summary grid state - FIXED: Default to 'brand'
    if 'config_pivot_columns' not in st.session_state:
        st.session_state.config_pivot_columns = []  # Will be set to ['brand'] when data loads
    
    # Expected values inputs
    if 'expected_values_inputs' not in st.session_state:
        st.session_state.expected_values_inputs = {}


def main():
    # Initialize session state first
    initialize_session_state()
    
    # Initialize the tool
    tool = EnhancedDQATool()
    
    # Main title and description
    st.markdown("<h1 style='text-align: center;'>🔍 Data Quality Assurance Tool</h1>", unsafe_allow_html=True)
    
    # Sidebar for configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # File upload section
        st.subheader("📁 Upload Files")
        uploaded_files = st.file_uploader(
            "Select CSV or Excel files for analysis",
            type=['csv', 'xlsx', 'xls'],
            accept_multiple_files=True,
            help="Upload one or more CSV or Excel files. Multiple files enable cross-file anomaly detection."
        )
        
        # Show file count and status
        if uploaded_files:
            st.success(f"✅ {len(uploaded_files)} file(s) uploaded")
            for i, file in enumerate(uploaded_files, 1):
                file_type = "📊 Excel" if file.name.lower().endswith(('.xlsx', '.xls')) else "📄 CSV"
                st.caption(f"{i}. {file_type} - {file.name}")
        else:
            st.info("Please upload at least one CSV or Excel file to begin analysis")
    
    # Main content area
    if not uploaded_files:
        # Welcome screen
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.markdown("""
            <div style='text-align: center; padding: 2rem; background-color: #f8f9fa; border-radius: 10px; margin: 2rem 0; border: 1px solid #dee2e6;'>
                <h3 style='color: #2c3e50; margin-bottom: 1rem;'>🚀 Welcome to DQA Tool</h3>
                <p style='color: #5a6c7d; font-size: 16px; margin-bottom: 1.5rem;'>Upload your CSV or Excel files to get started with comprehensive data quality analysis.</p>
                <h4 style='color: #2c3e50; margin-bottom: 1rem;'>✨ Key Features:</h4>
                <ul style='text-align: left; display: inline-block; color: #495057; line-height: 1.6; font-size: 14px;'>
                    <li>✅ <strong style='color: #2c3e50;'>Data Integrity Checks</strong></li>
                    <li>🎯 <strong style='color: #2c3e50;'>Expected Values Validation</strong></li>
                    <li>📈 <strong style='color: #2c3e50;'>Statistical Analysis- Anomaly detection</strong></li>
                    <li>📊 <strong style='color: #2c3e50;'>Summary Grid</strong></li>
                    <li>📈 <strong style='color: #2c3e50;'>Visualizations/Charts</strong></li>
                </ul>
            </div>
            """, unsafe_allow_html=True)
            
        return
    
    # Store uploaded files in tool
    tool.uploaded_files = uploaded_files
    
    # Load and process files (cached)
    try:
        file_data_list = []
        for file in uploaded_files:
            file.seek(0)
            content = file.read()
            file.seek(0)
            file_data_list.append((file.name, content))
        
        with st.spinner("Loading and processing files..."):
            dfs, file_info = load_and_process_files(file_data_list)
            
    except Exception as e:
        st.error(f"Error processing files: {str(e)}")
        return
    
    if not dfs:
        st.error("❌ No files could be loaded successfully. Please check your file formats.")
        return
    
    # Combine dataframes
    if len(dfs) == 1:
        combined_df = dfs[0]
    else:
        combined_df = pd.concat(dfs, ignore_index=True)
    
    # Check for duplicates immediately after loading
    if len(uploaded_files) > 1:
        duplicate_check = tool.validate_file_compatibility(uploaded_files)
    
    # Display file summary
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Files Loaded", len(dfs))
    with col2:
        st.metric("Total Records", len(combined_df))
    with col3:
        st.metric("Total Columns", len(combined_df.columns))
    with col4:
        st.metric("Unique Symbols", combined_df['sym'].nunique() if 'sym' in combined_df.columns else 0)
    
    # FIXED: Configuration section that doesn't trigger page reloads
    st.subheader("Quality Check Configuration")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.session_state.config_check_non_empty = st.checkbox(
            "Non-Empty Validation", 
            value=st.session_state.config_check_non_empty,
            key="check_non_empty"
        )
    with col2:
        st.session_state.config_check_non_blank = st.checkbox(
            "Non-Blank Validation", 
            value=st.session_state.config_check_non_blank,
            key="check_non_blank"
        ) 
    with col3:
        st.session_state.config_check_expected_values = st.checkbox(
            "Expected Values Validation", 
            value=st.session_state.config_check_expected_values,
            key="check_expected_values"
        )
    
    # Column Selection for Quality Checks
    st.subheader("📋 Column Selection for Quality Checks")
    
    # Get column suggestions
    column_analysis = tool.smart_column_detection(combined_df)
    all_columns = [col for col in combined_df.columns if col != 'source_file']
    all_numeric = combined_df.select_dtypes(include=[np.number]).columns.tolist()
    
    col1, col2 = st.columns(2)
    with col1:
        if st.session_state.config_check_non_empty:
            st.write("**Non-Empty Check Columns:**")
            # Set default if not already set
            if not st.session_state.config_non_empty_columns:
                st.session_state.config_non_empty_columns = [col for col in column_analysis['quality_check'] if col != 'source_file']
            
            st.session_state.config_non_empty_columns = st.multiselect(
                "Select columns to check for empty rows",
                all_columns,
                default=st.session_state.config_non_empty_columns,
                help="Choose columns that should not be completely empty. Exclude optional fields if needed.",
                key="non_empty_cols"
            )
    
    with col2:
        if st.session_state.config_check_non_blank:
            st.write("**Non-Blank Check Columns:**")
            # Set default if not already set
            if not st.session_state.config_non_blank_columns:
                st.session_state.config_non_blank_columns = [col for col in column_analysis['quality_check'] if col != 'source_file']
            
            st.session_state.config_non_blank_columns = st.multiselect(
                "Select columns to check for blank values", 
                all_columns,
                default=st.session_state.config_non_blank_columns,
                help="Choose columns that should not contain blank/null values. Optional fields like 'priceType' can be excluded.",
                key="non_blank_cols"
            )
    
    # Price and Time Column Selection
    st.subheader("📊 Price and Time Column Selection")
    
    col1, col2 = st.columns(2)
    with col1:        
        st.session_state.config_value_columns = st.multiselect(
            "Price Columns for Analysis",
            all_numeric,
            default=st.session_state.config_value_columns,
            help="Select one or more price columns to analyze (e.g., mid, bid, ask)",
            key="value_cols"
        )
        
        if not st.session_state.config_value_columns:
            st.info("💡**No price columns selected**")
    
    with col2:
        time_columns = [col for col in combined_df.columns if 'time' in col.lower() or 'date' in col.lower()]
        if not time_columns:
            time_columns = list(combined_df.columns)
        
        st.session_state.config_time_column = st.selectbox(
            "DateTime Column for Analysis",
            [""] + time_columns,
            index=([""] + time_columns).index(st.session_state.config_time_column) if st.session_state.config_time_column in time_columns else 0,
            help="Select the datetime column for time-series analysis. Empty selection means no datetime column available.",
            key="time_col"
        )
        
        if st.session_state.config_time_column == "":
            st.info("💡**No datetime column selected**")
    
    # Expected Values Configuration
    expected_values_config = {}
    if st.session_state.config_check_expected_values:
        st.subheader("🎯 Expected Values Configuration")
        
        st.session_state.config_expected_values_columns = st.multiselect(
            "Select columns for expected values validation",
            all_columns,
            default=st.session_state.config_expected_values_columns,
            help="Choose columns to validate against predefined value lists",
            key="expected_values_cols"
        )
        
        for col in st.session_state.config_expected_values_columns:
            st.write(f"**Expected values for {col}:**")
            
            # Use session state for expected values input
            input_key = f"expected_{col}"
            if input_key not in st.session_state.expected_values_inputs:
                st.session_state.expected_values_inputs[input_key] = ""
            
            values_input = st.text_input(
                f"Enter expected values for {col} (comma-separated)",
                value=st.session_state.expected_values_inputs[input_key],
                placeholder="USDCAD, USDGBP, EURUSD, USDSEK",
                help="Enter values separated by commas",
                key=f"input_{input_key}"
            )
            
            # Update session state
            st.session_state.expected_values_inputs[input_key] = values_input
            
            if values_input.strip():
                expected_values_config[col] = [v.strip() for v in values_input.split(',') if v.strip()]
    
    # File Analysis
    if len(uploaded_files) > 1:
        show_anomaly_config = True
        anomaly_capable = True
    else:
        anomaly_capable, feedback_message = tool.analyze_single_file_anomaly_capability(combined_df)
        st.markdown(feedback_message)
        show_anomaly_config = anomaly_capable
    
    # Anomaly Detection Configuration
    if show_anomaly_config:
        st.subheader("🚨 Advanced Anomaly Detection")
        
        st.session_state.config_run_anomaly_detection = st.checkbox(
            "Enable Anomaly Detection", 
            value=st.session_state.config_run_anomaly_detection,
            help="Enable statistical anomaly detection",
            key="run_anomaly"
        )
        
        if st.session_state.config_run_anomaly_detection:
            col1, col2 = st.columns(2)
            
            with col1:
                method_options = ["Log Return Spike Detection", "Z-Score Statistical Analysis", "Percentage Change Detection"]
                current_index = method_options.index(st.session_state.config_detection_method) if st.session_state.config_detection_method in method_options else 0
                
                st.session_state.config_detection_method = st.selectbox(
                    "Detection Method",
                    method_options,
                    index=current_index,
                    help="Choose the statistical method for anomaly detection",
                    key="detection_method"
                )
            
            with col2:
                # Threshold configuration
                if st.session_state.config_detection_method == "Log Return Spike Detection":
                    st.session_state.config_threshold = st.slider(
                        "Log Return Threshold", 0.01, 2.0, 
                        value=st.session_state.config_threshold if 0.01 <= st.session_state.config_threshold <= 2.0 else 0.25,
                        step=0.05,
                        help="Lower values = more sensitive detection",
                        key="threshold_log"
                    )
                elif st.session_state.config_detection_method == "Z-Score Statistical Analysis":
                    st.session_state.config_threshold = st.slider(
                        "Z-Score Threshold", 1.0, 5.0,
                        value=st.session_state.config_threshold if 1.0 <= st.session_state.config_threshold <= 5.0 else 2.0,
                        step=0.1,
                        help="Standard deviations from mean movement to flag as anomaly",
                        key="threshold_z"
                    )
                else:  # Percentage Change Detection
                    st.session_state.config_threshold = st.slider(
                        "Percentage Change Threshold (%)", 1.0, 100.0,
                        value=st.session_state.config_threshold if 1.0 <= st.session_state.config_threshold <= 100.0 else 10.0,
                        step=1.0,
                        help="Percentage change threshold to flag as spike",
                        key="threshold_pct"
                    )
    
    # Validation warnings
    if show_anomaly_config and st.session_state.config_run_anomaly_detection:
        warnings = []
        if not st.session_state.config_value_columns:
            warnings.append("⚠️ Please select at least one price column")
        if not st.session_state.config_time_column:
            warnings.append("⚠️ Please select a datetime column")
        
        if warnings:
            for warning in warnings:
                st.warning(warning)
    
    # FIXED: Analysis button that triggers analysis ONLY when clicked
    if st.button("🔍 Run Comprehensive Analysis", type="primary", use_container_width=True):
        
        with st.spinner("Running comprehensive data quality analysis..."):
            results = {}
            
            # Use session state values for analysis
            if st.session_state.config_check_non_empty:
                with st.status("Checking data completeness..."):
                    results['non_empty'] = tool.check_non_empty(
                        combined_df, 
                        st.session_state.config_non_empty_columns if st.session_state.config_non_empty_columns else None
                    )
            
            if st.session_state.config_check_non_blank:
                with st.status("Analyzing data integrity..."):
                    results['non_blank'] = tool.check_non_blank(
                        combined_df, 
                        st.session_state.config_non_blank_columns if st.session_state.config_non_blank_columns else None
                    )
            
            if st.session_state.config_check_expected_values and expected_values_config:
                with st.status("Validating expected values..."):
                    results['expected_values'] = tool.check_expected_values(combined_df, expected_values_config)
            
            # File validation
            with st.status("Validating file compatibility..."):
                results['file_validation'] = tool.validate_file_compatibility(uploaded_files)
            
            # Anomaly detection
            if show_anomaly_config and st.session_state.config_run_anomaly_detection:
                if not st.session_state.config_value_columns:
                    results['anomaly_detection'] = {'error': 'No price columns selected for analysis'}
                elif not st.session_state.config_time_column:
                    results['anomaly_detection'] = {'error': 'No datetime column selected for analysis'}
                else:
                    with st.status("Running anomaly detection..."):
                        if st.session_state.config_detection_method == "Log Return Spike Detection":
                            results['anomaly_detection'] = tool.run_log_return_analysis(
                                combined_df, st.session_state.config_value_columns, 
                                st.session_state.config_time_column, st.session_state.config_threshold
                            )
                        elif st.session_state.config_detection_method == "Z-Score Statistical Analysis":
                            results['anomaly_detection'] = tool.run_z_score_analysis(
                                combined_df, st.session_state.config_value_columns, 
                                st.session_state.config_time_column, st.session_state.config_threshold
                            )
                        else:  # Percentage Change Detection
                            results['anomaly_detection'] = tool.run_percentage_change_analysis(
                                combined_df, st.session_state.config_value_columns, 
                                st.session_state.config_time_column, st.session_state.config_threshold
                            )
        
        # Store results in session state
        st.session_state.analysis_results = results
        st.session_state.analysis_file_info = file_info
        st.session_state.combined_dataframe = combined_df
        
        # FIXED: Set default pivot columns to 'brand' when results first generate
        if not st.session_state.config_pivot_columns:
            available_grouping = [col for col in column_analysis['grouping'] if col in combined_df.columns]
            if 'brand' in available_grouping:
                st.session_state.config_pivot_columns = ['brand']
            elif available_grouping:
                st.session_state.config_pivot_columns = [available_grouping[0]]
            else:
                st.session_state.config_pivot_columns = ['sym']
        
        tool.results = results
        st.success("✅ Analysis completed successfully!")
    
    # Display results
    if st.session_state.analysis_results is not None:
        results = st.session_state.analysis_results
        file_info = st.session_state.analysis_file_info
        combined_df = st.session_state.combined_dataframe
        
        # Clear results button
        col1, col2 = st.columns([3, 1])
        with col1:
            st.header("📊 Analysis Results Dashboard")
        with col2:
            if st.button("🗑️ Clear Results"):
                st.session_state.analysis_results = None
                st.session_state.analysis_file_info = None
                st.session_state.combined_dataframe = None
                st.rerun()
        
        # Summary metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if 'non_empty' in results:
                status = "✅" if results['non_empty']['status'] == 'PASS' else "❌"
                st.metric(
                    "Data Completeness",
                    f"{status} {results['non_empty']['status']}",
                    f"{results['non_empty']['data_density']}% density"
                )
        
        with col2:
            if 'non_blank' in results:
                status = "✅" if results['non_blank']['status'] == 'PASS' else "❌"
                st.metric(
                    "Data Integrity", 
                    f"{status} {results['non_blank']['status']}",
                    f"{results['non_blank']['total_blank_fields']} blank fields"
                )
        
        with col3:
            if 'anomaly_detection' in results and 'error' not in results['anomaly_detection']:
                total_anomalies = results['anomaly_detection'].get('total_spikes', 0) or results['anomaly_detection'].get('total_anomalies', 0)
                status = "✅" if total_anomalies == 0 else "⚠️"
                st.metric(
                    "Anomalies Detected",
                    f"{status} {total_anomalies}",
                    f"{results['anomaly_detection'].get('affected_symbols', 0)} symbols affected"
                )
            else:
                st.metric("Anomalies Detected", "Not Available", "Single entry per sym")
        
        # FIXED: Show anomaly tabs based on actual anomaly detection results, not just file count
        show_anomaly_tabs = False
        
        # Check if anomaly detection was run and has results
        if ('anomaly_detection' in results and 
            'error' not in results['anomaly_detection']):
            
            # Check if any anomalies were actually found
            total_anomalies = (results['anomaly_detection'].get('total_spikes', 0) or 
                             results['anomaly_detection'].get('total_anomalies', 0))
            
            # Show tabs if anomalies found OR if it's multi-file (even with zero anomalies)
            show_anomaly_tabs = total_anomalies > 0 or len(uploaded_files) > 1
        
        if show_anomaly_tabs:
            # Show all tabs including anomaly detection
            tab1, tab2, tab3 = st.tabs(["📋 Summary & Quality Checks", "🚨 Anomaly Detection", "📈 Notable Spike Charts"])
            
            with tab1:
                display_summary_and_quality_checks(combined_df, results, tool)
            
            with tab2:
                display_anomaly_detection_results(results)
            
            with tab3:
                display_spike_charts(results, tool)
        else:
            # Show only summary tab (single file with no anomalies or no anomaly detection run)
            tab1, = st.tabs(["📋 Summary & Quality Checks"])
            
            with tab1:
                display_summary_and_quality_checks(combined_df, results, tool)
        
        # Download Results
        display_download_section(results, file_info, combined_df)


def display_summary_and_quality_checks(combined_df, results, tool):
    """Display summary grid and quality check results"""
    st.subheader("📊 Summary Grid")
    
    column_analysis = tool.smart_column_detection(combined_df)
    available_grouping_columns = [col for col in column_analysis['grouping'] if col in combined_df.columns]
    
    if not available_grouping_columns:
        available_grouping_columns = ['sym']  
    
    # FIXED: Default to 'brand' preference, allow user editing
    if 'brand' in available_grouping_columns and not st.session_state.config_pivot_columns:
        st.session_state.config_pivot_columns = ['brand']
    elif not st.session_state.config_pivot_columns:
        st.session_state.config_pivot_columns = [available_grouping_columns[0]]
    
    # Multi-select for grouping columns
    st.session_state.config_pivot_columns = st.multiselect(
        "Group by (select one or more):",
        available_grouping_columns,
        default=st.session_state.config_pivot_columns,
        help="Select multiple attributes to create detailed groupings (e.g., brand + priceType + convention)",
        key="pivot_cols"
    )
    
    if not st.session_state.config_pivot_columns:
        st.warning("⚠️ Please select at least one grouping column")
        st.session_state.config_pivot_columns = [available_grouping_columns[0]] if available_grouping_columns else ['sym']
    
    # Generate and display summary grid
    try:
        summary_df, error_msg = tool.generate_summary_grid(combined_df, results, st.session_state.config_pivot_columns)
        
        if error_msg:
            st.error(f"Error generating summary: {error_msg}")
        elif summary_df.empty:
            st.warning("No data available for summary grid")
        else:
            # Display summary statistics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Groups", len(summary_df))
            with col2:
                groups_with_issues = len(summary_df[summary_df['Total_Issues'] > 0])
                st.metric("Groups with Issues", groups_with_issues)
            with col3:
                avg_quality = summary_df['Quality_Score_%'].mean()
                st.metric("Average Quality Score", f"{avg_quality:.1f}%")
            with col4:
                pass_rate = len(summary_df[summary_df['Status'].str.contains('PASS')]) / len(summary_df) * 100
                st.metric("Pass Rate", f"{pass_rate:.1f}%")
            
            # Display the summary grid
            st.dataframe(
                summary_df,
                use_container_width=True,
                column_config={
                    "Status": st.column_config.TextColumn(
                        "Status",
                        help="Overall quality status for this group"
                    ),
                    "Quality_Score_%": st.column_config.ProgressColumn(
                        "Quality Score %",
                        help="Overall data quality percentage",
                        min_value=0,
                        max_value=100,
                        format="%.1f%%"
                    ),
                    "Total_Issues": st.column_config.NumberColumn(
                        "Total Issues",
                        help="Sum of all data quality issues",
                        format="%d"
                    )
                }
            )
            
    except Exception as e:
        st.error(f"Error generating summary grid: {str(e)}")
    
    # Quality Checks Section
    st.subheader("🔍 Detailed Quality Check Results")
    
    # Non-Empty Check Results
    if 'non_empty' in results:
        with st.expander("📊 Data Completeness Analysis", expanded=True):
            result = results['non_empty']
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Rows", f"{result['total_rows']:,}")
            with col2:
                st.metric("Data Rows", f"{result['data_rows']:,}")
            with col3:
                st.metric("Empty Rows", f"{result['empty_rows']:,}")
            with col4:
                st.metric("Data Density", f"{result['data_density']}%")
            
            if result['status'] == 'PASS':
                st.success(f"✅ {result['message']}")
            else:
                st.error(f"❌ {result['message']}")
    
    # Non-Blank Check Results
    if 'non_blank' in results:
        with st.expander("🔍 Data Integrity Analysis", expanded=True):
            result = results['non_blank']
            
            if result.get('column_results'):
                blank_df = pd.DataFrame(result['column_results']).T
                st.dataframe(blank_df, use_container_width=True)
            
            if result['status'] == 'PASS':
                st.success(f"✅ {result['message']}")
            else:
                st.error(f"❌ {result['message']}")
    
    # Expected Values Check Results
    if 'expected_values' in results:
        with st.expander("🎯 Expected Values Validation", expanded=True):
            result = results['expected_values']
            
            table_data = []
            for column, col_result in result['column_results'].items():
                table_data.append({
                    'Column': column,
                    'Expected_Count': col_result['expected_count'],
                    'Actual_Count': col_result['actual_count'],
                    'Compliant_Records': f"{col_result['compliant_records']}/{col_result['total_records']}",
                    'Compliance_Rate_%': col_result['compliance_rate'],
                    'Coverage_Rate_%': col_result['coverage_rate'],
                    'Status': col_result['status'],
                    'Unexpected_Values': ', '.join(col_result['unexpected_values'][:3]) + ('...' if len(col_result['unexpected_values']) > 3 else ''),
                    'Missing_Values': ', '.join(col_result['missing_values'][:3]) + ('...' if len(col_result['missing_values']) > 3 else '')
                })
            
            if table_data:
                summary_table = pd.DataFrame(table_data)
                st.dataframe(summary_table, use_container_width=True)
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Overall Compliance", f"{result['overall_compliance_rate']}%")
                with col2:
                    st.metric("Columns Checked", result['columns_checked'])
                with col3:
                    st.metric("Columns Passed", result['columns_passed'])
            else:
                st.info("No expected values configured")


def display_anomaly_detection_results(results):
    """Display anomaly detection results"""
    st.subheader("🚨 Anomaly Detection Results")
    
    if 'anomaly_detection' not in results:
        st.info("Anomaly detection was not enabled for this analysis.")
    elif 'error' in results['anomaly_detection']:
        st.error(f"❌ Anomaly detection failed: {results['anomaly_detection']['error']}")
        if 'details' in results['anomaly_detection']:
            st.info(f"Details: {results['anomaly_detection']['details']}")
    else:
        anomaly_result = results['anomaly_detection']
        
        # Summary metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            total_anomalies = anomaly_result.get('total_spikes', 0) or anomaly_result.get('total_anomalies', 0)
            st.metric("Total Anomalies", f"{total_anomalies:,}")
        with col2:
            st.metric("Affected Symbols", f"{anomaly_result.get('affected_symbols', 0):,}")
        with col3:
            st.metric("Detection Method", anomaly_result.get('method', 'Unknown'))
        
        # Threshold information
        if 'threshold_used' in anomaly_result:
            threshold_info = f"**Threshold used**: {anomaly_result['threshold_used']}"
            if 'Percentage' in anomaly_result.get('method', ''):
                threshold_info += "%"
            st.info(threshold_info)
        
        # Analysis details for Z-Score method
        if anomaly_result.get('method') == 'Z-Score Movement Analysis' and 'analysis_summary' in anomaly_result:
            analysis = anomaly_result['analysis_summary']
            
            with st.expander("📊 Z-Score Analysis Details", expanded=False):
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"**Analysis Approach**: {analysis.get('analysis_approach', 'Standard')}")
                    st.write(f"**Records Analyzed**: {analysis.get('total_records_analyzed', 0):,}")
                    st.write(f"**Symbols Analyzed**: {analysis.get('symbols_analyzed', 0):,}")
                with col2:
                    if 'files_analyzed' in analysis:
                        st.write(f"**Files Analyzed**: {analysis['files_analyzed']}")
                    if 'symbols_across_files' in analysis:
                        st.write(f"**Symbols Across Files**: {analysis['symbols_across_files']}")
                    st.write(f"**Date Range**: {analysis.get('date_range', 'N/A')}")
        
        # Anomaly details
        if 'spikes_summary' in anomaly_result and not anomaly_result['spikes_summary'].empty:
            st.write("**Detected Spikes:**")
            st.dataframe(anomaly_result['spikes_summary'], use_container_width=True)
        elif 'anomalies_summary' in anomaly_result and not anomaly_result['anomalies_summary'].empty:
            st.write("**Detected Anomalies:**")
            st.dataframe(anomaly_result['anomalies_summary'], use_container_width=True)
        elif total_anomalies == 0:
            st.success("✅ No anomalies detected in the data!")


def display_spike_charts(results, tool):
    """Display spike charts"""
    st.subheader("📈 Notable Spike Charts")
    
    if 'anomaly_detection' not in results:
        st.info("Notable spike charts are available when anomaly detection is enabled.")
    elif 'error' in results['anomaly_detection']:
        st.warning("⚠️ Cannot generate spike charts due to anomaly detection error.")
        st.info("Please check the Anomaly Detection tab for more details.")
    else:
        anomaly_result = results['anomaly_detection']
        total_anomalies = anomaly_result.get('total_spikes', 0) or anomaly_result.get('total_anomalies', 0)
        
        if total_anomalies > 0:
            charts = tool.create_notable_spike_charts(anomaly_result)
            
            if charts:
                st.info(f"📊 Displaying spike charts for top {len(charts)} symbols with most anomalies")
                
                for chart_name, fig in charts:
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("⚠️ No chart data available for the detected anomalies.")
                st.info("This may occur if anomalies lack sufficient date/value information for visualization.")
        else:
            st.success("✅ No anomalies detected - no spike charts to display!")
            st.info("Spike charts will appear here when anomalies are detected in your data.")


def display_download_section(results, file_info, combined_df):
    """Display download section"""
    st.header("💾 Download Results")
    
    report_data = {
        'analysis_timestamp': datetime.now().isoformat(),
        'files_analyzed': [info['name'] for info in file_info],
        'total_records': len(combined_df),
        'total_columns': len(combined_df.columns),
        'results': results
    }
    
    report_json = json.dumps(report_data, indent=2, default=json_serializer)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.download_button(
            label="📄 Download JSON Report",
            data=report_json,
            file_name=f"dqa_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json",
            use_container_width=True
        )
    
    with col2:
        if ('anomaly_detection' in results and 
            'error' not in results['anomaly_detection']):
            
            if ('spikes_summary' in results['anomaly_detection'] and 
                not results['anomaly_detection']['spikes_summary'].empty):
                csv_data = results['anomaly_detection']['spikes_summary'].to_csv(index=False)
                st.download_button(
                    label="📊 Download Spike Detection CSV",
                    data=csv_data,
                    file_name=f"spike_detection_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            elif ('anomalies_summary' in results['anomaly_detection'] and 
                  not results['anomaly_detection']['anomalies_summary'].empty):
                csv_data = results['anomaly_detection']['anomalies_summary'].to_csv(index=False)
                st.download_button(
                    label="📊 Download Anomaly Detection CSV", 
                    data=csv_data,
                    file_name=f"anomaly_detection_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            else:
                st.info("No anomaly data available for download")
        else:
            st.info("Anomaly detection data not available")


if __name__ == '__main__':
    main()