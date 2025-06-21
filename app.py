import os
import sqlite3
import requests
import json
from datetime import datetime, timedelta
import time
import logging
from functools import wraps
from flask import Flask, render_template, jsonify, request
from urllib.parse import quote, unquote
import pandas as pd
import threading
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
import numpy as np
import warnings


# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY')

# FRED API Configuration
FRED_API_KEY = os.environ.get('FRED_API_KEY')
FRED_BASE_URL = "https://api.stlouisfed.org/fred"

if not FRED_API_KEY:
    logger.error("FRED_API_KEY not found in environment variables!")
    logger.error("Please create a .env file with: FRED_API_KEY=your_key_here")

# Economic Indicators Configuration with proper FRED data scaling
INDICATORS = {
    "LABOR MARKET": {
        "UNRATE": {
            "name": "Unemployment Rate",
            "description": "The percentage of the labor force that is unemployed and actively seeking employment.",
            "unit": "percent",
            "format": "percentage"
        },
        "PAYEMS": {
            "name": "Nonfarm Payrolls",
            "description": "The total number of paid employees in the U.S. excluding farm workers, government employees, and employees of nonprofits.",
            "unit": "thousands",
            "format": "number"
        },
        "CIVPART": {
            "name": "Labor Force Participation Rate",
            "description": "The percentage of the working-age population that is either employed or actively looking for work.",
            "unit": "percent",
            "format": "percentage"
        },
        "ICSA": {
            "name": "Initial Jobless Claims",
            "description": "The number of first-time claims for unemployment insurance, indicating the pace of layoffs in the economy.",
            "unit": "number",
            "format": "number"
        },
        "JTSJOL": {
            "name": "Job Openings",
            "description": "The total number of job openings available, indicating labor demand and economic strength.",
            "unit": "thousands",
            "format": "number"
        },
        "CES0500000003": {
            "name": "Average Hourly Earnings",
            "description": "The average hourly wage for all employees on private nonfarm payrolls, indicating wage growth trends.",
            "unit": "dollars",
            "format": "currency"
        }
    },
    "INFLATION": {
        "CPIAUCSL": {
            "name": "Consumer Price Index (CPI)",
            "description": "Measures the average change in prices paid by consumers for goods and services over time.",
            "unit": "index",
            "format": "number"
        },
        "CPILFESL": {
            "name": "Core CPI",
            "description": "CPI excluding food and energy prices, providing a clearer view of underlying inflation trends.",
            "unit": "index",
            "format": "number"
        },
        "PCEPI": {
            "name": "Personal Consumption Expenditures Price Index (PCE)",
            "description": "The Federal Reserve's preferred inflation measure, tracking price changes in consumer spending.",
            "unit": "index",
            "format": "number"
        },
        "PCEPILFE": {
            "name": "Core PCE Price Index",
            "description": "PCE excluding food and energy, used by the Fed to guide monetary policy decisions.",
            "unit": "index",
            "format": "number"
        },
        "PPIACO": {
            "name": "Producer Price Index (PPI)",
            "description": "Measures the average change in selling prices received by producers for their output.",
            "unit": "index",
            "format": "number"
        }
    },
    "OUTPUT & GROWTH": {
        "GDPC1": {
            "name": "Real Gross Domestic Product (GDP)",
            "description": "The total value of all goods and services produced, adjusted for inflation.",
            "unit": "billions",
            "format": "currency"
        },
        "INDPRO": {
            "name": "Industrial Production Index",
            "description": "Measures the real output of manufacturing, mining, and electric and gas utilities sectors.",
            "unit": "index",
            "format": "number"
        },
        "TCU": {
            "name": "Capacity Utilization",
            "description": "The percentage of total industrial capacity currently being utilized in production.",
            "unit": "percent",
            "format": "percentage"
        },
        "TSIFRGHT": {
            "name": "Freight Transportation Services Index",
            "description": "Measures the volume of freight movement across the U.S. and signals trends in industrial activity and supply chain demand.",
            "unit": "index",
            "format": "number"
        },
        "RSAFS": {
            "name": "Retail Sales",
            "description": "The total receipts of retail stores, indicating consumer spending strength.",
            "unit": "millions",
            "format": "currency"
        },
        "TOTALSA": {
            "name": "Auto Sales",
            "description": "Total vehicle sales in the U.S., a key indicator of consumer confidence and economic health.",
            "unit": "millions",
            "format": "number"
        }
    },
    "HOUSING & CONSTRUCTION": {
        "HOUST": {
            "name": "Housing Starts",
            "description": "The number of new residential construction projects begun, indicating housing market strength.",
            "unit": "thousands",
            "format": "number"
        },
        "PERMIT": {
            "name": "Building Permits",
            "description": "Permits issued for new construction, a leading indicator of housing activity.",
            "unit": "thousands",
            "format": "number"
        },
        "HSN1F": {
            "name": "New Home Sales",
            "description": "The number of newly built homes sold, reflecting housing demand and economic health.",
            "unit": "thousands",
            "format": "number"
        },
        "EXHOSLUSM495S": {
            "name": "Existing Home Sales",
            "description": "The number of previously owned homes sold, indicating overall housing market activity.",
            "unit": "number",
            "format": "number"
        },
        "CSUSHPINSA": {
            "name": "Case-Shiller Home Price Index",
            "description": "Tracks changes in home prices across major U.S. metropolitan areas.",
            "unit": "index",
            "format": "number"
        }
    },
    "MONETARY POLICY & BANKING": {
        "FEDFUNDS": {
            "name": "Effective Federal Funds Rate",
            "description": "The interest rate at which banks lend to each other overnight, set by the Federal Reserve.",
            "unit": "percent",
            "format": "percentage"
        },
        "GS10": {
            "name": "10-Year Treasury Yield",
            "description": "The yield on 10-year U.S. Treasury bonds, a benchmark for long-term interest rates.",
            "unit": "percent",
            "format": "percentage"
        },
        "MPRIME": {
            "name": "Bank Prime Loan Rate",
            "description": "The interest rate banks charge their most creditworthy customers.",
            "unit": "percent",
            "format": "percentage"
        },
        "M2SL": {
            "name": "M2 Money Supply",
            "description": "The total amount of money in circulation including cash, checking deposits, and savings.",
            "unit": "billions",
            "format": "currency"
        },
        "WALCL": {
            "name": "Total Assets of the Federal Reserve",
            "description": "The total assets on the Federal Reserve's balance sheet, indicating monetary policy stance.",
            "unit": "millions",
            "format": "currency"
        },
        "GFDEGDQ188S": {
            "name": "Total Public Debt As % Of GDP",
            "description": "The total federal debt as a percentage of total GDP, assessing the country's financial sustainability.",
            "unit": "percent",
            "format": "percentage"
        }
    },
    "CREDIT & LENDING": {
        "TOTALSL": {
            "name": "Total Consumer Credit Outstanding",
            "description": "The total amount of consumer credit outstanding, including credit cards and auto loans.",
            "unit": "millions",
            "format": "currency"
        },
        "DRCLACBS": {
            "name": "Delinquency Rate on Consumer Loans",
            "description": "The percentage of consumer loans that are delinquent, indicating credit quality.",
            "unit": "percent",
            "format": "percentage"
        }
    },
    "TRADE & EXTERNAL SECTOR": {
        "NETEXP": {
            "name": "Net Exports (Trade Balance)",
            "description": "The difference between exports and imports, indicating trade competitiveness.",
            "unit": "billions",
            "format": "currency"
        },
        "IEABC": {
            "name": "Current Account Balance",
            "description": "The balance of trade, net income, and net current transfers with the rest of the world.",
            "unit": "millions",
            "format": "currency"
        }
    },
    "CORPORATE & INVESTMENT": {
        "CPATAX": {
            "name": "Corporate Profits After Tax",
            "description": "Total after-tax profits of U.S. corporations, indicating business profitability.",
            "unit": "billions",
            "format": "currency"
        },
        "ISRATIO": {
            "name": "Inventory-to-Sales Ratio",
            "description": "The ratio of business inventories to sales, indicating supply chain efficiency and demand.",
            "unit": "ratio",
            "format": "number"
        },
        "GPDI": {
            "name": "Gross Private Domestic Investment",
            "description": "Business investment in equipment, structures, and inventories, indicating economic confidence.",
            "unit": "billions",
            "format": "currency"
        },
        "BOGZ1FA105050005Q": {
            "name": "Nonfinancial Corporate Business CapEx",
            "description": "Reflects investment by nonfinancial corporations in long-term assets like property, plant, equipment, and software, indicating future productivity.",
            "unit": "millions",
            "format": "currency"
        }
    },
    "SENTIMENT & LEADING INDICATORS": {
        "UMCSENT": {
            "name": "University of Michigan Consumer Sentiment",
            "description": "A measure of consumer confidence and expectations about the economy.",
            "unit": "index",
            "format": "number"
        },
        "ATLSBUSRGEP": {
            "name": "Business Expectations of Sales Revenue Growth",
            "description": "Survey-based indicator measuring firms’ projections for their own sales revenue growth over the next 12 months.",
            "unit": "percent",
            "format": "percentage"
        },
        "T10Y3M": {
            "name": "Treasury Yield Curve Spread (10Y - 3M)",
            "description": "The spread between 10-year and 3-month Treasury yields, often used to predict recessions.",
            "unit": "percent",
            "format": "percentage"
        },
        "BAMLC0A1CAAA": {
            "name": "ICE BofA AAA Corporate Index Spread (AAA - 10Y) ",
            "description": "The spread between AAA corporate bond and 10-year Treasury yields, often used as an indicator for investor outlook on credit risk.",
            "unit": "percent",
            "format": "percentage"
        }
    }
}

# Database setup
def init_db():
    """Initialize the SQLite database"""
    conn = sqlite3.connect('economic_data.db')
    cursor = conn.cursor()
    
    # Create tables
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS indicator_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            series_id TEXT NOT NULL,
            date TEXT NOT NULL,
            value REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(series_id, date)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS api_metadata (
            series_id TEXT PRIMARY KEY,
            last_updated TIMESTAMP,
            frequency TEXT,
            units TEXT,
            title TEXT
        )
    ''')

    # Recent updates cache table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS recent_updates_cache (
            id INTEGER PRIMARY KEY,
            cache_data TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()

# Rate limiting decorator
def rate_limit(calls=120, period=60):
    def decorator(func):
        calls_made = []
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            now = time.time()
            calls_made[:] = [call_time for call_time in calls_made if now - call_time < period]
            
            if len(calls_made) >= calls:
                sleep_time = period - (now - calls_made[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    calls_made[:] = []
            
            calls_made.append(now)
            return func(*args, **kwargs)
        return wrapper
    return decorator

@rate_limit(calls=120, period=60)
def fetch_fred_data(series_id, start_date=None):
    """Fetch data from FRED API with rate limiting"""
    try:
        # First, get series metadata
        meta_params = {
            'series_id': series_id,
            'api_key': FRED_API_KEY,
            'file_type': 'json'
        }
        
        logger.info(f"Fetching metadata for {series_id}")
        meta_url = f"{FRED_BASE_URL}/series"
        meta_response = requests.get(meta_url, params=meta_params, timeout=10)
        
        if meta_response.status_code != 200:
            logger.error(f"Metadata request failed for {series_id}: {meta_response.status_code}")
            return {'success': False, 'error': f"HTTP {meta_response.status_code}: Failed to get series metadata"}
        
        meta_data = meta_response.json()
        if 'error_code' in meta_data:
            logger.error(f"FRED API error in metadata for {series_id}: {meta_data.get('error_message', 'Unknown error')}")
            return {'success': False, 'error': meta_data.get('error_message', 'FRED API error')}
        
        metadata = meta_data.get('seriess', [{}])[0]
        
        # Now get observations with minimal parameters
        obs_params = {
            'series_id': series_id,
            'api_key': FRED_API_KEY,
            'file_type': 'json'
        }
        
        # Only add start_date if provided and it's valid
        if start_date:
            obs_params['observation_start'] = start_date
        
        logger.info(f"Fetching observations for {series_id}")
        obs_url = f"{FRED_BASE_URL}/series/observations"
        response = requests.get(obs_url, params=obs_params, timeout=15)
        
        if response.status_code != 200:
            logger.error(f"Observations request failed for {series_id}: {response.status_code}")
            return {'success': False, 'error': f"HTTP {response.status_code}: Failed to get observations"}
        
        data = response.json()
        
        # Check for API errors in response
        if 'error_code' in data:
            logger.error(f"FRED API error for {series_id}: {data.get('error_message', 'Unknown error')}")
            return {'success': False, 'error': data.get('error_message', 'FRED API error')}
        
        observations = data.get('observations', [])
        logger.info(f"Successfully fetched {len(observations)} observations for {series_id}")
        
        return {
            'observations': observations,
            'metadata': metadata,
            'success': True
        }
        
    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching data for {series_id}")
        return {'success': False, 'error': 'Request timeout - FRED API may be slow'}
    except requests.exceptions.ConnectionError:
        logger.error(f"Connection error fetching data for {series_id}")
        return {'success': False, 'error': 'Connection error - check internet connection'}
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error fetching data for {series_id}: {e}")
        return {'success': False, 'error': f'Request error: {str(e)}'}
    except Exception as e:
        logger.error(f"Unexpected error for {series_id}: {e}")
        return {'success': False, 'error': f'Unexpected error: {str(e)}'}

def update_indicator_data(series_id, force_update=False):
    """Update data for a specific indicator"""
    conn = sqlite3.connect('economic_data.db')
    cursor = conn.cursor()
    
    try:
        # Check last update time
        cursor.execute('SELECT last_updated FROM api_metadata WHERE series_id = ?', (series_id,))
        result = cursor.fetchone()
        
        if not force_update and result:
            last_updated = datetime.fromisoformat(result[0])
            if datetime.now() - last_updated < timedelta(hours=12):  # Update twice daily
                logger.info(f"Skipping {series_id} - recently updated")
                return True
        
        # Fetch new data
        start_date = None
        if result and not force_update:
            # Only fetch data from last update minus a week for safety
            start_date = (datetime.fromisoformat(result[0]) - timedelta(days=7)).strftime('%Y-%m-%d')
        
        logger.info(f"Updating data for {series_id}")
        fred_data = fetch_fred_data(series_id, start_date)
        
        if not fred_data['success']:
            logger.error(f"Failed to fetch data for {series_id}: {fred_data.get('error', 'Unknown error')}")
            return False
        
        # Store observations
        observations_stored = 0
        for obs in fred_data['observations']:
            if obs['value'] != '.':  # FRED uses '.' for missing values
                try:
                    value = float(obs['value'])
                    cursor.execute('''
                        INSERT OR REPLACE INTO indicator_data (series_id, date, value)
                        VALUES (?, ?, ?)
                    ''', (series_id, obs['date'], value))
                    observations_stored += 1
                except (ValueError, TypeError) as e:
                    logger.warning(f"Skipping invalid value for {series_id} on {obs['date']}: {obs['value']}")
                    continue
        
        # Store metadata
        metadata = fred_data['metadata']
        cursor.execute('''
            INSERT OR REPLACE INTO api_metadata 
            (series_id, last_updated, frequency, units, title)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            series_id,
            datetime.now().isoformat(),
            metadata.get('frequency', ''),
            metadata.get('units', ''),
            metadata.get('title', '')
        ))
        
        conn.commit()
        logger.info(f"Successfully stored {observations_stored} observations for {series_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error updating {series_id}: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def get_indicator_data(series_id, days_back=None):
    """Get indicator data from database"""
    conn = sqlite3.connect('economic_data.db')
    cursor = conn.cursor()
    
    query = '''
        SELECT date, value FROM indicator_data 
        WHERE series_id = ? 
    '''
    params = [series_id]
    
    if days_back:
        cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        query += ' AND date >= ?'
        params.append(cutoff_date)
    
    query += ' ORDER BY date'
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    
    return [{'date': row[0], 'value': row[1]} for row in rows]

def calculate_period_change(data, format_type, period='1Y', series_id=None):
    """Calculate change over the selected period - fixed to use exact date lookup"""
    if len(data) < 2:
        return None
    
    latest_point = data[-1]
    current_date = latest_point['date']
    current_value = latest_point['value']
    
    # The key fix: instead of using data[0], look up the exact historical point
    if series_id:
        period_days = {
            '3M': 90,
            '6M': 180,
            '1Y': 365,
            '5Y': 1825,
            '10Y': 3650
        }
        
        if period in period_days:
            # Calculate target date (same logic as yoy_change)
            current_dt = datetime.strptime(current_date, '%Y-%m-%d')
            target_date = current_dt - timedelta(days=period_days[period])
            
            # Find closest historical point (same logic as yoy_change)
            conn = sqlite3.connect('economic_data.db')
            cursor = conn.cursor()
            
            # Look within ±15 day window for closest match
            window_start = (target_date - timedelta(days=15)).strftime('%Y-%m-%d')
            window_end = (target_date + timedelta(days=15)).strftime('%Y-%m-%d')
            
            cursor.execute('''
                SELECT date, value FROM indicator_data 
                WHERE series_id = ? AND date BETWEEN ? AND ?
                ORDER BY ABS(julianday(date) - julianday(?)) LIMIT 1
            ''', (series_id, window_start, window_end, target_date.strftime('%Y-%m-%d')))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                historical_value = result[1]
                
                if format_type == 'percentage':
                    # Percentage point change
                    return current_value - historical_value
                else:
                    # Percentage change
                    if historical_value == 0:
                        return None
                    return ((current_value - historical_value) / abs(historical_value)) * 100
    
    # Fallback to old method only for MAX period or missing series_id
    start_value = data[0]['value']
    end_value = current_value
    
    if format_type == 'percentage':
        return end_value - start_value
    else:
        if start_value == 0:
            return None
        return ((end_value - start_value) / abs(start_value)) * 100

def calculate_yoy_change(current_date, current_value, series_id, format_type):
    """Calculate year-over-year change - improved to handle missing series_id"""
    try:
        current_dt = datetime.strptime(current_date, '%Y-%m-%d')
        year_ago = current_dt.replace(year=current_dt.year - 1)
        
        # If no series_id provided (called from calculate_period_change), 
        # we need to handle this differently
        if series_id is None:
            return None
        
        # Find closest data point from a year ago (within 30 days window)
        conn = sqlite3.connect('economic_data.db')
        cursor = conn.cursor()
        
        # Look for data within 30 days of the target date
        window_start = (year_ago - timedelta(days=15)).strftime('%Y-%m-%d')
        window_end = (year_ago + timedelta(days=15)).strftime('%Y-%m-%d')
        
        cursor.execute('''
            SELECT date, value FROM indicator_data 
            WHERE series_id = ? AND date BETWEEN ? AND ?
            ORDER BY ABS(julianday(date) - julianday(?)) LIMIT 1
        ''', (series_id, window_start, window_end, year_ago.strftime('%Y-%m-%d')))
        
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            return None
        
        year_ago_value = result[1]
        
        if format_type == 'percentage':
            # Percentage point change
            return current_value - year_ago_value
        else:
            # Percentage change
            if year_ago_value == 0:
                return None
            return ((current_value - year_ago_value) / abs(year_ago_value)) * 100
            
    except Exception as e:
        logger.error(f"Error calculating YoY change: {e}")
        return None

def calculate_qoq_change(current_date, current_value, series_id, format_type):
    """Calculate quarter-over-quarter change (90 days back)"""
    try:
        current_dt = datetime.strptime(current_date, '%Y-%m-%d')
        quarter_ago = current_dt - timedelta(days=90)
        
        if series_id is None:
            return None
        
        # Find closest data point from 90 days ago (within 15 days window)
        conn = sqlite3.connect('economic_data.db')
        cursor = conn.cursor()
        
        window_start = (quarter_ago - timedelta(days=15)).strftime('%Y-%m-%d')
        window_end = (quarter_ago + timedelta(days=15)).strftime('%Y-%m-%d')
        
        cursor.execute('''
            SELECT date, value FROM indicator_data 
            WHERE series_id = ? AND date BETWEEN ? AND ?
            ORDER BY ABS(julianday(date) - julianday(?)) LIMIT 1
        ''', (series_id, window_start, window_end, quarter_ago.strftime('%Y-%m-%d')))
        
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            return None
        
        quarter_ago_value = result[1]
        
        if format_type == 'percentage':
            # Percentage point change
            return current_value - quarter_ago_value
        else:
            # Percentage change
            if quarter_ago_value == 0:
                return None
            return ((current_value - quarter_ago_value) / abs(quarter_ago_value)) * 100
            
    except Exception as e:
        logger.error(f"Error calculating Q/Q change: {e}")
        return None

def format_value(value, format_type, unit=None):
    """Format values according to their type, accounting for FRED's pre-scaled data"""
    if value is None:
        return "N/A"
    
    try:
        if format_type == 'currency':
            # Handle currency values - FRED data may be pre-scaled
            if unit == 'billions':
                # FRED data already in billions
                if abs(value) >= 1000:  # Trillions
                    return f"${value/1000:,.1f}T"
                else:
                    return f"${value:,.1f}B"
            elif unit == 'millions':
                # FRED data already in millions
                if abs(value) >= 1000000:  # Trillions
                    return f"${value/1000000:,.1f}T"
                elif abs(value) >= 1000:  # Billions
                    return f"${value/1000:,.1f}B"
                else:
                    return f"${value:,.1f}M"
            else:
                # Auto-scale currency
                if abs(value) >= 1000000000000:  # Trillions
                    return f"${value/1000000000000:,.1f}T"
                elif abs(value) >= 1000000000:  # Billions
                    return f"${value/1000000000:,.1f}B"
                elif abs(value) >= 1000000:  # Millions
                    return f"${value/1000000:,.1f}M"
                elif abs(value) >= 1000:  # Thousands
                    return f"${value/1000:,.1f}K"
                else:
                    return f"${value:,.1f}"
        
        elif format_type == 'percentage':
            return f"{value:.1f}%"
        
        elif format_type == 'number':
            # Handle FRED's pre-scaled numeric data
            if unit == 'thousands':
                # FRED data like 159000 means 159,000 thousands = 159 million
                if abs(value) >= 1000000:  # Billions
                    return f"{value/1000000:,.1f}B"
                elif abs(value) >= 1000:  # Millions
                    return f"{value/1000:,.1f}M"
                else:
                    return f"{value:,.1f}K"
            elif unit == 'millions':
                # FRED data already in millions
                if abs(value) >= 1000000:  # Trillions
                    return f"{value/1000000:,.1f}T"
                elif abs(value) >= 1000:  # Billions
                    return f"{value/1000:,.1f}B"
                else:
                    return f"{value:,.1f}M"
            elif unit == 'billions':
                # FRED data already in billions
                if abs(value) >= 1000:  # Trillions
                    return f"{value/1000:,.1f}T"
                else:
                    return f"{value:,.1f}B"
            else:
                # No unit specified, use auto-scaling
                abs_value = abs(value)
                if abs_value >= 1000000000000:  # Trillions
                    return f"{value/1000000000000:,.1f}T"
                elif abs_value >= 1000000000:  # Billions
                    return f"{value/1000000000:,.1f}B"
                elif abs_value >= 1000000:  # Millions
                    return f"{value/1000000:,.1f}M"
                elif abs_value >= 1000:  # Thousands
                    return f"{value/1000:,.1f}K"
                else:
                    return f"{value:,.1f}"
        
        else:
            # Default handling
            return f"{value:,.1f}"
            
    except Exception as e:
        logger.error(f"Error formatting value {value}: {e}")
        return str(value)

def format_change(change, format_type):
    """Format change values"""
    if change is None:
        return "N/A"
    
    try:
        if format_type == 'percentage':
            # Percentage points
            sign = "+" if change > 0 else ""
            return f"{sign}{change:.2f}pp"
        else:
            # Percentage change
            sign = "+" if change > 0 else ""
            return f"{sign}{change:.2f}%"
    except:
        return str(change)

def analyze_section_health(section_name, indicators):
    """Analyze the health of a section based on its indicators"""
    try:
        health_scores = []
        section_data = {}
        
        for series_id, config in indicators.items():
            # Get recent data
            recent_data = get_indicator_data(series_id, days_back=1095)  # 3 years
            if len(recent_data) < 12:  # Need at least a year of data
                continue
            
            current_value = recent_data[-1]['value']
            current_date = recent_data[-1]['date']
            
            # Calculate various metrics
            yoy_change = calculate_yoy_change(current_date, current_value, series_id, config['format'])
            
            # Calculate averages
            three_year_values = [d['value'] for d in recent_data]
            ten_year_data = get_indicator_data(series_id, days_back=3650)
            ten_year_values = [d['value'] for d in ten_year_data] if ten_year_data else three_year_values
            
            three_year_avg = mean(three_year_values) if three_year_values else current_value
            ten_year_avg = mean(ten_year_values) if ten_year_values else current_value
            
            # Store data for analysis
            section_data[series_id] = {
                'config': config,
                'current_value': current_value,
                'yoy_change': yoy_change,
                'three_year_avg': three_year_avg,
                'ten_year_avg': ten_year_avg,
                'vs_3yr_avg': ((current_value - three_year_avg) / three_year_avg * 100) if three_year_avg != 0 else 0,
                'vs_10yr_avg': ((current_value - ten_year_avg) / ten_year_avg * 100) if ten_year_avg != 0 else 0
            }
            
            # Score individual indicator
            score = analyze_indicator_health(series_id, current_value, yoy_change, three_year_avg, ten_year_avg, config)
            health_scores.append(score)
        
        if not health_scores:
            return "Insufficient data available for comprehensive analysis of this economic section.", "moderate"
        
        # Overall section health
        avg_score = mean(health_scores)
        
        if avg_score >= 0.7:
            health_status = "healthy"
        elif avg_score >= 0.4:
            health_status = "moderate"  
        else:
            health_status = "unhealthy"
        
        # Generate comprehensive analysis
        analysis = generate_section_analysis(section_name, section_data, health_status, avg_score)
        
        return analysis, health_status
        
    except Exception as e:
        logger.error(f"Error analyzing section {section_name}: {e}")
        return f"Analysis temporarily unavailable for {section_name} due to data processing issues. Please try refreshing the page.", "moderate"

def generate_section_analysis(section_name, section_data, health_status, avg_score):
    """Generate detailed paragraph analysis for a section"""
    
    if not section_data:
        return f"The {section_name} section currently lacks sufficient data for comprehensive analysis."
    
    # Start with overall assessment
    if health_status == "healthy":
        intro = f"The {section_name} data demonstrates strong economic performance with positive underlying trends."
    elif health_status == "moderate":
        intro = f"{section_name} data shows mixed signals with both encouraging and concerning developments."
    else:
        intro = f"{section_name} data points to significant challenges with multiple indicators showing weakness."
    
    # Analyze key indicators
    key_insights = []
    concerning_trends = []
    positive_trends = []
    
    for series_id, data in section_data.items():
        config = data['config']
        name = config['name']
        
        # Year-over-year analysis
        if data['yoy_change'] is not None:
            if config['format'] == 'percentage':
                if abs(data['yoy_change']) > 0.5:  # Significant change for rates
                    if data['yoy_change'] > 0:
                        if series_id == 'UNRATE':  # Unemployment increase is bad
                            concerning_trends.append(f"{name} increased by {abs(data['yoy_change']):.1f} percentage points year-over-year")
                        else:
                            positive_trends.append(f"{name} rose {data['yoy_change']:.1f} percentage points year-over-year")
                    else:
                        if series_id == 'UNRATE':  # Unemployment decrease is good
                            positive_trends.append(f"{name} declined by {abs(data['yoy_change']):.1f} percentage points year-over-year")
                        else:
                            concerning_trends.append(f"{name} fell {abs(data['yoy_change']):.1f} percentage points year-over-year")
            else:
                if abs(data['yoy_change']) > 5:  # Significant percentage change
                    if data['yoy_change'] > 0:
                        positive_trends.append(f"{name} grew {data['yoy_change']:.1f}% year-over-year")
                    else:
                        concerning_trends.append(f"{name} declined {abs(data['yoy_change']):.1f}% year-over-year")
        
        # Compare to historical averages
        if abs(data['vs_3yr_avg']) > 10:  # 10% deviation from 3-year average
            if data['vs_3yr_avg'] > 0:
                if series_id == 'UNRATE' or 'DELINQ' in series_id or 'CLAIM' in series_id:
                    concerning_trends.append(f"{name} is {abs(data['vs_3yr_avg']):.0f}% above its 3-year average")
                else:
                    key_insights.append(f"{name} is running {data['vs_3yr_avg']:.0f}% above its 3-year average")
            else:
                if series_id == 'UNRATE' or 'DELINQ' in series_id or 'CLAIM' in series_id:
                    positive_trends.append(f"{name} is {abs(data['vs_3yr_avg']):.0f}% below its 3-year average")
                else:
                    concerning_trends.append(f"{name} is {abs(data['vs_3yr_avg']):.0f}% below its 3-year average")
    
    # Build comprehensive paragraph
    analysis_parts = [intro]
    
    # Add specific insights
    if positive_trends:
        if len(positive_trends) == 1:
            analysis_parts.append(f"Notably, {positive_trends[0]}.")
        else:
            analysis_parts.append(f"Positive developments include: {', '.join(positive_trends[:2])}.")
    
    if concerning_trends:
        if len(concerning_trends) == 1:
            analysis_parts.append(f"However, {concerning_trends[0]}, which warrants attention.")
        else:
            analysis_parts.append(f"Areas of concern include: {', '.join(concerning_trends[:2])}, suggesting caution is warranted.")
    
    if key_insights:
        analysis_parts.append(f"Additionally, {key_insights[0]}.")
    
    # Add contextual interpretation based on section type
    context = get_section_context(section_name, section_data, health_status)
    if context:
        analysis_parts.append(context)
    
    # Combine into flowing paragraph
    return " ".join(analysis_parts)

def get_section_context(section_name, section_data, health_status):
    """Add section-specific economic context"""
    
    if section_name == "LABOR MARKET":
        if health_status == "healthy":
            return "This robust labor market performance typically supports consumer spending and broader economic growth."
        elif health_status == "unhealthy":
            return "Weakness in labor markets often signals broader economic challenges and may impact consumer confidence."
        else:
            return "Mixed labor market signals suggest the economy is in a transitional phase requiring careful monitoring."
    
    elif section_name == "INFLATION":
        if health_status == "healthy":
            return "Stable inflation near target levels provides a supportive environment for monetary policy and economic planning."
        elif health_status == "unhealthy":
            return "Inflation pressures may constrain Federal Reserve policy flexibility and impact consumer purchasing power."
        else:
            return "Evolving inflation dynamics require careful monitoring as they influence monetary policy decisions."
    
    elif section_name == "OUTPUT & GROWTH":
        if health_status == "healthy":
            return "Strong output growth indicates healthy business investment and productivity gains across the economy."
        elif health_status == "unhealthy":
            return "Weakening output suggests potential economic slowdown and reduced business confidence."
        else:
            return "Mixed growth signals indicate an economy navigating various crosscurrents and uncertainties."
    
    elif section_name == "HOUSING & CONSTRUCTION":
        if health_status == "healthy":
            return "A robust housing market typically reflects healthy household formation and supports related industries."
        elif health_status == "unhealthy":
            return "Housing market weakness can have broad economic implications given its role in wealth creation and consumer spending."
        else:
            return "Housing market conditions are showing divergent trends that reflect changing demographics and financing conditions."
    
    elif section_name == "MONETARY POLICY & BANKING":
        if health_status == "healthy":
            return "Stable monetary conditions support predictable financing costs and encourage long-term economic planning."
        elif health_status == "unhealthy":
            return "Financial market stress can constrain credit availability and impact broader economic activity."
        else:
            return "Evolving monetary conditions reflect the Federal Reserve's ongoing efforts to balance growth and stability objectives."
    
    return ""

def analyze_indicator_health(series_id, current_value, yoy_change, three_year_avg, ten_year_avg, config):
    """Analyze individual indicator health (0-1 score)"""
    try:
        # Indicator-specific health logic
        if series_id == "UNRATE":  # Unemployment - lower is better
            if current_value <= 4.0:
                return 0.9
            elif current_value <= 6.0:
                return 0.6
            else:
                return 0.2
        
        elif series_id in ["PAYEMS", "JTSJOL"]:  # Employment - higher growth is better
            if yoy_change and yoy_change > 2:
                return 0.8
            elif yoy_change and yoy_change > 0:
                return 0.6
            else:
                return 0.3
        
        elif series_id in ["CPIAUCSL", "CPILFESL", "PCEPI", "PCEPILFE"]:  # Inflation - target ~2% YoY
            if yoy_change:
                if 1.5 <= yoy_change <= 2.5:
                    return 0.9
                elif 1.0 <= yoy_change <= 3.0:
                    return 0.7
                elif yoy_change < 0:  # Deflation
                    return 0.2
                else:
                    return 0.4
            return 0.5
        
        elif series_id == "GDPC1":  # GDP - positive growth is good
            if yoy_change and yoy_change > 3:
                return 0.9
            elif yoy_change and yoy_change > 1:
                return 0.7
            elif yoy_change and yoy_change > 0:
                return 0.5
            else:
                return 0.2
        
        # Default scoring based on trend vs historical average
        if current_value > ten_year_avg * 1.1:
            return 0.7
        elif current_value > ten_year_avg * 0.9:
            return 0.6
        else:
            return 0.4
            
    except Exception as e:
        logger.error(f"Error in indicator health analysis: {e}")
        return 0.5  # Neutral if analysis fails

def get_cached_recent_updates():
    """Get recent updates from cache if less than 12 hours old"""
    try:
        conn = sqlite3.connect('economic_data.db')
        cursor = conn.cursor()
        
        # Get the most recent cache entry
        cursor.execute('''
            SELECT cache_data, created_at FROM recent_updates_cache 
            ORDER BY created_at DESC LIMIT 1
        ''')
        
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            return None
        
        cache_data, created_at_str = result
        created_at = datetime.fromisoformat(created_at_str)
        
        # Check if cache is less than 12 hours old
        if datetime.now() - created_at < timedelta(hours=12):
            logger.info("Using cached recent updates data")
            return json.loads(cache_data)
        else:
            logger.info("Cache expired, need fresh data")
            return None
            
    except Exception as e:
        logger.error(f"Error getting cached recent updates: {e}")
        return None

def save_recent_updates_cache(updates_data):
    """Save recent updates to cache"""
    try:
        conn = sqlite3.connect('economic_data.db')
        cursor = conn.cursor()
        
        # Clear old cache entries (keep only last 5 for cleanup)
        cursor.execute('''
            DELETE FROM recent_updates_cache 
            WHERE id NOT IN (
                SELECT id FROM recent_updates_cache 
                ORDER BY created_at DESC LIMIT 5
            )
        ''')
        
        # Insert new cache data
        cache_json = json.dumps(updates_data)
        cursor.execute('''
            INSERT INTO recent_updates_cache (cache_data, created_at)
            VALUES (?, ?)
        ''', (cache_json, datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
        
        logger.info("Saved recent updates to cache")
        
    except Exception as e:
        logger.error(f"Error saving recent updates cache: {e}")

def get_recent_updates_with_cache(max_indicators=10):
    """Get recent updates from cache or FRED API if cache is stale"""
    
    # Try to get from cache first
    cached_data = get_cached_recent_updates()
    if cached_data:
        logger.info("Returning cached recent updates data")
        return cached_data
    
    # Cache miss or expired - fetch from FRED API
    logger.info("Cache miss/expired - fetching fresh data from FRED API")
    
    try:
        fresh_updates = get_recent_updates_from_fred(max_indicators)
        
        if fresh_updates:
            # Save to cache
            save_recent_updates_cache(fresh_updates)
            logger.info(f"Fetched and cached {len(fresh_updates)} fresh updates from FRED")
        else:
            logger.warning("No fresh updates returned from FRED API")
        
        return fresh_updates
        
    except Exception as e:
        logger.error(f"Error fetching fresh updates from FRED: {e}")
        
        # Fallback: try to get any cached data, even if expired
        try:
            conn = sqlite3.connect('economic_data.db')
            cursor = conn.cursor()
            cursor.execute('''
                SELECT cache_data FROM recent_updates_cache 
                ORDER BY created_at DESC LIMIT 1
            ''')
            result = cursor.fetchone()
            conn.close()
            
            if result:
                logger.info("Using expired cache as fallback")
                return json.loads(result[0])
                
        except Exception as fallback_error:
            logger.error(f"Fallback cache read failed: {fallback_error}")
        
        return []

def get_recent_updates_from_fred(max_indicators=10):
    """Get recent indicator updates directly from FRED API, sorted by recency"""
    try:
        all_updates = []
        
        # Priority indicators list
        priority_indicators = ['UNRATE', 'PAYEMS', 'CPIAUCSL', 'CPILFESL', 'GDPC1', 'FEDFUNDS', 'GS10', 'HOUST', 'RSAFS', 'INDPRO']
        
        # Check ALL indicators first and collect their recency
        for section_indicators in INDICATORS.values():
            for series_id, config in section_indicators.items():
                try:
                    # Get fresh data from FRED API
                    fred_data = fetch_fred_data(series_id)
                    
                    if not fred_data['success'] or not fred_data['observations']:
                        continue
                    
                    # Filter valid observations
                    valid_obs = [obs for obs in fred_data['observations'] 
                               if obs['value'] != '.' and obs['value'] is not None]
                    
                    if len(valid_obs) < 2:
                        continue
                    
                    # Get latest data point
                    latest_obs = valid_obs[-1]
                    current_value = float(latest_obs['value'])
                    current_date = latest_obs['date']
                    
                    # Calculate time since last data point
                    current_dt = datetime.strptime(current_date, '%Y-%m-%d')
                    days_since_data = (datetime.now() - current_dt).days
                    
                    # Only include if data is reasonably recent (within 90 days)
                    if days_since_data > 90:
                        continue
                    
                    # Calculate YoY change using FRED data
                    yoy_change = None
                    target_date = current_dt.replace(year=current_dt.year - 1)
                    year_ago_obs = None
                    min_diff = float('inf')
                    
                    for obs in valid_obs:
                        obs_date = datetime.strptime(obs['date'], '%Y-%m-%d')
                        diff = abs((obs_date - target_date).days)
                        if diff < min_diff and diff <= 30:  # Within 30 days
                            min_diff = diff
                            year_ago_obs = obs
                    
                    if year_ago_obs:
                        year_ago_value = float(year_ago_obs['value'])
                        if config['format'] == 'percentage':
                            yoy_change = current_value - year_ago_value  # Percentage points
                        else:
                            if year_ago_value != 0:
                                yoy_change = ((current_value - year_ago_value) / abs(year_ago_value)) * 100
                    
                    # Calculate QoQ change (90 days back)
                    qoq_change = None
                    quarter_ago_date = current_dt - timedelta(days=90)
                    quarter_ago_obs = None
                    min_diff = float('inf')
                    
                    for obs in valid_obs:
                        obs_date = datetime.strptime(obs['date'], '%Y-%m-%d')
                        diff = abs((obs_date - quarter_ago_date).days)
                        if diff < min_diff and diff <= 15:  # Within 15 days
                            min_diff = diff
                            quarter_ago_obs = obs
                    
                    if quarter_ago_obs:
                        quarter_ago_value = float(quarter_ago_obs['value'])
                        if config['format'] == 'percentage':
                            qoq_change = current_value - quarter_ago_value  # Percentage points
                        else:
                            if quarter_ago_value != 0:
                                qoq_change = ((current_value - quarter_ago_value) / abs(quarter_ago_value)) * 100
                    
                    # Add to all_updates with recency info
                    all_updates.append({
                        'series_id': series_id,
                        'name': config['name'],
                        'latest_value': format_value(current_value, config['format'], config['unit']),
                        'yoy_change': format_change(yoy_change, config['format']),
                        'qoq_change': format_change(qoq_change, config['format']),
                        'yoy_change_raw': yoy_change,
                        'qoq_change_raw': qoq_change,
                        'days_ago': days_since_data,
                        'last_updated_date': current_date,
                        'data_date': current_date,
                        'config': config,
                        'is_priority': series_id in priority_indicators
                    })
                    
                    time.sleep(0.1)  # Rate limiting
                    
                except Exception as e:
                    logger.error(f"Error processing {series_id}: {e}")
                    continue
        
        # Sort by recency FIRST, then by priority
        all_updates.sort(key=lambda x: (x['days_ago'], not x['is_priority']))
        
        # Return the most recent ones up to max_indicators
        return all_updates[:max_indicators]
        
    except Exception as e:
        logger.error(f"Error in get_recent_updates_from_fred: {e}")
        return []

def analyze_recent_economic_trends_from_fred(updates):
    """Generate economic analysis from FRED-sourced updates"""
    if not updates:
        return "Unable to retrieve current economic data from Federal Reserve sources for analysis."
    
    # Count recent vs older data
    very_recent = sum(1 for u in updates if u['days_ago'] <= 7)
    recent = sum(1 for u in updates if u['days_ago'] <= 30)
    
    # Analyze trends from the fresh data
    positive_signals = []
    negative_signals = []
    mixed_signals = []
    
    key_indicators = {
        'UNRATE': 'unemployment',
        'GDPC1': 'economic growth', 
        'CPIAUCSL': 'inflation',
        'PAYEMS': 'employment',
        'FEDFUNDS': 'interest rates'
    }
    
    for update in updates:
        yoy = update['yoy_change_raw']
        series_id = update['series_id']
        name = key_indicators.get(series_id, update['name'].lower())
        
        if yoy is None:
            continue
            
        # Special handling for unemployment (lower is better)
        if series_id == 'UNRATE':
            if yoy < -0.3:
                positive_signals.append(f"declining {name}")
            elif yoy > 0.3:
                negative_signals.append(f"rising {name}")
            continue
        
        # General indicators
        if yoy > 2:
            positive_signals.append(f"strong {name} growth")
        elif yoy < -2:
            negative_signals.append(f"weakening {name}")
        elif abs(yoy) > 0.5:
            mixed_signals.append(f"moderate {name} changes")
    
    # Generate analysis
    sentences = []
    
    # Opening sentence about data freshness
    if very_recent >= 3:
        sentences.append(f"Current Federal Reserve data shows {len(updates)} key economic indicators have been updated, with {very_recent} showing data from the past week, providing a fresh view of economic conditions.")
    else:
        sentences.append(f"Analysis of {len(updates)} current economic indicators from Federal Reserve data reveals the latest trends across major economic sectors.")
    
    # Overall trend assessment
    if len(positive_signals) > len(negative_signals):
        sentences.append("The latest data suggests economic resilience with more indicators showing positive momentum than negative trends.")
    elif len(negative_signals) > len(positive_signals):
        sentences.append("Recent indicators point to emerging economic headwinds with several key metrics showing concerning developments.")
    else:
        sentences.append("Economic indicators present a mixed picture with positive developments balanced by areas of concern.")
    
    # Specific highlights
    if positive_signals:
        sentences.append(f"Encouraging developments include {', '.join(positive_signals[:2])}, indicating underlying economic strength.")
    elif negative_signals:
        sentences.append(f"Areas of concern include {', '.join(negative_signals[:2])}, suggesting potential economic challenges ahead.")
    else:
        sentences.append("Economic indicators are showing moderate changes across sectors without clear directional momentum.")
    
    # Data quality and recency
    if recent >= len(updates) * 0.7:
        sentences.append("The high proportion of recently updated data provides confidence in the current economic assessment and near-term outlook.")
    else:
        sentences.append("While some indicators reflect recent conditions, a fuller economic picture will emerge as additional data becomes available.")
    
    # Forward-looking statement
    priority_count = sum(1 for u in updates if u['series_id'] in ['UNRATE', 'GDPC1', 'CPIAUCSL', 'PAYEMS', 'FEDFUNDS'])
    if priority_count >= 3:
        sentences.append("With updates spanning employment, growth, and inflation metrics, the data provides a solid foundation for understanding current economic trajectory and policy implications.")
    else:
        sentences.append("Continued monitoring of core economic indicators will be essential for assessing the durability and direction of current trends.")
    
    return " ".join(sentences)

# Background data update
def update_all_indicators():
    """Update all indicators in background"""
    logger.info("Starting background data update...")
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for section_indicators in INDICATORS.values():
            for series_id in section_indicators.keys():
                future = executor.submit(update_indicator_data, series_id)
                futures.append(future)
        
        # Wait for all updates to complete
        for future in futures:
            try:
                future.result(timeout=30)
            except Exception as e:
                logger.error(f"Error in background update: {e}")
    
    logger.info("Background data update completed")

# Routes
@app.route('/')
def index():
    return render_template('index.html', indicators=INDICATORS)

@app.route('/routes')
def show_routes():
    """Debug route to show all registered routes"""
    routes = []
    for rule in app.url_map.iter_rules():
        routes.append({
            'endpoint': rule.endpoint,
            'methods': list(rule.methods),
            'rule': str(rule)
        })
    return jsonify(routes)

@app.route('/debug')
def debug_info():
    """Debug endpoint to check system status"""
    try:
        # Check database
        conn = sqlite3.connect('economic_data.db')
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM indicator_data')
        data_count = cursor.fetchone()[0]
        cursor.execute('SELECT COUNT(DISTINCT series_id) FROM indicator_data')
        series_count = cursor.fetchone()[0]
        conn.close()
        
        # Check API key
        api_key_status = "Set" if FRED_API_KEY else "Missing"
        
        # Sample data check
        sample_data = get_indicator_data('UNRATE', days_back=30)
        
        debug_info = {
            'api_key': api_key_status,
            'database_records': data_count,
            'series_with_data': series_count,
            'sample_data_points': len(sample_data),
            'sample_latest': sample_data[-1] if sample_data else None,
            'indicators_configured': sum(len(section.keys()) for section in INDICATORS.values()),
            'sections': list(INDICATORS.keys())
        }
        
        return f"""
        <html>
        <head><title>Debug Info</title></head>
        <body style="font-family: Arial; margin: 40px; background: #1a1a1e; color: #fff;">
            <h1>Dashboard Debug Information</h1>
            <h2>System Status</h2>
            <ul>
                <li><strong>API Key:</strong> {debug_info['api_key']}</li>
                <li><strong>Database Records:</strong> {debug_info['database_records']}</li>
                <li><strong>Series with Data:</strong> {debug_info['series_with_data']}</li>
                <li><strong>Configured Indicators:</strong> {debug_info['indicators_configured']}</li>
            </ul>
            
            <h2>Sample Data (UNRATE - Last 30 days)</h2>
            <ul>
                <li><strong>Data Points:</strong> {debug_info['sample_data_points']}</li>
                <li><strong>Latest:</strong> {debug_info['sample_latest']}</li>
            </ul>
            
            <h2>Configured Sections</h2>
            <ul>
                {''.join(f'<li>{section}</li>' for section in debug_info['sections'])}
            </ul>
            
            <h2>Test Links</h2>
            <ul>
                <li><a href="/api/indicator/UNRATE?period=1Y" style="color: #8b5cf6;">Test UNRATE API</a></li>
                <li><a href="/api/section/LABOR%20MARKET" style="color: #8b5cf6;">Test Labor Market Analysis</a></li>
                <li><a href="/api/recent-updates" style="color: #10b981;">Test Recent Updates API</a></li>
                <li><a href="/routes" style="color: #10b981;">Show All Routes</a></li>
                <li><a href="/" style="color: #10b981;">Back to Dashboard</a></li>
            </ul>
        </body>
        </html>
        """
        
    except Exception as e:
        return f"""
        <html><body style="background: #1a1a1e; color: #fff; font-family: Arial; margin: 40px;">
        <h1>Debug Error</h1>
        <p>Error getting debug info: {str(e)}</p>
        <a href="/" style="color: #10b981;">Back to Dashboard</a>
        </body></html>
        """

@app.route('/api/indicator/<series_id>')
def get_indicator_api(series_id):
    """API endpoint for individual indicator data"""
    try:
        # Get time period from query params
        period = request.args.get('period', '1Y')
        
        # Convert period to days
        period_days = {
            '3M': 90,
            '6M': 180,
            'YTD': (datetime.now() - datetime(datetime.now().year, 1, 1)).days,
            '1Y': 365,
            '5Y': 1825,
            '10Y': 3650,
            'MAX': None
        }
        
        days_back = period_days.get(period)
        
        logger.info(f"API request for {series_id}, period: {period}, days_back: {days_back}")
        
        # Update data if needed
        update_indicator_data(series_id)
        
        # Get data
        data = get_indicator_data(series_id, days_back)
        
        if not data:
            return jsonify({'error': 'No data available'}), 404
        
        # Find indicator config
        indicator_config = None
        for section_indicators in INDICATORS.values():
            if series_id in section_indicators:
                indicator_config = section_indicators[series_id]
                break
        
        if not indicator_config:
            return jsonify({'error': 'Indicator not found'}), 404
        
        # Calculate metrics - FIX: Pass series_id and period to calculate_period_change
        latest_value = data[-1]['value']
        period_change = calculate_period_change(
            data, 
            indicator_config['format'], 
            period,      # ← Add period parameter
            series_id    # ← Add series_id parameter
        )
        
        # Format data for response
        formatted_data = []
        for point in data:
            yoy_change = calculate_yoy_change(
                point['date'], 
                point['value'], 
                series_id, 
                indicator_config['format']
            )
            
            formatted_data.append({
                'date': point['date'],
                'value': point['value'],
                'formatted_value': format_value(point['value'], indicator_config['format'], indicator_config['unit']),
                'yoy_change': yoy_change,
                'formatted_yoy_change': format_change(yoy_change, indicator_config['format'])
            })
        
        return jsonify({
            'data': formatted_data,
            'latest_value': format_value(latest_value, indicator_config['format'], indicator_config['unit']),
            'period_change': format_change(period_change, indicator_config['format']),
            'metadata': {
                'name': indicator_config['name'],
                'description': indicator_config['description'],
                'unit': indicator_config['unit'],
                'format': indicator_config['format']
            }
        })
        
    except Exception as e:
        logger.error(f"Error in indicator API for {series_id}: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/section/<path:section_name>')
def get_section_analysis(section_name):
    """API endpoint for section analysis"""
    try:
        # Handle URL encoding and clean up the section name
        section_name = unquote(section_name).strip()
        
        # Debug logging
        logger.info(f"Section analysis requested for: '{section_name}'")
        logger.info(f"Available sections: {list(INDICATORS.keys())}")
        
        if section_name not in INDICATORS:
            return jsonify({
                'error': f'Section not found: "{section_name}"', 
                'available_sections': list(INDICATORS.keys()),
                'requested_section': section_name
            }), 404
        
        analysis, health_status = analyze_section_health(section_name, INDICATORS[section_name])
        
        return jsonify({
            'analysis': analysis,
            'health_status': health_status,
            'section_name': section_name
        })
        
    except Exception as e:
        logger.error(f"Error in section analysis for '{section_name}': {e}")
        return jsonify({'error': f'Internal server error: {str(e)}', 'section_name': section_name}), 500

@app.route('/api/recent-updates')
def get_recent_updates_api():
    """API endpoint for recent economic data updates (cached)"""
    try:
        logger.info("API request for recent updates (cached)")
        
        # Get updates from cache or fresh from FRED
        updates = get_recent_updates_with_cache(max_indicators=10)
        
        # Generate economic analysis
        economic_analysis = analyze_recent_economic_trends_from_fred(updates)
        
        # Check if data is from cache
        cached_data = get_cached_recent_updates()
        is_cached = cached_data is not None
        
        return jsonify({
            'updates': updates,
            'economic_analysis': economic_analysis,
            'total_updates': len(updates),
            'data_source': 'Cache' if is_cached else 'FRED API',
            'is_cached': is_cached,
            'fetch_time': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error in recent updates API: {e}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/test/chart/<series_id>')
def test_chart_data(series_id):
    """Test endpoint for chart data debugging"""
    try:
        # Get data
        data = get_indicator_data(series_id, days_back=365)
        
        if not data:
            return jsonify({'error': 'No data found', 'series_id': series_id}), 404
        
        # Find indicator config
        indicator_config = None
        for section_indicators in INDICATORS.values():
            if series_id in section_indicators:
                indicator_config = section_indicators[series_id]
                break
        
        if not indicator_config:
            return jsonify({'error': 'Indicator not configured', 'series_id': series_id}), 404
        
        # Test data formatting
        latest_value = data[-1]['value']
        formatted_latest = format_value(latest_value, indicator_config['format'], indicator_config['unit'])
        
        # Test YoY calculation
        yoy_change = calculate_yoy_change(
            data[-1]['date'], 
            latest_value, 
            series_id, 
            indicator_config['format']
        )
        formatted_yoy = format_change(yoy_change, indicator_config['format'])
        
        return jsonify({
            'series_id': series_id,
            'config': indicator_config,
            'data_points': len(data),
            'date_range': {
                'start': data[0]['date'],
                'end': data[-1]['date']
            },
            'latest_value': latest_value,
            'formatted_latest': formatted_latest,
            'yoy_change': yoy_change,
            'formatted_yoy': formatted_yoy,
            'sample_points': data[-5:],  # Last 5 points
            'chart_ready': True
        })
        
    except Exception as e:
        logger.error(f"Error in test chart data for {series_id}: {e}")
        return jsonify({'error': str(e), 'series_id': series_id}), 500

# Initialize and run
if __name__ == '__main__':
    # Initialize database
    init_db()
    
    # Start background update thread only in development
    import os
    if os.environ.get('FLASK_ENV') != 'production':
        update_thread = threading.Thread(target=update_all_indicators, daemon=True)
        update_thread.start()
    
    # Run the app
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)