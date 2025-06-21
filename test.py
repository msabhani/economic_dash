#!/usr/bin/env python3
"""
Debug script to test FRED API connection
Run this to verify your API key and connection before running the full app
"""

import os
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

FRED_API_KEY = os.environ.get('FRED_API_KEY')
FRED_BASE_URL = "https://api.stlouisfed.org/fred"

def test_api_key():
    """Test if the API key is valid"""
    print("🔑 Testing FRED API Key...")
    
    if not FRED_API_KEY:
        print("❌ ERROR: FRED_API_KEY not found in environment variables")
        print("   Make sure you have a .env file with: FRED_API_KEY=your_key_here")
        return False
    
    print(f"   API Key found: {FRED_API_KEY[:8]}...")
    
    # Test with the correct FRED API endpoint
    params = {
        'api_key': FRED_API_KEY,
        'file_type': 'json'
    }
    
    try:
        # Test basic API access with category endpoint
        url = f"{FRED_BASE_URL}/category"
        print(f"   Testing URL: {url}")
        
        response = requests.get(url, params=params, timeout=10)
        print(f"   Response Status: {response.status_code}")
        print(f"   Full URL called: {response.url}")
        
        if response.status_code == 200:
            data = response.json()
            if 'categories' in data:
                print("✅ API Key is valid!")
                return True
            else:
                print(f"❌ Unexpected response format: {data}")
                return False
        elif response.status_code == 400:
            print("❌ Bad Request - Check your API key format")
            print(f"   Response: {response.text}")
            return False
        elif response.status_code == 403:
            print("❌ Forbidden - API key may be invalid or expired")
            return False
        elif response.status_code == 404:
            print("❌ Not Found - API endpoint may be wrong")
            print(f"   URL attempted: {response.url}")
            return False
        else:
            print(f"❌ HTTP {response.status_code}: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Network error: {e}")
        return False

def test_series_metadata():
    """Test fetching series metadata"""
    print("\n📋 Testing series metadata (UNRATE)...")
    
    params = {
        'series_id': 'UNRATE',
        'api_key': FRED_API_KEY,
        'file_type': 'json'
    }
    
    try:
        url = f"{FRED_BASE_URL}/series"
        print(f"   Testing URL: {url}")
        
        response = requests.get(url, params=params, timeout=10)
        print(f"   Response Status: {response.status_code}")
        print(f"   Full URL called: {response.url}")
        
        if response.status_code == 200:
            data = response.json()
            
            if 'error_code' in data:
                print(f"❌ FRED API Error: {data.get('error_message', 'Unknown error')}")
                return False
            
            series_info = data.get('seriess', [])
            if series_info:
                series = series_info[0]
                print("✅ Metadata retrieved successfully:")
                print(f"   Title: {series.get('title', 'N/A')}")
                print(f"   Frequency: {series.get('frequency', 'N/A')}")
                print(f"   Units: {series.get('units', 'N/A')}")
                return True
            else:
                print("❌ No series metadata returned")
                print(f"   Response: {data}")
                return False
        else:
            print(f"❌ HTTP {response.status_code}: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Network error: {e}")
        return False

def test_single_indicator():
    """Test fetching data for a single indicator"""
    print("\n📊 Testing single indicator observations (UNRATE)...")
    
    params = {
        'series_id': 'UNRATE',
        'api_key': FRED_API_KEY,
        'file_type': 'json',
        'limit': 10  # Just get last 10 observations
    }
    
    try:
        url = f"{FRED_BASE_URL}/series/observations"
        print(f"   Testing URL: {url}")
        
        response = requests.get(url, params=params, timeout=10)
        print(f"   Response Status: {response.status_code}")
        print(f"   Full URL called: {response.url}")
        
        if response.status_code == 200:
            data = response.json()
            
            if 'error_code' in data:
                print(f"❌ FRED API Error: {data.get('error_message', 'Unknown error')}")
                return False
            
            observations = data.get('observations', [])
            if observations:
                print(f"✅ Successfully fetched {len(observations)} observations")
                print(f"   Latest data point: {observations[-1]}")
                return True
            else:
                print("❌ No observations returned")
                print(f"   Response: {data}")
                return False
        else:
            print(f"❌ HTTP {response.status_code}: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Network error: {e}")
        return False

def test_direct_url():
    """Test with direct URL construction"""
    print("\n🌐 Testing direct URL construction...")
    
    if not FRED_API_KEY:
        print("❌ No API key available")
        return False
    
    # Test the exact URL format FRED expects
    test_url = f"https://api.stlouisfed.org/fred/series/observations?series_id=UNRATE&api_key={FRED_API_KEY}&file_type=json&limit=5"
    
    try:
        print(f"   Direct URL: {test_url[:80]}...")
        response = requests.get(test_url, timeout=10)
        print(f"   Response Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            if 'observations' in data:
                print(f"✅ Direct URL works! Got {len(data['observations'])} observations")
                return True
        
        print(f"❌ Direct URL failed: {response.text[:200]}")
        return False
        
    except Exception as e:
        print(f"❌ Direct URL error: {e}")
        return False

def main():
    """Run all tests"""
    print("🚀 FRED API Connection Test")
    print("=" * 50)
    
    # Test 1: API Key validation
    if not test_api_key():
        print("\n❌ API key test failed. Please check your .env file and API key.")
        return
    
    # Test 2: Series metadata
    if not test_series_metadata():
        print("\n❌ Metadata test failed.")
        return
    
    # Test 3: Single indicator
    if not test_single_indicator():
        print("\n❌ Indicator test failed.")
        # Try direct URL as backup
        test_direct_url()
        return
    
    # Test 4: Direct URL
    test_direct_url()
    
    print("\n🎉 All tests passed! Your FRED API connection is working correctly.")
    print("   You can now run the main application with: python app.py")

if __name__ == "__main__":
    main()