"""
Binance API Debug Tool

This script helps diagnose issues with your Binance testnet API configuration.
Run this to identify the exact problem with your API keys.
"""

import os
import sys
import logging
import requests
import hashlib
import hmac
import time
from urllib.parse import urlencode

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
LOG = logging.getLogger("binance_debug")

# Your configuration
API_KEY = os.getenv("TESTNET_API_KEY", "6e39984cba7dc317232d9510d6b06a1aab59703720637e1e1d79608cfe5e8d69")
API_SECRET = os.getenv("TESTNET_API_SECRET", "1a54fe2c45931502505d8bc6ebd983be2fe1165bacfc8b9d33a50f68e5751bf4")
BASE_URL = os.getenv("BASE_PATH", "https://testnet.binancefuture.com")

def validate_credentials():
    """Basic validation of credentials format"""
    print("=== Credential Validation ===")
    
    if not API_KEY:
        print("❌ API_KEY is empty")
        return False
    
    if not API_SECRET:
        print("❌ API_SECRET is empty")
        return False
    
    print(f"✅ API_KEY length: {len(API_KEY)} (should be ~64)")
    print(f"✅ API_SECRET length: {len(API_SECRET)} (should be ~64)")
    print(f"✅ BASE_URL: {BASE_URL}")
    
    if len(API_KEY) < 50 or len(API_SECRET) < 50:
        print("⚠️  Keys seem short - verify they are correct")
        
    return True

def create_signature(query_string, secret):
    """Create HMAC SHA256 signature"""
    return hmac.new(
        secret.encode('utf-8'),
        query_string.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

def test_server_time():
    """Test server time endpoint (no auth required)"""
    print("\n=== Testing Server Time (No Auth) ===")
    
    try:
        url = f"{BASE_URL}/fapi/v1/time"
        response = requests.get(url, timeout=10)
        
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        
        if response.status_code == 200:
            print("✅ Server connection working")
            return True
        else:
            print("❌ Server connection failed")
            return False
            
    except Exception as e:
        print(f"❌ Server connection error: {e}")
        return False

def test_account_info():
    """Test account info endpoint (requires auth)"""
    print("\n=== Testing Account Info (With Auth) ===")
    
    try:
        timestamp = int(time.time() * 1000)
        
        # Parameters
        params = {
            'timestamp': timestamp,
            'recvWindow': 60000
        }
        
        # Create query string
        query_string = urlencode(params)
        
        # Create signature
        signature = create_signature(query_string, API_SECRET)
        
        # Add signature to params
        params['signature'] = signature
        
        # Headers
        headers = {
            'X-MBX-APIKEY': API_KEY
        }
        
        # Make request
        url = f"{BASE_URL}/fapi/v2/account"
        response = requests.get(url, headers=headers, params=params, timeout=10)
        
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text[:500]}...")  # First 500 chars
        
        if response.status_code == 200:
            print("✅ Account info retrieved successfully")
            return True
        else:
            print("❌ Account info failed")
            
            # Parse error details
            try:
                error_data = response.json()
                if 'code' in error_data and 'msg' in error_data:
                    print(f"Error Code: {error_data['code']}")
                    print(f"Error Message: {error_data['msg']}")
                    
                    # Common error codes and solutions
                    if error_data['code'] == -2015:
                        print("\n🔧 Error -2015 Solutions:")
                        print("1. Check API key has 'Enable Futures' permission")
                        print("2. Remove IP restrictions from API key")
                        print("3. Verify you're using testnet keys with testnet URL")
                        print("4. Try generating new testnet API keys")
                        
                    elif error_data['code'] == -1021:
                        print("\n🔧 Error -1021 Solutions:")
                        print("1. Check system time synchronization")
                        print("2. Increase recvWindow parameter")
                        
            except:
                pass
                
            return False
            
    except Exception as e:
        print(f"❌ Account info error: {e}")
        return False

def test_balance():
    """Test balance endpoint (requires auth)"""
    print("\n=== Testing Balance Endpoint (With Auth) ===")
    
    try:
        timestamp = int(time.time() * 1000)
        
        # Parameters
        params = {
            'timestamp': timestamp,
            'recvWindow': 60000
        }
        
        # Create query string
        query_string = urlencode(params)
        
        # Create signature
        signature = create_signature(query_string, API_SECRET)
        
        # Add signature to params
        params['signature'] = signature
        
        # Headers
        headers = {
            'X-MBX-APIKEY': API_KEY
        }
        
        # Make request (this is the endpoint your code is using)
        url = f"{BASE_URL}/fapi/v3/balance"
        response = requests.get(url, headers=headers, params=params, timeout=10)
        
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text[:500]}...")  # First 500 chars
        
        if response.status_code == 200:
            print("✅ Balance retrieved successfully")
            
            # Try to parse and show balance details
            try:
                balance_data = response.json()
                if isinstance(balance_data, list):
                    print(f"Found {len(balance_data)} asset balances")
                    for asset in balance_data:
                        if float(asset.get('balance', 0)) > 0:
                            print(f"  {asset.get('asset')}: {asset.get('balance')} (available: {asset.get('availableBalance')})")
                else:
                    print("Unexpected balance format")
            except:
                pass
                
            return True
        else:
            print("❌ Balance retrieval failed")
            return False
            
    except Exception as e:
        print(f"❌ Balance error: {e}")
        return False

def diagnose_issues():
    """Run comprehensive diagnostic"""
    print("🔍 BINANCE API DIAGNOSTIC TOOL")
    print("=" * 50)
    
    # Check credentials
    if not validate_credentials():
        print("\n❌ FATAL: Invalid credentials")
        return False
    
    # Test server connection
    if not test_server_time():
        print("\n❌ FATAL: Cannot connect to server")
        return False
    
    # Test authentication
    if not test_account_info():
        print("\n❌ FATAL: Authentication failed")
        return False
    
    # Test balance endpoint
    if not test_balance():
        print("\n❌ FATAL: Balance endpoint failed")
        return False
    
    print("\n✅ ALL TESTS PASSED!")
    print("Your API configuration should be working.")
    
    return True

def show_checklist():
    """Show manual checklist for common issues"""
    print("\n" + "=" * 50)
    print("MANUAL CHECKLIST - Please verify:")
    print("=" * 50)
    print("1. ✓ Go to https://testnet.binancefuture.com")
    print("2. ✓ Login to your testnet account")
    print("3. ✓ Go to API Management")
    print("4. ✓ Check your API key has 'Enable Futures' permission")
    print("5. ✓ Ensure no IP restrictions (or add your server IP)")
    print("6. ✓ Try generating new API keys if problems persist")
    print("7. ✓ Verify you're not mixing mainnet/testnet credentials")
    print("8. ✓ Check system time is synchronized")

if __name__ == "__main__":
    success = diagnose_issues()
    
    if not success:
        show_checklist()
        
        print("\nNext steps:")
        print("1. Fix the issues identified above")
        print("2. Run this diagnostic again")
        print("3. If still failing, try generating new testnet API keys")
        
    print("\n" + "=" * 50)
    print("END OF DIAGNOSTIC")
    print("=" * 50)