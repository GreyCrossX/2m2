#!/usr/bin/env python3
"""
Debug script to validate API credentials format
Run this in your container to check your environment variables
"""

import os
import re


def validate_binance_credentials():
    """Validate Binance API credentials format."""

    # Get environment variables
    testnet_key = os.getenv("TESTNET_API_KEY", "")
    testnet_secret = os.getenv("TESTNET_API_SECRET", "")
    prod_key = os.getenv("API_KEY", "")
    prod_secret = os.getenv("API_SECRET", "")
    base_path = os.getenv("BASE_PATH", "")

    print("=== Environment Variables Debug ===")
    print(
        f"TESTNET_API_KEY length: {len(testnet_key)} (first 8 chars: '{testnet_key[:8]}...')"
    )
    print(
        f"TESTNET_API_SECRET length: {len(testnet_secret)} (first 8 chars: '{testnet_secret[:8]}...')"
    )
    print(f"API_KEY length: {len(prod_key)} (first 8 chars: '{prod_key[:8]}...')")
    print(
        f"API_SECRET length: {len(prod_secret)} (first 8 chars: '{prod_secret[:8]}...')"
    )
    print(f"BASE_PATH: {base_path}")

    # Choose which credentials to validate
    api_key = testnet_key if testnet_key else prod_key
    api_secret = testnet_secret if testnet_secret else prod_secret

    print(f"\n=== Using {'TESTNET' if testnet_key else 'PROD'} credentials ===")

    # Validation checks
    issues = []

    if not api_key:
        issues.append("❌ No API key found in environment")
    elif len(api_key) < 32:
        issues.append(f"❌ API key too short ({len(api_key)} chars, expected ≥32)")
    elif not re.match(r"^[A-Za-z0-9_-]+$", api_key):
        issues.append(
            "❌ API key contains invalid characters (should be alphanumeric + underscores/hyphens)"
        )
    else:
        print(f"✅ API key format looks valid ({len(api_key)} chars)")

    if not api_secret:
        issues.append("❌ No API secret found in environment")
    elif len(api_secret) < 32:
        issues.append(
            f"❌ API secret too short ({len(api_secret)} chars, expected ≥32)"
        )
    elif not re.match(r"^[A-Za-z0-9_-]+$", api_secret):
        issues.append(
            "❌ API secret contains invalid characters (should be alphanumeric + underscores/hyphens)"
        )
    else:
        print(f"✅ API secret format looks valid ({len(api_secret)} chars)")

    # Check for common issues
    if api_key.startswith('"') or api_key.endswith('"'):
        issues.append("⚠️  API key has quotes - these will be stripped")
    if api_secret.startswith('"') or api_secret.endswith('"'):
        issues.append("⚠️  API secret has quotes - these will be stripped")

    if api_key.startswith("'") or api_key.endswith("'"):
        issues.append("⚠️  API key has single quotes - these will be stripped")
    if api_secret.startswith("'") or api_secret.endswith("'"):
        issues.append("⚠️  API secret has single quotes - these will be stripped")

    # Print issues
    if issues:
        print("\n=== Issues Found ===")
        for issue in issues:
            print(issue)
    else:
        print("\n✅ All credential checks passed!")

    # Test sanitization
    print("\n=== Testing Sanitization ===")

    def sanitize(x):
        if not x:
            return ""
        x = x.strip()
        if len(x) >= 2 and x[0] == x[-1] and x[0] in ("'", '"'):
            return x[1:-1].strip()
        return x

    clean_key = sanitize(api_key)
    clean_secret = sanitize(api_secret)

    print(f"Original API key length: {len(api_key)}")
    print(f"Sanitized API key length: {len(clean_key)}")
    print(f"Original API secret length: {len(api_secret)}")
    print(f"Sanitized API secret length: {len(clean_secret)}")

    if clean_key != api_key:
        print("⚠️  API key was modified by sanitization")
    if clean_secret != api_secret:
        print("⚠️  API secret was modified by sanitization")


if __name__ == "__main__":
    validate_binance_credentials()
