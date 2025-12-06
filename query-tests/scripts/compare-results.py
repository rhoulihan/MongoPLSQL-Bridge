#!/usr/bin/env python3
"""
Document structure comparison for cross-database validation.

Compares MongoDB results with Oracle results to verify they have:
1. The same number of documents
2. The same document structure (fields)
3. Equivalent values (with tolerance for numeric precision)

Usage:
    python3 compare-results.py <mongo_file> <oracle_file> [--sort-by <field>]

    # Or via stdin with JSON on separate lines:
    echo '{"mongo": [...], "oracle": [...]}' | python3 compare-results.py

    # Try strict first, then loose on failure:
    python3 compare-results.py --try-both ...

Exit codes:
    0 - Results match (STRICT_MATCH or LOOSE_MATCH)
    1 - Results don't match (NO_MATCH)
    2 - Parse error

Output format:
    STRICT_MATCH:count=N - Values and types match exactly
    LOOSE_MATCH:count=N  - Values match with type coercion (e.g., "10" == 10)
    NO_MATCH:reason      - Results don't match
"""

import json
import sys
import argparse


def normalize_value(v, strict_types=True):
    """
    Normalize values for comparison (handle numeric precision, None, etc.)

    Args:
        v: The value to normalize
        strict_types: If True (default), strings are NOT coerced to numbers.
                      This ensures "10" (string) != 10 (number).
    """
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        # Round to 6 decimal places for comparison
        return round(float(v), 6)
    if isinstance(v, str):
        if strict_types:
            # STRICT: Keep strings as strings - don't coerce to numbers
            # This catches type mismatches like "10" vs 10
            return v.strip() if v else v
        else:
            # LOOSE: Try to parse as number if it looks like one
            try:
                f = float(v)
                return round(f, 6)
            except (ValueError, TypeError):
                return v.strip() if v else v
    if isinstance(v, dict):
        return normalize_doc(v, strict_types)
    if isinstance(v, list):
        return [normalize_value(x, strict_types) for x in v]
    return v


def normalize_doc(doc, strict_types=True):
    """Normalize a document for comparison"""
    if not isinstance(doc, dict):
        return normalize_value(doc, strict_types)
    result = {}
    for k, v in doc.items():
        # Skip internal fields (except _id)
        if k.startswith('_') and k != '_id':
            continue
        # Normalize the key: strip quotes and convert to uppercase for case-insensitive
        key = k.strip('"').strip("'").upper()
        # Map MongoDB _id to ID (Oracle uses ID without underscore)
        if key == '_ID':
            key = 'ID'
        # Also handle GRP_ID -> ID mapping for grouped results
        if key == 'GRP_ID':
            key = 'ID'
        # Normalize common aggregate aliases
        if key == 'CNT':
            key = 'COUNT'
        result[key] = normalize_value(v, strict_types)
    return result


def normalize_bool(val):
    """Normalize boolean values for comparison"""
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        if val.lower() == 'true':
            return True
        if val.lower() == 'false':
            return False
    if isinstance(val, (int, float)):
        # 1/0 can represent true/false
        if val == 1:
            return True
        if val == 0:
            return False
    return val


def values_equal(mongo_val, oracle_val, strict_types=True):
    """
    Check if two values are equal.

    Args:
        mongo_val: Value from MongoDB result
        oracle_val: Value from Oracle result
        strict_types: If True (default), types must match exactly.
                      e.g., "10" (string) != 10 (number)
    """
    if mongo_val == oracle_val:
        return True

    # STRICT TYPE CHECKING: If types differ, values don't match
    if strict_types:
        mongo_type = type(mongo_val).__name__
        oracle_type = type(oracle_val).__name__
        # Allow int/float interchangeability (both are numbers)
        mongo_is_num = isinstance(mongo_val, (int, float)) and not isinstance(mongo_val, bool)
        oracle_is_num = isinstance(oracle_val, (int, float)) and not isinstance(oracle_val, bool)
        if not (mongo_is_num and oracle_is_num):
            # Types must match exactly (unless both are numbers)
            if mongo_type != oracle_type:
                return False

    # Check for boolean equality (handles True vs "true" vs 1)
    # Only allow if NOT in strict mode, or both are actual booleans
    if isinstance(mongo_val, bool) and isinstance(oracle_val, bool):
        if mongo_val == oracle_val:
            return True
    elif not strict_types:
        mongo_bool = normalize_bool(mongo_val)
        oracle_bool = normalize_bool(oracle_val)
        if isinstance(mongo_bool, bool) and isinstance(oracle_bool, bool):
            if mongo_bool == oracle_bool:
                return True

    # Check for numeric equality with tolerance
    if isinstance(mongo_val, (int, float)) and isinstance(oracle_val, (int, float)):
        if not isinstance(mongo_val, bool) and not isinstance(oracle_val, bool):
            if abs(float(mongo_val) - float(oracle_val)) < 0.0001:
                return True

    # Check for array set equality (same elements, possibly different order)
    if isinstance(mongo_val, list) and isinstance(oracle_val, list):
        if len(mongo_val) != len(oracle_val):
            return False
        # Try set comparison for primitive types
        try:
            mongo_set = set(
                json.dumps(x, sort_keys=True) if isinstance(x, dict) else str(x)
                for x in mongo_val
            )
            oracle_set = set(
                json.dumps(x, sort_keys=True) if isinstance(x, dict) else str(x)
                for x in oracle_val
            )
            if mongo_set == oracle_set:
                return True
        except Exception:
            pass

    return False


def docs_match(mongo_doc, oracle_doc, strict_types=True):
    """Check if two documents have matching structure and values"""
    mongo_norm = normalize_doc(mongo_doc, strict_types)
    oracle_norm = normalize_doc(oracle_doc, strict_types)

    # Get all keys from both documents
    all_keys = set(mongo_norm.keys()) | set(oracle_norm.keys())

    for key in all_keys:
        mongo_val = mongo_norm.get(key)
        oracle_val = oracle_norm.get(key)

        if not values_equal(mongo_val, oracle_val, strict_types):
            # Include type info for better debugging
            mongo_type = type(mongo_val).__name__
            oracle_type = type(oracle_val).__name__
            return (
                False,
                f"Field '{key}' mismatch: MongoDB={mongo_val} ({mongo_type}), "
                f"Oracle={oracle_val} ({oracle_type})"
            )

    return True, None


def get_sort_key(doc):
    """Get a sort key from document for ordering comparison"""
    # Try common ID field names
    for key in ['_id', 'id', 'ID', 'Id', 'grp_id', 'GRP_ID']:
        if key in doc:
            return str(doc[key])
    # Fall back to first field value
    if doc:
        return str(list(doc.values())[0])
    return ''


def compare_results(mongo_docs, oracle_docs, sort_by=None, strict_types=True):
    """
    Compare MongoDB and Oracle result sets.

    Args:
        mongo_docs: List of documents from MongoDB
        oracle_docs: List of documents from Oracle
        sort_by: Optional field to sort by before comparison
        strict_types: If True (default), types must match (e.g., "10" != 10)

    Returns:
        tuple: (success: bool, message: str)
        Message prefixes: STRICT_MATCH, LOOSE_MATCH, COUNT_MISMATCH, DOC_MISMATCH
    """
    mongo_count = len(mongo_docs)
    oracle_count = len(oracle_docs)

    if mongo_count != oracle_count:
        return False, f"COUNT_MISMATCH:MongoDB={mongo_count},Oracle={oracle_count}"

    if mongo_count == 0:
        prefix = "STRICT_MATCH" if strict_types else "LOOSE_MATCH"
        return True, f"{prefix}:count=0"

    # Sort both arrays for comparison - auto-sort by ID if no sort_by specified
    try:
        if sort_by:
            mongo_docs = sorted(mongo_docs, key=lambda x: str(x.get(sort_by, '')))
            oracle_docs = sorted(oracle_docs, key=lambda x: str(x.get(sort_by, '')))
        else:
            # Auto-sort by ID field
            mongo_docs = sorted(mongo_docs, key=get_sort_key)
            oracle_docs = sorted(oracle_docs, key=get_sort_key)
    except Exception:
        pass  # If sorting fails, compare as-is

    # Compare each document
    for i, (mongo_doc, oracle_doc) in enumerate(zip(mongo_docs, oracle_docs)):
        match, error = docs_match(mongo_doc, oracle_doc, strict_types)
        if not match:
            return False, f"DOC_MISMATCH:row={i},{error}"

    prefix = "STRICT_MATCH" if strict_types else "LOOSE_MATCH"
    return True, f"{prefix}:count={mongo_count}"


def compare_with_fallback(mongo_docs, oracle_docs, sort_by=None):
    """
    Compare results trying strict mode first, then loose mode on failure.

    Returns:
        tuple: (success: bool, message: str, match_type: str)
        match_type: 'strict', 'loose', or 'none'
    """
    # First try strict comparison
    success, message = compare_results(mongo_docs, oracle_docs, sort_by, strict_types=True)
    if success:
        return True, message, 'strict'

    # If strict fails, try loose comparison
    success, message = compare_results(mongo_docs, oracle_docs, sort_by, strict_types=False)
    if success:
        return True, message, 'loose'

    # Neither matched
    return False, message, 'none'


def main():
    parser = argparse.ArgumentParser(description='Compare MongoDB and Oracle query results')
    parser.add_argument('mongo_file', nargs='?', help='MongoDB results JSON file')
    parser.add_argument('oracle_file', nargs='?', help='Oracle results JSON file')
    parser.add_argument('--sort-by', '-s', help='Field to sort by before comparison')
    parser.add_argument('--stdin', action='store_true', help='Read JSON from stdin')
    parser.add_argument('--try-both', action='store_true',
                        help='Try strict first, then loose on failure')
    args = parser.parse_args()

    try:
        if args.stdin or (not args.mongo_file and not args.oracle_file):
            # Read from stdin
            data = json.load(sys.stdin)
            mongo_docs = data.get('mongo', [])
            oracle_docs = data.get('oracle', [])
            sort_by = data.get('sort_by') or args.sort_by
        else:
            # Read from files
            with open(args.mongo_file, 'r') as f:
                mongo_json = f.read().strip()
                mongo_docs = json.loads(mongo_json) if mongo_json else []

            with open(args.oracle_file, 'r') as f:
                oracle_json = f.read().strip()
                if not oracle_json or oracle_json == 'null':
                    oracle_docs = []
                else:
                    oracle_docs = json.loads(oracle_json)
                    if oracle_docs is None:
                        oracle_docs = []
                    elif not isinstance(oracle_docs, list):
                        oracle_docs = [oracle_docs]

            sort_by = args.sort_by

        # Ensure mongo_docs is a list
        if not isinstance(mongo_docs, list):
            mongo_docs = [mongo_docs] if mongo_docs else []

        if args.try_both:
            success, message, match_type = compare_with_fallback(mongo_docs, oracle_docs, sort_by)
        else:
            success, message = compare_results(mongo_docs, oracle_docs, sort_by)
        print(message)
        sys.exit(0 if success else 1)

    except json.JSONDecodeError as e:
        print(f"PARSE_ERROR:{e}")
        sys.exit(2)
    except Exception as e:
        print(f"ERROR:{e}")
        sys.exit(2)


if __name__ == '__main__':
    main()
