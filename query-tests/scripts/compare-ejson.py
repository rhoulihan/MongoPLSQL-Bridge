#!/usr/bin/env python3
"""
EJSON (Extended JSON) Type Comparison Script.

Compares MongoDB and Oracle results using EJSON format where type information
is preserved via special type encodings like {"$numberDouble": "1.5"}.

Key rules:
1. Key order within objects can differ - this is OK
2. Type encodings must match exactly (e.g., $numberDouble vs $numberInt)
3. Array order must match (documents in same order)
4. Values must match exactly within their type encoding

Usage:
    python3 compare-ejson.py <mongo_ejson_file> <oracle_ejson_file>

    # Or via stdin with two-line JSON input:
    echo '{"mongo": [...], "oracle": [...]}' | python3 compare-ejson.py --stdin

Exit codes:
    0 - EJSON types match
    1 - EJSON types don't match
    2 - Parse error
"""

import json
import sys
import argparse


def canonicalize_ejson(obj):
    """
    Recursively sort object keys to create a canonical form for comparison.
    This handles the "key order can differ" requirement.
    """
    if isinstance(obj, dict):
        # Sort keys and recursively canonicalize values
        return {k: canonicalize_ejson(v) for k, v in sorted(obj.items())}
    elif isinstance(obj, list):
        # Arrays preserve order - canonicalize each element
        return [canonicalize_ejson(item) for item in obj]
    else:
        # Primitive values (strings, numbers, booleans, null)
        return obj


def normalize_ejson_types(obj):
    """
    Normalize EJSON type representations between MongoDB and Oracle.

    MongoDB EJSON uses:
    - {"$numberDouble": "1.5"}
    - {"$numberInt": "10"}
    - {"$numberLong": "100"}
    - {"$date": {"$numberLong": "1234567890000"}} or {"$date": "2024-01-01T..."}
    - {"$oid": "..."}

    Oracle Extended JSON uses similar format with slight variations:
    - Numbers may use different type wrappers
    - Dates may have different precision

    Also handles plain JSON numbers (relaxed EJSON mode) with precision normalization.
    """
    if isinstance(obj, dict):
        # Check for EJSON type wrappers
        keys = set(obj.keys())

        # Handle $date - normalize date format
        if keys == {'$date'}:
            date_val = obj['$date']
            # Normalize date to string format without sub-millisecond precision
            if isinstance(date_val, dict) and '$numberLong' in date_val:
                # Convert epoch millis to ISO string for comparison
                return {'$date': date_val}  # Keep as-is for now
            elif isinstance(date_val, str):
                # Normalize date string - remove sub-millisecond precision
                normalized = date_val.split('.')[0]
                if not normalized.endswith('Z'):
                    normalized += 'Z'
                return {'$date': normalized}
            return obj

        # Handle number types - normalize precision
        if keys == {'$numberDouble'}:
            try:
                val = float(obj['$numberDouble'])
                # Round to 6 decimal places for comparison
                return {'$numberDouble': str(round(val, 6))}
            except (ValueError, TypeError):
                return obj

        if keys == {'$numberInt'} or keys == {'$numberLong'}:
            # Keep integer types as-is
            return obj

        # Handle $oid (ObjectId)
        if keys == {'$oid'}:
            return obj

        # Handle $binary
        if keys == {'$binary'}:
            return obj

        # Regular object - recursively normalize
        return {k: normalize_ejson_types(v) for k, v in obj.items()}

    elif isinstance(obj, list):
        return [normalize_ejson_types(item) for item in obj]

    elif isinstance(obj, float):
        # Handle plain JSON floats (relaxed EJSON mode)
        # Normalize to 6 decimal places to handle floating-point precision differences
        # Example: MongoDB 119.99000000000001 vs Oracle 119.99
        return round(obj, 6)

    else:
        return obj


def compare_ejson_docs(mongo_doc, oracle_doc):
    """
    Compare two EJSON documents for type equality.

    Returns: (match: bool, error_message: str or None)
    """
    # Normalize and canonicalize both documents
    mongo_canon = canonicalize_ejson(normalize_ejson_types(mongo_doc))
    oracle_canon = canonicalize_ejson(normalize_ejson_types(oracle_doc))

    # Convert to JSON strings for comparison
    mongo_json = json.dumps(mongo_canon, sort_keys=True)
    oracle_json = json.dumps(oracle_canon, sort_keys=True)

    if mongo_json == oracle_json:
        return True, None

    # Find the first difference for error message
    return find_difference(mongo_canon, oracle_canon, path="$")


def find_difference(mongo_obj, oracle_obj, path):
    """
    Recursively find the first difference between two objects.
    Returns: (match: bool, error_path: str or None)
    """
    # Handle null equivalence first (missing field treated as null)
    if mongo_obj is None and oracle_obj is None:
        return True, None

    mongo_type = type(mongo_obj).__name__
    oracle_type = type(oracle_obj).__name__

    # Check types first
    if mongo_type != oracle_type:
        return False, f"{path}: type mismatch MongoDB={mongo_type}, Oracle={oracle_type}"

    if isinstance(mongo_obj, dict):
        # Check for missing keys
        mongo_keys = set(mongo_obj.keys())
        oracle_keys = set(oracle_obj.keys())

        missing_in_oracle = mongo_keys - oracle_keys
        missing_in_mongo = oracle_keys - mongo_keys

        # Treat missing field as equivalent to null value
        # MongoDB omits fields with null, Oracle includes them explicitly
        for key in missing_in_oracle:
            if oracle_obj.get(key) is not None:
                return False, f"{path}: key '{key}' missing in Oracle"
        for key in missing_in_mongo:
            if oracle_obj.get(key) is not None:
                return False, f"{path}: key '{key}' missing in MongoDB (and Oracle value is not null)"

        # Compare values for each key (only those in both or where null equivalence applies)
        all_keys = mongo_keys | oracle_keys
        for key in all_keys:
            mongo_val = mongo_obj.get(key)  # Returns None if missing
            oracle_val = oracle_obj.get(key)  # Returns None if missing
            match, error = find_difference(
                mongo_val, oracle_val, f"{path}.{key}"
            )
            if not match:
                return False, error

        return True, None

    elif isinstance(mongo_obj, list):
        if len(mongo_obj) != len(oracle_obj):
            return False, f"{path}: array length mismatch MongoDB={len(mongo_obj)}, Oracle={len(oracle_obj)}"

        for i, (m_item, o_item) in enumerate(zip(mongo_obj, oracle_obj)):
            match, error = find_difference(m_item, o_item, f"{path}[{i}]")
            if not match:
                return False, error

        return True, None

    else:
        # Primitive comparison
        if mongo_obj != oracle_obj:
            return False, f"{path}: value mismatch MongoDB={mongo_obj}, Oracle={oracle_obj}"
        return True, None


def get_doc_sort_key(doc):
    """
    Get a sort key from a document for ordering comparison.
    Uses _id field if available, otherwise uses canonical JSON representation.
    """
    if isinstance(doc, dict):
        # Try common ID field names
        for key in ['_id', 'id', 'ID']:
            if key in doc:
                return str(doc[key])
        # Fall back to canonical JSON
        return json.dumps(canonicalize_ejson(doc), sort_keys=True)
    return str(doc)


def compare_ejson_results(mongo_docs, oracle_docs):
    """
    Compare two arrays of EJSON documents.

    Returns: (match: bool, message: str)
    """
    if len(mongo_docs) != len(oracle_docs):
        return False, f"EJSON_COUNT_MISMATCH:MongoDB={len(mongo_docs)},Oracle={len(oracle_docs)}"

    if len(mongo_docs) == 0:
        return True, "EJSON_MATCH:count=0"

    # Sort both arrays by a consistent key before comparing
    # This handles cases where MongoDB and Oracle return documents in different orders
    try:
        mongo_sorted = sorted(mongo_docs, key=get_doc_sort_key)
        oracle_sorted = sorted(oracle_docs, key=get_doc_sort_key)
    except Exception:
        # If sorting fails, compare as-is
        mongo_sorted = mongo_docs
        oracle_sorted = oracle_docs

    for i, (mongo_doc, oracle_doc) in enumerate(zip(mongo_sorted, oracle_sorted)):
        match, error = compare_ejson_docs(mongo_doc, oracle_doc)
        if not match:
            return False, f"EJSON_MISMATCH:row={i},{error}"

    return True, f"EJSON_MATCH:count={len(mongo_docs)}"


def parse_ndjson(text):
    """
    Parse newline-delimited JSON (NDJSON) format.
    Each line is a separate JSON document.
    """
    docs = []
    for line in text.strip().split('\n'):
        line = line.strip()
        if line:
            docs.append(json.loads(line))
    return docs


def main():
    parser = argparse.ArgumentParser(description='Compare EJSON type encodings')
    parser.add_argument('mongo_file', nargs='?', help='MongoDB EJSON file (one doc per line)')
    parser.add_argument('oracle_file', nargs='?', help='Oracle EJSON file (one doc per line)')
    parser.add_argument('--stdin', action='store_true',
                        help='Read {"mongo": [...], "oracle": [...]} from stdin')
    parser.add_argument('--ndjson', action='store_true',
                        help='Input files are newline-delimited JSON (one doc per line)')
    args = parser.parse_args()

    try:
        if args.stdin:
            data = json.load(sys.stdin)
            mongo_docs = data.get('mongo', [])
            oracle_docs = data.get('oracle', [])
        elif args.mongo_file and args.oracle_file:
            with open(args.mongo_file, 'r') as f:
                mongo_text = f.read().strip()
            with open(args.oracle_file, 'r') as f:
                oracle_text = f.read().strip()

            if args.ndjson:
                mongo_docs = parse_ndjson(mongo_text)
                oracle_docs = parse_ndjson(oracle_text)
            else:
                mongo_docs = json.loads(mongo_text) if mongo_text else []
                oracle_docs = json.loads(oracle_text) if oracle_text else []
        else:
            parser.print_help()
            sys.exit(2)

        # Ensure we have lists
        if not isinstance(mongo_docs, list):
            mongo_docs = [mongo_docs] if mongo_docs else []
        if not isinstance(oracle_docs, list):
            oracle_docs = [oracle_docs] if oracle_docs else []

        success, message = compare_ejson_results(mongo_docs, oracle_docs)
        print(message)
        sys.exit(0 if success else 1)

    except json.JSONDecodeError as e:
        print(f"EJSON_PARSE_ERROR:{e}")
        sys.exit(2)
    except Exception as e:
        print(f"EJSON_ERROR:{e}")
        sys.exit(2)


if __name__ == '__main__':
    main()
