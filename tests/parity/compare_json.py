
import json
import math
from typing import Any, Dict, List, Tuple, Union

def compare_values(v1: Any, v2: Any, path: str, float_tol: float = 0.01) -> List[str]:
    """
    Compare two values recursively. Return list of error strings.
    """
    errors = []

    # Type check (loose for numbers)
    if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
        # Float comparison
        try:
            diff = abs(float(v1) - float(v2))
            if diff > float_tol:
                errors.append(f"{path}: {v1} != {v2} (diff {diff} > {float_tol})")
        except:
             errors.append(f"{path}: Could not compare numbers {v1} and {v2}")
        return errors

    if type(v1) != type(v2):
        # Allow None vs "" or similar? Strict for now.
        errors.append(f"{path}: Type mismatch ({type(v1)} vs {type(v2)})")
        return errors

    if isinstance(v1, dict):
        return compare_dicts(v1, v2, path, float_tol)
    elif isinstance(v1, list):
        return compare_lists(v1, v2, path, float_tol)
    else:
        # Primitive equality
        if v1 != v2:
            errors.append(f"{path}: {v1} != {v2}")
        return errors

def compare_dicts(d1: Dict, d2: Dict, path: str, float_tol: float) -> List[str]:
    errors = []
    keys1 = set(d1.keys())
    keys2 = set(d2.keys())

    # Missing keys
    for k in keys1 - keys2:
        errors.append(f"{path}.{k}: Missing in Actual") # d2 is actual
    for k in keys2 - keys1:
        errors.append(f"{path}.{k}: Extra in Actual")

    # Common keys
    for k in keys1 & keys2:
        errors.extend(compare_values(d1[k], d2[k], f"{path}.{k}", float_tol))

    return errors

def compare_lists(l1: List, l2: List, path: str, float_tol: float) -> List[str]:
    errors = []
    if len(l1) != len(l2):
        errors.append(f"{path}: Length mismatch ({len(l1)} vs {len(l2)})")
        # Try to compare items anyway up to min length

    for i in range(min(len(l1), len(l2))):
        errors.extend(compare_values(l1[i], l2[i], f"{path}[{i}]", float_tol))

    return errors

def compare_snapshots(gold_path: str, actual_path: str) -> List[str]:
    """
    Compare two JSON snapshot files.
    Returns list of mismatch descriptions.
    """
    try:
        with open(gold_path, 'r') as f:
            gold = json.load(f)
    except FileNotFoundError:
        return [f"Gold file missing: {gold_path}"]

    try:
        with open(actual_path, 'r') as f:
            actual = json.load(f)
    except FileNotFoundError:
        return [f"Actual file missing: {actual_path}"]

    # Sort logic (assuming list of docs)
    # We normalized sort in export, but we should ensure both are sorted similarly just in case.
    # Assuming the structure is List[Dict].
    if isinstance(gold, list):
        # Try to sort determinesistically using employee_id if present
        def sort_key(x):
            if isinstance(x, dict):
                return x.get("employee_id") or x.get("_id") or str(x)
            return str(x)

        try:
            gold.sort(key=sort_key)
            actual.sort(key=sort_key)
        except:
            pass # fallback to index based


    return compare_values(gold, actual, "ROOT")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: python compare_json.py <gold.json> <actual.json>")
        sys.exit(1)

    gold_file = sys.argv[1]
    actual_file = sys.argv[2]

    errors = compare_snapshots(gold_file, actual_file)
    if errors:
        print(f"FAILED: Found {len(errors)} differences")
        for e in errors[:20]:
            print(f"  - {e}")
        if len(errors) > 20:
            print(f"  ... and {len(errors)-20} more.")
        sys.exit(1)
    else:
        print("PASS: Snapshots are identical (within tolerance)")
        sys.exit(0)

