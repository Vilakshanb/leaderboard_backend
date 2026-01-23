import sys
import os

# Add root to path to find tests module
sys.path.append(os.getcwd())

try:
    from tests.parity.compare_json import compare_snapshots
except ImportError:
    # Fallback if running from tools/ or similar
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from tests.parity.compare_json import compare_snapshots

def main():
    gold_path = 'gold/2025-11/default/Leaderboard_Lumpsum.json'
    engine_path = 'engine_artifacts/Leaderboard_Lumpsum.json'

    if not os.path.exists(gold_path):
        print(f"Gold file not found: {gold_path}")
        sys.exit(1)
    if not os.path.exists(engine_path):
        print(f"Engine file not found: {engine_path}")
        sys.exit(1)

    errors = compare_snapshots(gold_path, engine_path)

    if not errors:
        print('✅ SUCCESS: TS Engine Lumpsum output matches Gold (Default)!')
        sys.exit(0)
    else:
        print(f'❌ FAILURE: {len(errors)} differences found:')
        for e in errors[:15]:
            print(f'  - {e}')
        if len(errors) > 15:
            print(f'  ...and {len(errors)-15} more.')
        sys.exit(1)

if __name__ == "__main__":
    main()
