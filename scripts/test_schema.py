#!/usr/bin/env python
# ============================================================================
# SCHEMA TEST SCRIPT
# ============================================================================
# Tests database connection and DDL generation
# Run: python scripts/test_schema.py
# ============================================================================

import sys
sys.path.insert(0, '/Users/robertharrison/python_builds/rmhdagmaster')

from core.schema import PydanticToSQL

def main():
    print("=" * 70)
    print("RMHDAGMASTER - Schema Generation Test")
    print("=" * 70)

    # Create generator for dagapp schema
    generator = PydanticToSQL(schema_name="dagapp")

    print("\n1. Generating DDL statements...")
    statements = generator.generate_all()

    print(f"\n2. Generated {len(statements)} DDL statements:\n")

    # Print statements (for review)
    for i, stmt in enumerate(statements, 1):
        # Convert to string for display
        sql_str = stmt.as_string(None) if hasattr(stmt, 'as_string') else str(stmt)
        print(f"-- Statement {i}")
        print(sql_str)
        print()

    print("=" * 70)
    print("DDL generation complete. Review statements above.")
    print("=" * 70)

if __name__ == "__main__":
    main()
