#!/usr/bin/env python
# ============================================================================
# SCHEMA DEPLOYMENT SCRIPT
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# PURPOSE: Deploy dagapp schema to PostgreSQL using PydanticToSQL
# USAGE:
#   python scripts/deploy_schema.py --dry-run    # Preview SQL
#   python scripts/deploy_schema.py              # Execute deployment
#   python scripts/deploy_schema.py --status     # Check current status
# ============================================================================

import sys
import os
import argparse

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from infrastructure import DatabaseInitializer, initialize_database


def main():
    parser = argparse.ArgumentParser(
        description="Deploy dagapp schema to PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/deploy_schema.py --dry-run     # Preview DDL without executing
  python scripts/deploy_schema.py               # Deploy schema
  python scripts/deploy_schema.py --status      # Check current installation

Environment Variables:
  DATABASE_URL          Full PostgreSQL connection string
  POSTGRES_HOST         Database host (default: localhost)
  POSTGRES_DB           Database name (default: postgres)
  POSTGRES_USER         Database user (default: postgres)
  POSTGRES_PASSWORD     Database password
  POSTGRES_PORT         Database port (default: 5432)
  POSTGRES_SSLMODE      SSL mode (default: require)
        """
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print DDL without executing"
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Check current installation status"
    )
    parser.add_argument(
        "--connection",
        type=str,
        help="PostgreSQL connection string (overrides environment)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    args = parser.parse_args()

    # Configure logging
    import logging
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    print("=" * 70)
    print("RMHDAGMASTER - Schema Deployment")
    print("=" * 70)

    # Create initializer
    initializer = DatabaseInitializer(connection_string=args.connection)

    print(f"Host: {initializer.host}")
    print(f"Database: {initializer.database}")
    print(f"Schema: {initializer.SCHEMA_NAME}")
    print("=" * 70)

    if args.status:
        # Just check status
        print("\n[STATUS CHECK]\n")
        status = initializer.verify_installation()

        print(f"Schema exists: {status.get('schema_exists', False)}")

        if status.get("tables"):
            print(f"\nTables ({len(status['tables'])}):")
            for table, info in status["tables"].items():
                print(f"  - {initializer.SCHEMA_NAME}.{table} ({info['columns']} columns)")

        if status.get("enum_types"):
            print(f"\nEnum types ({len(status['enum_types'])}):")
            for enum_type in status["enum_types"]:
                print(f"  - {enum_type}")

        if status.get("error"):
            print(f"\nError: {status['error']}")
            sys.exit(1)

        # Get row counts
        counts = initializer.get_table_counts()
        if any(c >= 0 for c in counts.values()):
            print(f"\nRow counts:")
            for table, count in counts.items():
                if count >= 0:
                    print(f"  - {table}: {count}")

        print("\n" + "=" * 70)
        return

    # Deploy schema
    print(f"\nMode: {'DRY RUN' if args.dry_run else 'EXECUTE'}\n")

    result = initializer.initialize_all(dry_run=args.dry_run)

    # Print results
    print("\n[RESULTS]\n")
    for step in result.steps:
        status_emoji = {
            "success": "✅",
            "failed": "❌",
            "skipped": "⏭️"
        }.get(step.status, "❓")

        print(f"{status_emoji} {step.name}: {step.message}")
        if step.error:
            print(f"   Error: {step.error}")
        if step.details and args.verbose:
            for key, value in step.details.items():
                print(f"   {key}: {value}")

    print("\n" + "=" * 70)
    if result.success:
        print("✅ Deployment completed successfully!")
    else:
        print("❌ Deployment failed!")
        for error in result.errors:
            print(f"   - {error}")
        sys.exit(1)
    print("=" * 70)


if __name__ == "__main__":
    main()
