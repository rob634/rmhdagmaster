#!/usr/bin/env python3
# ============================================================================
# CLI JOB SUBMISSION TOOL
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Tool - Submit jobs via Service Bus queue
# PURPOSE: Test job submission without the Function App gateway
# CREATED: 07 FEB 2026
# ============================================================================
"""
Submit jobs to the dag-jobs Service Bus queue.

Simulates exactly what the Function App gateway does:
1. Creates a JobQueueMessage
2. Publishes to the dag-jobs queue
3. Orchestrator consumes, creates Job with ownership, processes it

Usage:
    # Echo test
    python tools/submit_job.py echo_test '{"message": "hello", "count": 3}'

    # Fan-out test
    python tools/submit_job.py fan_out_test '{"item_list": ["alpha", "bravo", "charlie"]}'

    # With correlation ID for tracking
    python tools/submit_job.py echo_test '{"message": "test"}' --correlation-id my-trace-001

    # Poll for completion
    python tools/submit_job.py echo_test '{"message": "test"}' --poll

Requires:
    SERVICE_BUS_CONNECTION_STRING env var (or SERVICE_BUS_NAMESPACE for managed identity)
"""

import argparse
import json
import os
import sys
import time

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.models import JobQueueMessage
from infrastructure.service_bus import ServiceBusPublisher


def submit_job(
    workflow_id: str,
    input_params: dict,
    correlation_id: str = None,
    queue_name: str = "dag-jobs",
) -> str:
    """Submit a job to the Service Bus queue."""
    msg = JobQueueMessage(
        workflow_id=workflow_id,
        input_params=input_params,
        correlation_id=correlation_id,
        submitted_by="cli-submit-tool",
    )

    publisher = ServiceBusPublisher.instance()
    message_id = publisher.send_message(queue_name, msg, ttl_hours=24)
    return message_id


def poll_status(correlation_id: str, orchestrator_url: str, timeout: int = 120):
    """Poll orchestrator for job status until terminal."""
    import httpx

    print(f"\nPolling for completion (correlation_id={correlation_id})...")
    print(f"  Orchestrator: {orchestrator_url}")
    print(f"  Timeout: {timeout}s\n")

    start = time.time()
    job_id = None

    while time.time() - start < timeout:
        try:
            with httpx.Client(timeout=10.0) as client:
                if job_id:
                    # We know the job_id, poll directly
                    url = f"{orchestrator_url}/api/v1/jobs/{job_id}"
                    resp = client.get(url)
                    if resp.status_code == 200:
                        data = resp.json()
                        # JobDetailResponse has {"job": {...}, "nodes": [...]}
                        job_data = data.get("job", data)
                        status = job_data.get("status", "unknown")
                        elapsed = int(time.time() - start)
                        print(f"  [{elapsed:3d}s] {job_id[:12]}... status={status}")

                        if status in ("completed", "failed", "cancelled"):
                            print(f"\n--- FINAL RESULT ---")
                            print(json.dumps(data, indent=2, default=str))
                            return data
                else:
                    # List all jobs and find by correlation_id
                    url = f"{orchestrator_url}/api/v1/jobs"
                    resp = client.get(url)
                    if resp.status_code == 200:
                        data = resp.json()
                        jobs = data.get("jobs", [])
                        match = next(
                            (j for j in jobs if j.get("correlation_id") == correlation_id),
                            None,
                        )
                        if match:
                            job_id = match["job_id"]
                            elapsed = int(time.time() - start)
                            print(f"  [{elapsed:3d}s] Found job {job_id[:12]}... status={match['status']}")
                            if match["status"] in ("completed", "failed", "cancelled"):
                                # Fetch full detail
                                detail_resp = client.get(f"{orchestrator_url}/api/v1/jobs/{job_id}")
                                if detail_resp.status_code == 200:
                                    detail = detail_resp.json()
                                    print(f"\n--- FINAL RESULT ---")
                                    print(json.dumps(detail, indent=2, default=str))
                                    return detail
                        else:
                            elapsed = int(time.time() - start)
                            print(f"  [{elapsed:3d}s] Job not yet created...")

        except Exception as e:
            elapsed = int(time.time() - start)
            print(f"  [{elapsed:3d}s] Poll error: {e}")

        time.sleep(5)

    print(f"\nTimeout after {timeout}s")
    return None


def main():
    parser = argparse.ArgumentParser(
        description="Submit a job to the DAG orchestrator via Service Bus queue",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s echo_test '{"message": "hello", "count": 3}'
  %(prog)s fan_out_test '{"item_list": ["alpha", "bravo", "charlie"]}'
  %(prog)s echo_test '{"message": "test"}' --correlation-id trace-001 --poll
        """,
    )
    parser.add_argument("workflow_id", help="Workflow ID to execute")
    parser.add_argument("params", help="JSON input parameters")
    parser.add_argument(
        "--correlation-id", "-c",
        help="Correlation ID for tracking (auto-generated if not set)",
    )
    parser.add_argument(
        "--queue", "-q",
        default="dag-jobs",
        help="Service Bus queue name (default: dag-jobs)",
    )
    parser.add_argument(
        "--poll", "-p",
        action="store_true",
        help="Poll orchestrator for job completion",
    )
    parser.add_argument(
        "--orchestrator-url", "-u",
        default=os.environ.get(
            "ORCHESTRATOR_URL",
            "https://rmhdagmaster-gcfzd5bqfxc7g7cv.eastus-01.azurewebsites.net"
        ),
        help="Orchestrator base URL (for --poll)",
    )
    parser.add_argument(
        "--timeout", "-t",
        type=int,
        default=120,
        help="Poll timeout in seconds (default: 120)",
    )

    args = parser.parse_args()

    # Parse params
    try:
        input_params = json.loads(args.params)
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON params: {e}", file=sys.stderr)
        sys.exit(1)

    # Generate correlation ID if not provided
    correlation_id = args.correlation_id
    if not correlation_id:
        import uuid
        correlation_id = f"cli-{uuid.uuid4().hex[:8]}"

    # Check env
    has_conn_str = bool(os.environ.get("SERVICE_BUS_CONNECTION_STRING"))
    has_namespace = bool(os.environ.get("SERVICE_BUS_NAMESPACE") or os.environ.get("SERVICE_BUS_FQDN"))
    if not has_conn_str and not has_namespace:
        print(
            "ERROR: Set SERVICE_BUS_CONNECTION_STRING or SERVICE_BUS_NAMESPACE",
            file=sys.stderr,
        )
        sys.exit(1)

    # Submit
    print(f"Submitting job:")
    print(f"  workflow:       {args.workflow_id}")
    print(f"  params:         {json.dumps(input_params)}")
    print(f"  correlation_id: {correlation_id}")
    print(f"  queue:          {args.queue}")
    print()

    try:
        message_id = submit_job(
            workflow_id=args.workflow_id,
            input_params=input_params,
            correlation_id=correlation_id,
            queue_name=args.queue,
        )
        print(f"Submitted successfully!")
        print(f"  message_id:     {message_id}")
        print(f"  correlation_id: {correlation_id}")
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    # Poll if requested
    if args.poll:
        result = poll_status(
            correlation_id=correlation_id,
            orchestrator_url=args.orchestrator_url,
            timeout=args.timeout,
        )
        job_status = result.get("job", {}).get("status") if result else None
        if job_status == "completed":
            sys.exit(0)
        else:
            sys.exit(1)


if __name__ == "__main__":
    main()
