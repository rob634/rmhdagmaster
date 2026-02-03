"""
UI Routes - Jinja2 template rendering for dashboard.
"""
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import httpx
import psutil
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from __version__ import __version__, BUILD_DATE, EPOCH
from ui.terminology import Terminology
from core.contracts import JobStatus, NodeStatus
from core.models.events import EventType, EventStatus

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ui", tags=["ui"])

# Initialize templates
templates = Jinja2Templates(directory="templates")

# Default terminology for DAG orchestrator
TERMS = Terminology(
    mode_display="DAG Orchestrator",
    workflow_singular="Workflow",
    workflow_plural="Workflows",
    job_singular="Job",
    job_plural="Jobs",
    step_singular="Node",
    step_plural="Nodes",
)

# ============================================================================
# SERVICE REFERENCES (set by main.py)
# ============================================================================

_job_service = None
_node_service = None
_workflow_service = None
_event_service = None
_orchestrator = None
_pool = None
_start_time = datetime.now(timezone.utc)


def set_ui_services(job_service, node_service, workflow_service, orchestrator, pool, event_service=None):
    """Set service instances for UI routes."""
    global _job_service, _node_service, _workflow_service, _event_service, _orchestrator, _pool
    _job_service = job_service
    _node_service = node_service
    _workflow_service = workflow_service
    _event_service = event_service
    _orchestrator = orchestrator
    _pool = pool


# ============================================================================
# HELPERS
# ============================================================================

def get_base_context(request: Request, nav_active: str = "") -> dict:
    """Build base context for all templates."""
    return {
        "request": request,
        "nav_active": nav_active,
        "terms": TERMS,
        "orchestrator_running": _orchestrator.is_running if _orchestrator else False,
    }


def format_uptime(start: datetime) -> str:
    """Format uptime as human-readable string."""
    delta = datetime.now(timezone.utc) - start
    hours, remainder = divmod(int(delta.total_seconds()), 3600)
    minutes, _ = divmod(remainder, 60)
    return f"{hours}h {minutes}m"


# ============================================================================
# ROUTES
# ============================================================================

@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Render the main dashboard."""
    context = get_base_context(request, "dashboard")

    # Fetch stats from database
    stats = {
        "total_jobs": 0,
        "running_jobs": 0,
        "completed_jobs": 0,
        "failed_jobs": 0,
    }

    recent_jobs = []
    workflows = []

    if _pool and _job_service:
        try:
            from repositories import JobRepository
            repo = JobRepository(_pool)

            # Get counts by status
            for status in [JobStatus.RUNNING, JobStatus.COMPLETED, JobStatus.FAILED]:
                jobs = await repo.list_by_status(status, limit=1000)
                if status == JobStatus.RUNNING:
                    stats["running_jobs"] = len(jobs)
                elif status == JobStatus.COMPLETED:
                    stats["completed_jobs"] = len(jobs)
                elif status == JobStatus.FAILED:
                    stats["failed_jobs"] = len(jobs)

            # Get recent jobs
            all_jobs = await _job_service.list_active_jobs(limit=10)
            if not all_jobs:
                all_jobs = await repo.list_by_status(JobStatus.COMPLETED, limit=10)

            for job in all_jobs[:5]:
                # Get node count for this job
                from repositories import NodeRepository
                node_repo = NodeRepository(_pool)
                nodes = await node_repo.get_all_for_job(job.job_id)
                completed = sum(1 for n in nodes if n.status == NodeStatus.COMPLETED)

                recent_jobs.append({
                    "job_id": job.job_id,
                    "workflow_id": job.workflow_id,
                    "status": job.status.value,
                    "total_nodes": len(nodes),
                    "completed_nodes": completed,
                    "created_at": job.created_at.strftime("%Y-%m-%d %H:%M") if job.created_at else "-",
                })

            stats["total_jobs"] = stats["running_jobs"] + stats["completed_jobs"] + stats["failed_jobs"]

        except Exception as e:
            logger.warning(f"Error fetching dashboard stats: {e}")

    # Get workflows
    if _workflow_service:
        try:
            wf_list = _workflow_service.list_all()
            workflows = [
                {
                    "id": wf.workflow_id,
                    "description": wf.description,
                    "node_count": len(wf.nodes),
                    "version": wf.version,
                }
                for wf in wf_list
            ]
        except Exception as e:
            logger.warning(f"Error fetching workflows: {e}")

    context["stats"] = stats
    context["recent_jobs"] = recent_jobs
    context["workflows"] = workflows

    return templates.TemplateResponse("dashboard.html", context)


@router.get("/health", response_class=HTMLResponse)
async def health_page(request: Request):
    """Render the health monitoring page."""
    context = get_base_context(request, "health")

    # Database health
    db_connected = False
    db_latency = None
    if _pool:
        try:
            import time
            start = time.time()
            async with _pool.connection() as conn:
                await conn.execute("SELECT 1")
            db_latency = int((time.time() - start) * 1000)
            db_connected = True
        except Exception as e:
            logger.warning(f"Database health check failed: {e}")

    # Messaging health
    sb_connected = False
    messages_sent = 0
    try:
        from messaging import get_publisher
        publisher = await get_publisher()
        sb_connected = publisher._client is not None
    except Exception:
        pass

    # Worker health check
    worker_url = os.environ.get(
        "WORKER_HEALTH_URL",
        "https://rmhdagworker-fedshwfme6drd6gq.eastus-01.azurewebsites.net"
    )
    worker_healthy = False
    worker_status = "Unknown"
    worker_latency = None
    worker_version = None
    worker_queue = None

    try:
        import time
        start = time.time()
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{worker_url}/health")
            worker_latency = int((time.time() - start) * 1000)
            if resp.status_code == 200:
                worker_healthy = True
                worker_status = "Healthy"
                data = resp.json()
                worker_version = data.get("version", "-")
                worker_queue = data.get("queue", data.get("worker_queue", "-"))
            else:
                worker_status = f"Unhealthy ({resp.status_code})"
    except httpx.ConnectError:
        worker_status = "Connection Failed"
    except httpx.TimeoutException:
        worker_status = "Timeout"
    except Exception as e:
        worker_status = f"Error: {str(e)[:30]}"
        logger.warning(f"Worker health check failed: {e}")

    # System resources
    memory = psutil.virtual_memory()
    cpu = psutil.cpu_percent(interval=0.1)

    # Orchestrator stats
    orch_stats = _orchestrator.stats if _orchestrator else {}

    context["health"] = {
        "orchestrator": {
            "running": _orchestrator.is_running if _orchestrator else False,
            "loop_interval_seconds": _orchestrator.poll_interval if _orchestrator else 5,
            "last_poll": datetime.now(timezone.utc).strftime("%H:%M:%S"),
            "cycles": orch_stats.get("cycles", 0),
        },
        "database": {
            "connected": db_connected,
            "pool_size": _pool.max_size if _pool else 0,
            "active_connections": _pool.get_stats().get("pool_size", 0) if _pool and hasattr(_pool, "get_stats") else "-",
            "latency_ms": db_latency if db_latency else "-",
        },
        "messaging": {
            "connected": sb_connected,
            "queue_name": "dag-worker-tasks",
            "messages_sent": orch_stats.get("tasks_dispatched", 0),
            "send_errors": 0,
        },
        "worker": {
            "healthy": worker_healthy,
            "status": worker_status,
            "url": worker_url,
            "latency_ms": worker_latency if worker_latency else "-",
            "version": worker_version if worker_version else "-",
            "queue": worker_queue if worker_queue else "-",
        },
        "system": {
            "memory_percent": round(memory.percent, 1),
            "cpu_percent": round(cpu, 1),
            "uptime": format_uptime(_start_time),
            "version": __version__,
            "build_date": BUILD_DATE,
            "epoch": EPOCH,
        },
        "workflows": [],
    }

    # Get loaded workflows
    if _workflow_service:
        try:
            wf_list = _workflow_service.list_all()
            context["health"]["workflows"] = [
                {
                    "id": wf.workflow_id,
                    "valid": True,
                    "node_count": len(wf.nodes),
                    "loaded_at": "-",
                }
                for wf in wf_list
            ]
        except Exception:
            pass

    return templates.TemplateResponse("health.html", context)


@router.get("/jobs", response_class=HTMLResponse)
async def jobs_list(
    request: Request,
    status: Optional[str] = Query(None),
    workflow_id: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
):
    """Render the jobs listing page."""
    context = get_base_context(request, "jobs")

    # Filters
    context["filters"] = {
        "status": status or "",
        "workflow_id": workflow_id or "",
    }

    # Get workflows for filter dropdown
    workflows = []
    if _workflow_service:
        try:
            wf_list = _workflow_service.list_all()
            workflows = [{"id": wf.workflow_id} for wf in wf_list]
        except Exception:
            pass
    context["workflows"] = workflows

    # Fetch jobs
    jobs = []
    total = 0

    if _pool:
        try:
            from repositories import JobRepository, NodeRepository
            job_repo = JobRepository(_pool)
            node_repo = NodeRepository(_pool)

            # Apply status filter
            if status:
                try:
                    status_enum = JobStatus(status)
                    job_list = await job_repo.list_by_status(status_enum, limit=per_page * page)
                except ValueError:
                    job_list = []
            else:
                # Get all recent jobs regardless of status
                job_list = await job_repo.list_recent(limit=per_page * page)

            # Apply workflow filter
            if workflow_id:
                job_list = [j for j in job_list if j.workflow_id == workflow_id]

            total = len(job_list)

            # Paginate
            start = (page - 1) * per_page
            end = start + per_page
            page_jobs = job_list[start:end]

            for job in page_jobs:
                nodes = await node_repo.get_all_for_job(job.job_id)
                completed = sum(1 for n in nodes if n.status == NodeStatus.COMPLETED)

                # Calculate duration
                duration_ms = None
                if job.completed_at and job.started_at:
                    duration_ms = int((job.completed_at - job.started_at).total_seconds() * 1000)

                jobs.append({
                    "job_id": job.job_id,
                    "workflow_id": job.workflow_id,
                    "status": job.status.value,
                    "total_nodes": len(nodes),
                    "completed_nodes": completed,
                    "created_at": job.created_at.strftime("%Y-%m-%d %H:%M") if job.created_at else "-",
                    "duration_ms": duration_ms,
                })

        except Exception as e:
            logger.warning(f"Error fetching jobs: {e}")

    context["jobs"] = jobs
    context["pagination"] = {
        "page": page,
        "per_page": per_page,
        "total": total,
        "total_pages": (total + per_page - 1) // per_page if total > 0 else 0,
    }

    return templates.TemplateResponse("jobs.html", context)


@router.get("/jobs/{job_id}", response_class=HTMLResponse)
async def job_detail(request: Request, job_id: str):
    """Render the job detail page."""
    context = get_base_context(request, "jobs")

    job_data = {
        "job_id": job_id,
        "workflow_id": "unknown",
        "status": "pending",
        "created_at": "-",
        "input_params": {},
        "entity_id": None,
    }
    nodes = []
    tasks = []

    if _pool and _job_service:
        try:
            result = await _job_service.get_job_with_nodes(job_id)

            if result:
                job = result["job"]
                node_states = result["nodes"]

                job_data = {
                    "job_id": job.job_id,
                    "workflow_id": job.workflow_id,
                    "status": job.status.value,
                    "created_at": job.created_at.strftime("%Y-%m-%d %H:%M:%S") if job.created_at else "-",
                    "input_params": job.input_params or {},
                    "entity_id": job.correlation_id,
                }

                # Get workflow for handler info
                workflow = _workflow_service.get(job.workflow_id) if _workflow_service else None

                for node in node_states:
                    # Get handler from workflow definition
                    handler = None
                    depends_on = []
                    if workflow:
                        try:
                            node_def = workflow.get_node(node.node_id)
                            handler = node_def.handler
                            if node_def.next:
                                # This node's "next" doesn't tell us dependencies
                                # We need to find nodes that point TO this node
                                pass
                        except KeyError:
                            pass

                    # Calculate duration
                    duration_ms = None
                    if node.completed_at and node.started_at:
                        duration_ms = int((node.completed_at - node.started_at).total_seconds() * 1000)

                    nodes.append({
                        "node_id": node.node_id,
                        "status": node.status.value,
                        "handler": handler,
                        "depends_on": depends_on,
                        "started_at": node.started_at.strftime("%H:%M:%S") if node.started_at else None,
                        "duration_ms": duration_ms,
                        "output": node.output,
                        "error_message": node.error_message,
                    })

                # Get task results
                from repositories import TaskResultRepository
                task_repo = TaskResultRepository(_pool)
                task_results = await task_repo.get_for_job(job_id)

                for task in task_results:
                    tasks.append({
                        "task_id": task.task_id,
                        "node_id": task.node_id,
                        "status": task.status.value,
                        "worker_id": task.worker_id,
                        "execution_duration_ms": task.execution_duration_ms,
                        "completed_at": task.reported_at.strftime("%H:%M:%S") if task.reported_at else "-",
                    })

        except Exception as e:
            logger.warning(f"Error fetching job detail: {e}")

    context["job"] = job_data
    context["nodes"] = nodes
    context["tasks"] = tasks

    return templates.TemplateResponse("job_detail.html", context)


@router.get("/jobs/{job_id}/timeline", response_class=HTMLResponse)
async def job_timeline(
    request: Request,
    job_id: str,
    event_type: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    node_id: Optional[str] = Query(None),
):
    """Render the job timeline page."""
    context = get_base_context(request, "jobs")

    # Filters
    context["filters"] = {
        "event_type": event_type or "",
        "status": status or "",
        "node_id": node_id or "",
    }

    job_data = {
        "job_id": job_id,
        "workflow_id": "unknown",
        "status": "pending",
        "created_at": "-",
    }
    events = []
    node_ids = set()
    stats = {"total": 0, "success": 0, "warning": 0, "failure": 0, "info": 0}

    if _pool and _event_service:
        try:
            # Get job info
            job = await _job_service.get_job(job_id) if _job_service else None
            if job:
                job_data = {
                    "job_id": job.job_id,
                    "workflow_id": job.workflow_id,
                    "status": job.status.value,
                    "created_at": job.created_at.strftime("%Y-%m-%d %H:%M:%S") if job.created_at else "-",
                }

            # Get all events for this job
            all_events = await _event_service.get_job_timeline(job_id, limit=500)

            # Collect node_ids for filter dropdown
            for event in all_events:
                if event.node_id:
                    node_ids.add(event.node_id)

            # Calculate stats from all events
            for event in all_events:
                stats["total"] += 1
                if event.event_status == EventStatus.SUCCESS:
                    stats["success"] += 1
                elif event.event_status == EventStatus.WARNING:
                    stats["warning"] += 1
                elif event.event_status == EventStatus.FAILURE:
                    stats["failure"] += 1
                else:
                    stats["info"] += 1

            # Apply filters
            filtered_events = all_events
            if event_type:
                try:
                    et = EventType(event_type)
                    filtered_events = [e for e in filtered_events if e.event_type == et]
                except ValueError:
                    pass
            if status:
                try:
                    es = EventStatus(status)
                    filtered_events = [e for e in filtered_events if e.event_status == es]
                except ValueError:
                    pass
            if node_id:
                filtered_events = [e for e in filtered_events if e.node_id == node_id]

            events = filtered_events

        except Exception as e:
            logger.warning(f"Error fetching timeline for job {job_id}: {e}")

    context["job"] = job_data
    context["events"] = events
    context["node_ids"] = sorted(node_ids)
    context["stats"] = stats

    return templates.TemplateResponse("timeline.html", context)


@router.get("/workflows", response_class=HTMLResponse)
async def workflows_list(request: Request):
    """Render the workflows listing page."""
    context = get_base_context(request, "workflows")

    workflows = []
    workflows_dir = "./workflows/"
    if _workflow_service:
        try:
            workflows = _workflow_service.list_all()
            workflows_dir = str(_workflow_service.workflows_dir)
        except Exception as e:
            logger.warning(f"Error fetching workflows: {e}")

    context["workflows"] = workflows
    context["workflows_dir"] = workflows_dir
    return templates.TemplateResponse("workflows.html", context)


@router.get("/workflows/{workflow_id}", response_class=HTMLResponse)
async def workflow_detail(request: Request, workflow_id: str):
    """Render the workflow detail page."""
    context = get_base_context(request, "workflows")

    workflow = None
    if _workflow_service:
        try:
            workflow = _workflow_service.get(workflow_id)
        except Exception as e:
            logger.warning(f"Error fetching workflow {workflow_id}: {e}")

    if workflow is None:
        # Return 404-style page
        context["error"] = f"Workflow not found: {workflow_id}"
        return templates.TemplateResponse("workflows.html", context)

    context["workflow"] = workflow
    return templates.TemplateResponse("workflow_detail.html", context)


@router.get("/nodes", response_class=HTMLResponse)
async def nodes_monitor(
    request: Request,
    status: Optional[str] = Query(None),
    show: str = Query("all"),
):
    """Render the nodes monitoring page."""
    context = get_base_context(request, "nodes")

    # Filters
    context["filters"] = {
        "status": status or "",
        "show": show,
    }

    nodes = []
    stats = {
        "total": 0,
        "running": 0,
        "pending": 0,
        "failed": 0,
    }

    if _pool:
        try:
            from repositories import NodeRepository
            node_repo = NodeRepository(_pool)

            # Get status counts
            status_counts = await node_repo.get_status_counts()
            stats["total"] = sum(status_counts.values())
            stats["running"] = status_counts.get("running", 0) + status_counts.get("dispatched", 0)
            stats["pending"] = status_counts.get("pending", 0) + status_counts.get("ready", 0)
            stats["failed"] = status_counts.get("failed", 0)

            # Get nodes based on filters
            if show == "active":
                nodes = await node_repo.list_active(limit=100)
            elif status:
                try:
                    status_enum = NodeStatus(status)
                    nodes = await node_repo.list_by_status(status_enum, limit=100)
                except ValueError:
                    nodes = []
            else:
                nodes = await node_repo.list_by_status(limit=100)

            # Enrich with handler info from workflow
            if _workflow_service:
                for node in nodes:
                    # Try to get handler from workflow
                    # First we need job info to get workflow_id
                    pass  # Handler lookup would require job->workflow mapping

        except Exception as e:
            logger.warning(f"Error fetching nodes: {e}")

    context["nodes"] = nodes
    context["stats"] = stats
    return templates.TemplateResponse("nodes.html", context)
