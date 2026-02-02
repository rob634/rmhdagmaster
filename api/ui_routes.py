"""
UI Routes - Jinja2 template rendering for dashboard.
"""
import logging
from datetime import datetime, timezone
from typing import Optional

import psutil
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from ui.terminology import Terminology
from core.contracts import JobStatus, NodeStatus

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
_orchestrator = None
_pool = None
_start_time = datetime.now(timezone.utc)


def set_ui_services(job_service, node_service, workflow_service, orchestrator, pool):
    """Set service instances for UI routes."""
    global _job_service, _node_service, _workflow_service, _orchestrator, _pool
    _job_service = job_service
    _node_service = node_service
    _workflow_service = workflow_service
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
        "system": {
            "memory_percent": round(memory.percent, 1),
            "cpu_percent": round(cpu, 1),
            "uptime": format_uptime(_start_time),
            "version": "0.1.0",
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
                # Get all active jobs, then completed
                job_list = await _job_service.list_active_jobs(limit=per_page * page) if _job_service else []
                if not job_list:
                    job_list = await job_repo.list_by_status(JobStatus.COMPLETED, limit=per_page * page)

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


@router.get("/workflows", response_class=HTMLResponse)
async def workflows_list(request: Request):
    """Render the workflows listing page."""
    context = get_base_context(request, "workflows")

    workflows = []
    if _workflow_service:
        try:
            wf_list = _workflow_service.list_all()
            workflows = [
                {
                    "id": wf.workflow_id,
                    "name": wf.name,
                    "description": wf.description,
                    "node_count": len(wf.nodes),
                    "version": wf.version,
                }
                for wf in wf_list
            ]
        except Exception:
            pass

    context["workflows"] = workflows
    return templates.TemplateResponse("dashboard.html", context)


@router.get("/nodes", response_class=HTMLResponse)
async def nodes_monitor(request: Request):
    """Render the nodes monitoring page."""
    context = get_base_context(request, "nodes")
    return templates.TemplateResponse("dashboard.html", context)
