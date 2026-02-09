"""FastAPI application - REST API + HTML UI for the Lead Disposition System."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from lead_disposition.campaign_fill import CampaignFillEngine
from lead_disposition.core.config import Settings
from lead_disposition.core.db_factory import create_database
from lead_disposition.core.models import (
    CampaignFillRequest,
    Channel,
    DispositionStatus,
)
from lead_disposition.deconfliction import Deconfliction
from lead_disposition.importer import CSVImporter
from lead_disposition.providers.ai_ark import AIArkProvider
from lead_disposition.providers.clay import ClayProvider
from lead_disposition.providers.jina import JinaProvider
from lead_disposition.providers.spider import SpiderProvider
from lead_disposition.state_machine import TRANSITIONS, StateMachine, TransitionError
from lead_disposition.tam_tracker import TAMTracker
from lead_disposition.waterfall.engine import WaterfallEngine, WaterfallFillRequest

logger = logging.getLogger(__name__)

settings = Settings()
db = create_database(settings)

# Initialize providers
_providers = []
if settings.ai_ark_api_key:
    _providers.append(AIArkProvider(settings))
if settings.clay_webhook_url:
    _providers.append(ClayProvider(settings))
if settings.jina_api_key:
    _providers.append(JinaProvider(settings))
if settings.spider_api_key:
    _providers.append(SpiderProvider(settings))

waterfall = WaterfallEngine(db, _providers, settings)

TEMPLATES_DIR = Path(__file__).parent.parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Pre-compute serializable transition map for the UI
TRANSITION_MAP: dict[str, list[str]] = {
    status.value: [t.value for t in targets]
    for status, targets in TRANSITIONS.items()
}

ALL_STATUSES = [s.value for s in DispositionStatus]


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    yield
    for p in _providers:
        await p.close()
    await db.close()


app = FastAPI(title="Lead Disposition", version="0.2.0", lifespan=lifespan)


# =========================================================================
# HTML Page Routes
# =========================================================================


@app.get("/", response_class=HTMLResponse)
async def page_dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.get("/contacts", response_class=HTMLResponse)
async def page_contacts(request: Request):
    return templates.TemplateResponse("contacts.html", {
        "request": request,
        "statuses": ALL_STATUSES,
    })


@app.get("/contacts/{email}/{client_id}", response_class=HTMLResponse)
async def page_contact_detail(request: Request, email: str, client_id: str):
    contact = await db.get_contact(email, client_id)
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")
    allowed = TRANSITION_MAP.get(contact.disposition_status.value, [])
    return templates.TemplateResponse("contact_detail.html", {
        "request": request,
        "contact": contact,
        "allowed_transitions": allowed,
    })


@app.get("/campaign-fill", response_class=HTMLResponse)
async def page_campaign_fill(request: Request):
    return templates.TemplateResponse("campaign_fill.html", {"request": request})


@app.get("/import", response_class=HTMLResponse)
async def page_import(request: Request):
    return templates.TemplateResponse("import.html", {"request": request})


@app.get("/ownership", response_class=HTMLResponse)
async def page_ownership(request: Request):
    return templates.TemplateResponse("ownership.html", {"request": request})


# =========================================================================
# API: TAM
# =========================================================================


@app.get("/api/tam/health")
async def api_tam_health(client_id: str | None = Query(None)):
    tracker = TAMTracker(db, settings)
    health = await tracker.get_health(client_id)
    return health.model_dump(mode="json")


@app.post("/api/tam/snapshot")
async def api_tam_snapshot(client_id: str | None = Query(None)):
    tracker = TAMTracker(db, settings)
    health = await tracker.capture_snapshot(client_id)
    return health.model_dump(mode="json")


@app.get("/api/tam/trends")
async def api_tam_trends(
    client_id: str | None = Query(None),
    days: int = Query(30),
):
    tracker = TAMTracker(db, settings)
    return await tracker.get_trends(client_id, days)


# =========================================================================
# API: Contacts
# =========================================================================


@app.get("/api/contacts")
async def api_contacts(
    client_id: str | None = Query(None),
    status: str | None = Query(None),
    search: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    contacts, total = await db.list_contacts(client_id, status, search, limit, offset)
    return {
        "items": [c.model_dump(mode="json") for c in contacts],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@app.get("/api/contacts/{email}/{client_id}")
async def api_contact(email: str, client_id: str):
    contact = await db.get_contact(email, client_id)
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")
    return contact.model_dump(mode="json")


@app.get("/api/contacts/{email}/{client_id}/history")
async def api_contact_history(email: str, client_id: str):
    history = await db.get_contact_history(email, client_id)
    return {"items": history}


@app.post("/api/contacts/{email}/{client_id}/transition")
async def api_transition(
    email: str,
    client_id: str,
    new_status: str = Query(...),
    reason: str | None = Query(None),
):
    sm = StateMachine(db, settings)
    try:
        status_enum = DispositionStatus(new_status)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid status: {new_status}")
    try:
        await sm.transition(email, client_id, status_enum, reason=reason, triggered_by="ui")
    except TransitionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    contact = await db.get_contact(email, client_id)
    return contact.model_dump(mode="json") if contact else {"error": "not found"}


# =========================================================================
# API: Campaign Fill
# =========================================================================


@app.post("/api/campaign/fill")
async def api_campaign_fill(request: CampaignFillRequest):
    engine = CampaignFillEngine(db, settings)
    try:
        result = await engine.fill(request)
        return result.model_dump(mode="json")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# =========================================================================
# API: Import
# =========================================================================


@app.post("/api/import/csv")
async def api_import_csv(
    file: UploadFile = File(...),
    client_id: str = Form(...),
):
    content = (await file.read()).decode("utf-8-sig")
    importer = CSVImporter(db, settings)
    result = await importer.import_csv_string(content, client_id)
    return {
        "total_rows": result.total_rows,
        "imported": result.imported,
        "duplicates": result.duplicates,
        "skipped": result.skipped,
        "errors": result.errors,
    }


# =========================================================================
# API: Ownership
# =========================================================================


@app.get("/api/ownership")
async def api_ownership(client_id: str | None = Query(None)):
    companies = await db.list_owned_companies(client_id)
    return {"items": [c.model_dump(mode="json") for c in companies]}


@app.post("/api/ownership/{domain}/release")
async def api_release_ownership(domain: str):
    decon = Deconfliction(db, settings)
    ok = await decon.release_ownership(domain)
    if not ok:
        raise HTTPException(status_code=404, detail="Company not found or not owned")
    return {"success": True, "domain": domain}


@app.post("/api/ownership/{domain}/transfer")
async def api_transfer_ownership(domain: str, new_client_id: str = Query(...)):
    decon = Deconfliction(db, settings)
    ok = await decon.transfer_ownership(domain, new_client_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Company not found")
    return {"success": True, "domain": domain, "new_owner": new_client_id}


# =========================================================================
# API: Utilities
# =========================================================================


@app.get("/api/clients")
async def api_clients():
    clients = await db.get_distinct_clients()
    return {"items": clients}


@app.post("/api/maintenance/cooldowns")
async def api_maintenance_cooldowns():
    sm = StateMachine(db, settings)
    count = await sm.process_expired_cooldowns()
    return {"processed": count}


@app.post("/api/maintenance/stale")
async def api_maintenance_stale():
    sm = StateMachine(db, settings)
    count = await sm.process_stale_data()
    return {"processed": count}


@app.post("/api/maintenance/ownerships")
async def api_maintenance_ownerships():
    decon = Deconfliction(db, settings)
    count = await decon.process_expired_ownerships()
    return {"released": count}


# =========================================================================
# API: Waterfall Lead Pulling
# =========================================================================


@app.post("/api/waterfall/fill")
async def api_waterfall_fill(request: WaterfallFillRequest):
    """Execute waterfall fill: internal DB first, then external providers on shortfall."""
    try:
        result = await waterfall.fill_campaign(request)
        return result.model_dump(mode="json")
    except Exception as e:
        logger.exception("Waterfall fill failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/waterfall/providers")
async def api_waterfall_providers():
    """List configured providers and their health status."""
    providers_info = []
    for p in _providers:
        healthy = False
        try:
            healthy = await p.health_check()
        except Exception:
            pass
        providers_info.append({
            "name": p.provider_name,
            "priority": p.priority,
            "healthy": healthy,
        })
    return {
        "waterfall_enabled": settings.waterfall_enabled,
        "provider_order": settings.waterfall_provider_order,
        "providers": providers_info,
    }


@app.post("/api/waterfall/search-external")
async def api_waterfall_search_external(
    client_id: str = Query(...),
    industry: str | None = Query(None),
    title_keywords: str | None = Query(None),
    locations: str | None = Query(None),
    company_domains: str | None = Query(None),
    limit: int = Query(10, ge=1, le=100),
    provider: str | None = Query(None),
):
    """Search external providers without committing leads. Preview only."""
    from lead_disposition.providers.base import SearchCriteria

    criteria = SearchCriteria(
        client_id=client_id,
        industry=industry,
        job_titles=title_keywords.split(",") if title_keywords else [],
        locations=locations.split(",") if locations else [],
        company_domains=company_domains.split(",") if company_domains else [],
        limit=limit,
    )

    results: dict[str, Any] = {}
    target_providers = _providers
    if provider:
        target_providers = [p for p in _providers if p.provider_name == provider]
        if not target_providers:
            raise HTTPException(status_code=404, detail=f"Provider '{provider}' not configured")

    for p in target_providers:
        try:
            pr = await p.search_leads(criteria)
            results[p.provider_name] = {
                "leads": [lead.model_dump(mode="json") for lead in pr.leads],
                "total_found": pr.total_found,
                "credits_consumed": pr.credits_consumed,
                "errors": pr.errors,
            }
        except Exception as e:
            results[p.provider_name] = {"error": str(e)}

    return {"results": results}
