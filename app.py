"""
Zendesk Duplicate Ticket Auto-Merger
=====================================
A FastAPI service that receives Zendesk webhooks on ticket creation,
detects duplicate tickets from the same requester, and auto-merges them.

Matching logic:
  1. Same requester (by email/ID)
  2. Created within a configurable time window (default: 72 hours)
  3. Subject + description similarity above threshold (default: 0.6)

Requires:
  - ZENDESK_SUBDOMAIN: your Zendesk subdomain
  - ZENDESK_EMAIL: agent email for API auth
  - ZENDESK_API_TOKEN: Zendesk API token
  - WEBHOOK_SECRET: shared secret to validate incoming webhooks
"""

import os
import re
import hmac
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher

import httpx
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from pydantic import BaseModel
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    zendesk_subdomain: str
    zendesk_email: str
    zendesk_api_token: str
    webhook_secret: str = ""
    lookback_hours: int = 72
    similarity_threshold: float = 0.6
    min_description_length: int = 20
    merge_comment: str = "Auto-merged duplicate ticket #{source_id} from the same requester."
    log_level: str = "INFO"

    class Config:
        env_file = ".env"


settings = Settings()
logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO))
logger = logging.getLogger("zendesk-dedup")
app = FastAPI(title="Zendesk Duplicate Merger", version="1.0.0")

ZENDESK_BASE = f"https://{settings.zendesk_subdomain}.zendesk.com/api/v2"
AUTH = (f"{settings.zendesk_email}/token", settings.zendesk_api_token)


async def zendesk_get(path, params=None):
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{ZENDESK_BASE}{path}", auth=AUTH, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()


async def zendesk_post(path, json_body):
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{ZENDESK_BASE}{path}", auth=AUTH, json=json_body, timeout=30)
        resp.raise_for_status()
        return resp.json()


def normalize_text(text):
    if not text:
        return ""
    text = text.lower().strip()
    text = re.sub(r"(--\\s*\\n|on .* wrote:|from:.*|sent:.*)", "", text)
    text = re.sub(r"\\s+", " ", text)
    return text


def similarity(a, b):
    a_norm = normalize_text(a)
    b_norm = normalize_text(b)
    if not a_norm or not b_norm:
        return 0.0
    return SequenceMatcher(None, a_norm, b_norm).ratio()


def combined_similarity(subject_a, body_a, subject_b, body_b):
    subj_sim = similarity(subject_a, subject_b)
    body_sim = similarity(body_a, body_b)
    if subj_sim > 0.85:
        return 0.7 * subj_sim + 0.3 * body_sim
    return 0.5 * subj_sim + 0.5 * body_sim


async def find_duplicates(ticket):
    requester_id = ticket["requester_id"]
    ticket_id = ticket["id"]
    created_at = datetime.fromisoformat(ticket["created_at"].replace("Z", "+00:00"))
    lookback = created_at - timedelta(hours=settings.lookback_hours)
    query = f"type:ticket requester_id:{requester_id} created>{lookback.strftime('%Y-%m-%dT%H:%M:%SZ')} status<solved"
    try:
        result = await zendesk_get("/search.json", params={"query": query, "sort_by": "created_at", "sort_order": "desc"})
    except httpx.HTTPStatusError as e:
        logger.error(f"Zendesk search failed: {e}")
        return []
    candidates = result.get("results", [])
    new_subject = ticket.get("subject", "") or ""
    new_body = ticket.get("description", "") or ""
    duplicates = []
    for candidate in candidates:
        if candidate["id"] == ticket_id:
            continue
        score = combined_similarity(new_subject, new_body, candidate.get("subject", "") or "", candidate.get("description", "") or "")
        logger.info(f"Ticket #{ticket_id} vs #{candidate['id']}: similarity={score:.3f}")
        if score >= settings.similarity_threshold:
            duplicates.append({"ticket": candidate, "score": score})
    duplicates.sort(key=lambda x: x["score"], reverse=True)
    return duplicates


async def merge_ticket(source_id, target_id):
    logger.info(f"Merging ticket #{source_id} into #{target_id}")
    await zendesk_post(f"/tickets/{target_id}/merge.json", json_body={"ids": [source_id], "target_comment": settings.merge_comment.format(source_id=source_id), "source_comment": f"This ticket has been merged into ticket #{target_id}."})
    logger.info(f"Successfully merged #{source_id} -> #{target_id}")


async def process_new_ticket(ticket_id):
    try:
        result = await zendesk_get(f"/tickets/{ticket_id}.json")
        ticket = result["ticket"]
    except httpx.HTTPStatusError as e:
        logger.error(f"Failed to fetch ticket #{ticket_id}: {e}")
        return
    if ticket.get("status") in ("closed", "solved"):
        return
    duplicates = await find_duplicates(ticket)
    if not duplicates:
        logger.info(f"No duplicates found for ticket #{ticket_id}")
        return
    best = duplicates[0]
    logger.info(f"Duplicate detected: #{ticket_id} matches #{best['ticket']['id']} (score={best['score']:.3f})")
    await merge_ticket(source_id=ticket_id, target_id=best["ticket"]["id"])


def verify_webhook_signature(body, signature):
    if not settings.webhook_secret:
        return True
    expected = hmac.new(settings.webhook_secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "zendesk-dedup"}


@app.post("/webhook/ticket-created")
async def ticket_created(request: Request, background_tasks: BackgroundTasks):
    body = await request.body()
    signature = request.headers.get("x-zendesk-webhook-signature", "")
    if settings.webhook_secret and not verify_webhook_signature(body, signature):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")
    payload = await request.json()
    ticket_id = None
    if "ticket" in payload:
        ticket_id = payload["ticket"].get("id")
    elif "id" in payload:
        ticket_id = payload["id"]
    elif "ticket_id" in payload:
        ticket_id = payload["ticket_id"]
    if not ticket_id:
        raise HTTPException(status_code=400, detail="No ticket ID in payload")
    logger.info(f"Received webhook for ticket #{ticket_id}")
    background_tasks.add_task(process_new_ticket, ticket_id)
    return {"status": "accepted", "ticket_id": ticket_id}


@app.post("/manual/check/{ticket_id}")
async def manual_check(ticket_id: int):
    try:
        result = await zendesk_get(f"/tickets/{ticket_id}.json")
        ticket = result["ticket"]
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=404, detail=f"Ticket not found: {e}")
    duplicates = await find_duplicates(ticket)
    return {"ticket_id": ticket_id, "subject": ticket.get("subject"), "duplicates_found": len(duplicates), "matches": [{"ticket_id": d["ticket"]["id"], "subject": d["ticket"].get("subject"), "score": round(d["score"], 3)} for d in duplicates]}


@app.post("/manual/merge/{source_id}/{target_id}")
async def manual_merge(source_id: int, target_id: int):
    await merge_ticket(source_id=source_id, target_id=target_id)
    return {"status": "merged", "source": source_id, "target": target_id}
