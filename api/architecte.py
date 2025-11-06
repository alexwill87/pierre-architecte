#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Architecte API (archi.py)
- FastAPI service pour comparer/inspecter des bases Notion
- Règles de sécurité (token) compatibles "Aurel":
    * Si API_TOKEN est défini côté serveur : exiger un token côté client
      via le header `X-Token: <valeur>` ou via `Authorization: Bearer <valeur>`.
    * Si API_TOKEN est absent : ne pas bloquer (mode dev) mais logger un avertissement.
- Endpoints principaux:
    GET /healthcheck                      -> statut simple
    GET /architecte/analyse?db=fwk        -> renvoie le schéma de la DB
    GET /architecte/rows?db=fwk&limit=10  -> renvoie des lignes normalisées
    GET /architecte/compare?db=fwk&ref=FWK_DB_ID  -> diff de schémas
    GET /architecte/logtest               -> écrit un log (si LOGS_DB_ID + Notion)
    GET /debug/env_status                 -> état des variables d'env (masquées)
    GET /debug/routes                     -> liste des routes
    GET /                                 -> redirection vers /docs
"""
import os
import logging
from pathlib import Path
from typing import Literal, Optional, Dict, Any, List
from datetime import datetime, timezone

import requests
from fastapi import FastAPI, Query, HTTPException, Header, Depends
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from notion_client import Client
from dotenv import load_dotenv, find_dotenv

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("architecte")

# ---------------------------------------------------------------------
# .env loading (forcer le .env à la racine du projet si présent)
# ---------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent
# On autorise un .env à la racine du repo (ex: .../)
env_path = PROJECT_ROOT / ".env"
_found_env = find_dotenv(filename=str(env_path), raise_error_if_not_found=False)
_loaded = load_dotenv(dotenv_path=str(env_path)) if env_path.exists() else load_dotenv()
logger.info("dotenv path=%s exists=%s loaded=%s", env_path, env_path.exists(), bool(_loaded))

# ---------------------------------------------------------------------
# FastAPI app + CORS
# ---------------------------------------------------------------------
app = FastAPI(title="Architecte API", version="1.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://pierre-architecte.vercel.app",
        "http://localhost:3000",
        "*",  # Optionnel: à restreindre si besoin
    ],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------
# Sécurité
# ---------------------------------------------------------------------
API_TOKEN = os.getenv("API_TOKEN")

def _read_token_from_headers(x_token: Optional[str], authorization: Optional[str]) -> Optional[str]:
    # Headers HTTP sont case-insensitive; FastAPI normalise en minuscules
    if x_token:
        return x_token
    if authorization and authorization.lower().startswith("bearer "):
        return authorization.split(" ", 1)[1].strip()
    return None

def require_token(
    x_token: Optional[str] = Header(None, alias="X-Token"),
    authorization: Optional[str] = Header(None, alias="Authorization"),
):
    """
    Règle "Aurel":
      - si API_TOKEN est défini côté serveur : exiger un token
      - sinon : ne pas bloquer (mode dev), mais logguer un warning
    """
    if not API_TOKEN:
        logger.warning("API_TOKEN not set — token check skipped (dev mode)")
        return

    token = _read_token_from_headers(x_token, authorization)
    if token is None:
        raise HTTPException(status_code=401, detail="X-Token manquant")
    if token != API_TOKEN:
        raise HTTPException(status_code=403, detail="X-Token invalide")

# ---------------------------------------------------------------------
# Notion config
# ---------------------------------------------------------------------
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
if not NOTION_TOKEN:
    logger.warning("⚠️ NOTION_TOKEN missing; Notion calls will fail.")
notion = Client(auth=NOTION_TOKEN) if NOTION_TOKEN else None
NOTION_API_VERSION = os.getenv("NOTION_VERSION", "2022-06-28")

DBS: Dict[str, Optional[str]] = {
    "fwk": os.getenv("FWK_DB_ID"),
    "agent": os.getenv("AGENT_DB_ID"),
    "module": os.getenv("MODULE_DB_ID"),
    "vars": os.getenv("VARS_DB_ID"),
    "validations": os.getenv("VALIDATIONS_DB_ID"),
    "outputs": os.getenv("OUTPUTS_DB_ID"),
    "inputs": os.getenv("INPUTS_DB_ID"),
    "tests": os.getenv("TESTS_DB_ID"),
    "logs": os.getenv("LOGS_DB_ID"),
    "kpis": os.getenv("KPIS_DB_ID"),
}

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _mask(val: Optional[str]) -> Optional[str]:
    if not val:
        return None
    s = str(val)
    return (s[:4] + "..." + s[-4:]) if len(s) > 8 else s

def _env_status(keys: List[str]) -> Dict[str, Dict[str, Any]]:
    return {k: {"present": bool(os.getenv(k)), "masked": _mask(os.getenv(k))} for k in keys}

def _ensure_db(db_key: str) -> str:
    dbid = DBS.get(db_key)
    if not dbid:
        logger.error("Database missing or not configured: %s", db_key)
        raise HTTPException(status_code=400, detail=f"Base inconnue ou non configurée : {db_key}")
    return dbid

def _first_title_prop(properties: dict) -> Optional[str]:
    for name, p in properties.items():
        if p.get("type") == "title":
            return name
    return None

def _schema_of(database_id: str) -> Dict[str, str]:
    """Retourne {nom_propriété: type} pour une base Notion donnée. SDK puis fallback REST."""
    # SDK
    if notion:
        meta = notion.databases.retrieve(database_id=database_id)
    else:
        token = NOTION_TOKEN
        if not token:
            raise RuntimeError("NOTION_TOKEN manquant pour lire le schéma Notion")
        url = f"https://api.notion.com/v1/databases/{database_id}"
        headers = {"Authorization": f"Bearer {token}", "Notion-Version": NOTION_API_VERSION}
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        meta = resp.json()
    props = meta.get("properties", {}) if isinstance(meta, dict) else {}
    return {name: p.get("type", "unknown") for name, p in props.items()}

def _query_database(database_id: str, page_size: int = 10) -> Dict[str, Any]:
    """Query une DB Notion avec SDK si possible, sinon REST POST /query."""
    if notion:
        try:
            return notion.databases.query(database_id=database_id, page_size=page_size)
        except TypeError:
            # Compat: certains SDK n'acceptent pas page_size
            return notion.databases.query(database_id=database_id)
    token = NOTION_TOKEN
    if not token:
        raise RuntimeError("No NOTION_TOKEN available for Notion REST fallback")
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_API_VERSION,
        "Content-Type": "application/json",
    }
    payload = {"page_size": page_size}
    resp = requests.post(url, json=payload, headers=headers, timeout=15)
    resp.raise_for_status()
    return resp.json()

def _create_log_entry(message: str, level: str = "INFO", extra: Optional[Dict[str, Any]] = None) -> Optional[dict]:
    logs_db = DBS.get("logs")
    if not logs_db:
        logger.warning("LOGS_DB_ID not configured; skipping log write")
        return None
    if not (notion or NOTION_TOKEN):
        logger.error("No Notion client/token available; cannot write log")
        return None

    # Lire le schéma pour détecter le champ title et props disponibles
    try:
        if notion:
            meta = notion.databases.retrieve(database_id=logs_db)
        else:
            url = f"https://api.notion.com/v1/databases/{logs_db}"
            headers = {"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": NOTION_API_VERSION}
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            meta = resp.json()
        props_meta = meta.get("properties", {})
        title_prop = _first_title_prop(props_meta) or "Name"
    except Exception as e:
        logger.exception("Cannot read Logs DB metadata: %s", e)
        return None

    props: Dict[str, Any] = {title_prop: {"title": [{"text": {"content": message}}]}}
    if "Type" in props_meta and level:
        props["Type"] = {"select": {"name": level}}
    if "Meta" in props_meta and extra:
        props["Meta"] = {"rich_text": [{"text": {"content": str(extra)}}]}
    if "Date du changement" in props_meta:
        props["Date du changement"] = {"date": {"start": datetime.now(timezone.utc).isoformat()}}

    page_payload = {"parent": {"database_id": logs_db}, "properties": props}

    try:
        if notion:
            res = notion.pages.create(**page_payload)
        else:
            url = "https://api.notion.com/v1/pages"
            headers = {
                "Authorization": f"Bearer {NOTION_TOKEN}",
                "Notion-Version": NOTION_API_VERSION,
                "Content-Type": "application/json",
            }
            resp = requests.post(url, json=page_payload, headers=headers, timeout=15)
            resp.raise_for_status()
            res = resp.json()
        logger.info("Wrote log to Notion id=%s", _mask(res.get("id")))
        return res
    except Exception as e:
        logger.exception("Failed to write log to Notion: %s", e)
        return None

# ---------------------------------------------------------------------
# Types & endpoints
# ---------------------------------------------------------------------
AllowedDB = Literal["fwk", "agent", "module", "vars", "validations", "outputs", "inputs", "tests", "logs", "kpis"]

@app.get("/healthcheck")
def healthcheck():
    return {"status": "ok"}

@app.get("/architecte/analyse")
def analyse_base(db: AllowedDB = Query("fwk", description="Nom court de la base")):
    try:
        database_id = _ensure_db(db)
        logger.info("Retrieving schema for DB %s id=%s", db, _mask(database_id))
        schema = _schema_of(database_id)
        return {"status": "ok", "base": db, "schema": schema}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error analysing DB %s: %s", db, e)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/architecte/rows")
def get_rows(db: AllowedDB = Query("fwk"), limit: int = Query(3, ge=1, le=100)):
    try:
        dbid = _ensure_db(db)
        logger.info("Query rows base=%s limit=%s", db, limit)
        res = _query_database(database_id=dbid, page_size=limit)
        results = res.get("results") if isinstance(res, dict) and "results" in res else (res if isinstance(res, list) else [])
        # Récupérer le titre
        meta = {}
        try:
            if notion:
                meta = notion.databases.retrieve(database_id=dbid)
            else:
                token = NOTION_TOKEN
                if token:
                    url = f"https://api.notion.com/v1/databases/{dbid}"
                    headers = {"Authorization": f"Bearer {token}", "Notion-Version": NOTION_API_VERSION}
                    resp = requests.get(url, headers=headers, timeout=15)
                    resp.raise_for_status()
                    meta = resp.json()
        except Exception:
            logger.debug("Could not retrieve DB meta for title prop; continuing with defaults")
            meta = {}
        title_prop = _first_title_prop(meta.get("properties", {})) or "title"
        items: List[Dict[str, Any]] = []
        for r in results:
            props = r.get("properties", {}) if isinstance(r, dict) else {}
            title = ""
            tp = props.get(title_prop)
            if tp and tp.get("type") == "title":
                title_parts = tp.get("title", [])
                title = "".join(part.get("plain_text", "") for part in title_parts)
            items.append({"id": r.get("id"), "title": title, "properties": props})
        return {"status": "ok", "base": db, "count": len(items), "items": items}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error fetching rows for %s: %s", db, e)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/architecte/compare")
def compare(
    db: AllowedDB = Query("fwk", description="Base courante"),
    ref: str = Query("FWK_DB_ID", description="Nom de la variable d'env de la base de référence"),
    _: None = Depends(require_token)
):
    # Paramètres requis ? -> si pas fournis, 400 (pas 401) pour éviter confusion
    if not db or not ref:
        raise HTTPException(status_code=400, detail="Paramètres db et ref requis")

    curr_id = _ensure_db(db)
    ref_id = os.getenv(ref)
    if not ref_id:
        raise HTTPException(status_code=400, detail=f"Référence absente (env var '{ref}')")

    curr = _schema_of(curr_id)
    target = _schema_of(ref_id)

    missing = [k for k in target if k not in curr]
    extra = [k for k in curr if k not in target]
    type_mismatch = [k for k in target if k in curr and target[k] != curr[k]]

    _create_log_entry(f"Compare {db} → {ref}", "INFO", {"missing": missing[:5], "extra": extra[:5], "types": type_mismatch[:5]})
    return {
        "status": "ok",
        "base": db,
        "ref_env": ref,
        "missing": missing,
        "extra": extra,
        "type_mismatch": type_mismatch,
    }

@app.get("/architecte/logtest")
def log_test(_: None = Depends(require_token)):
    _create_log_entry("Test manuel via /logtest", "INFO", {"source": "manual"})
    return {"status": "ok", "message": "log envoyé"}

@app.post("/architecte/edit")
def edit_entry(
    db: AllowedDB = Query(..., description="Nom court de la base Notion (ex: agent, module, logs)"),
    data: dict = None,
    _: None = Depends(require_token)
):
    """
    Crée une nouvelle entrée dans une base Notion donnée.
    Exemple d'appel :
    POST /architecte/edit?db=agent
    Body JSON = {"Nom": {"title": [{"text": {"content": "Mira"}}]}, "Statut": {"select": {"name": "Actif"}}}
    """
    try:
        dbid = _ensure_db(db)
        if not notion:
            raise HTTPException(status_code=500, detail="Notion client non configuré")
        notion.pages.create(
            parent={"database_id": dbid},
            properties=data
        )
        _create_log_entry(f"Nouvelle entrée ajoutée à {db}", "INFO", {"agent": "Aurel", "target": db})
        return {"status": "ok", "message": f"Entrée créée dans {db}"}
    except Exception as e:
        logger.exception("Erreur création entrée Notion : %s", e)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/aurel")
async def aurel_router(request: Request):
    body = await request.json()
    target = body.get("target")
    payload = body.get("payload")
    if target == "edit":
        # redirige vers la fonction existante /architecte/edit
        db = payload.get("db")
        data = payload.get("data")
        return await edit_entry(db=db, data=data)
    elif target == "log":
        # redirige vers /logtest ou création de log spécifique
        return log_test()
    else:
        return {"status": "error", "message": "Unknown target"}

@app.get("/debug/env_status")
def debug_env_status():
    # Clés "globales"
    env_keys = [
        "NOTION_TOKEN", "API_TOKEN",
        "FWK_DB_ID","AGENT_DB_ID","MODULE_DB_ID","VARS_DB_ID",
        "VALIDATIONS_DB_ID","OUTPUTS_DB_ID","INPUTS_DB_ID","TESTS_DB_ID","LOGS_DB_ID","KPIS_DB_ID"
    ]
    return {
        "dotenv_path": str(env_path),
        "loaded": bool(_loaded),
        "env": _env_status(env_keys)
    }

@app.get("/debug/routes")
def debug_routes():
    return {"routes": [{"path": r.path, "name": r.name, "methods": list(getattr(r, "methods", []))} for r in app.routes]}

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs")

# Local runner
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000, reload=True)
