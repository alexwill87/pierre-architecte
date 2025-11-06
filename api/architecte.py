import os
import logging
from pathlib import Path
from typing import Literal, Optional, Dict, Any, List

import requests
from fastapi import FastAPI, Query, HTTPException, Depends, Header
from fastapi.responses import RedirectResponse
from notion_client import Client
from dotenv import load_dotenv, find_dotenv

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("architecte")

# --- Load .env (force root .env) ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
env_path = PROJECT_ROOT / ".env"
_found_env = find_dotenv(filename=str(env_path), raise_error_if_not_found=False)
_loaded = load_dotenv(dotenv_path=str(env_path)) if env_path.exists() else load_dotenv()
logger.info("dotenv path=%s exists=%s loaded=%s", env_path, env_path.exists(), bool(_loaded))

# --- Application ---
app = FastAPI(title="Architecte API")

from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://pierre-architecte.vercel.app",  # ton front déployé
        "http://localhost:3000",                 # pour le front local (React, Next.js, etc.)
    ],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# --- Env & security ---
API_TOKEN = os.getenv("API_TOKEN")  # récupéré depuis l'env (Vercel, .env, etc.)

def require_token(x_token: str = Header(None)):
    # Si API_TOKEN est défini côté serveur, on l'exige :
    if API_TOKEN and x_token != API_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

# --- Notion client & DBs ---
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
if not NOTION_TOKEN:
    logger.warning("NOTION_TOKEN not found in environment; Notion calls will fail")
notion = Client(auth=NOTION_TOKEN) if NOTION_TOKEN else None

DBS: Dict[str, Optional[str]] = {
    "fwk":   os.getenv("FWK_DB_ID"),
    "agent": os.getenv("AGENT_DB_ID"),
    "module": os.getenv("MODULE_DB_ID"),
    "vars":  os.getenv("VARS_DB_ID"),
    "validations": os.getenv("VALIDATIONS_DB_ID"),
    "outputs": os.getenv("OUTPUTS_DB_ID"),
    "inputs": os.getenv("INPUTS_DB_ID"),
    "tests": os.getenv("TESTS_DB_ID"),
    "logs":  os.getenv("LOGS_DB_ID"),
    "kpis":  os.getenv("KPIS_DB_ID"),
}

NOTION_API_VERSION = os.getenv("NOTION_VERSION", "2022-06-28")

# --- Helpers ---
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


def _schema_of(database_id: str) -> Dict[str, str]:
    """
    Retourne {nom_propriété: type} pour une base Notion donnée.
    Utilise le SDK si dispo, sinon fallback REST.
    """
    # SDK
    if notion:
        meta = notion.databases.retrieve(database_id=database_id)
    else:
        token = NOTION_TOKEN or os.getenv("NOTION_TOKEN")
        if not token:
            raise RuntimeError("NOTION_TOKEN manquant pour lire le schéma Notion")
        url = f"https://api.notion.com/v1/databases/{database_id}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Notion-Version": NOTION_API_VERSION,
        }
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        meta = resp.json()

    props = meta.get("properties", {}) if isinstance(meta, dict) else {}
    return {name: p.get("type", "unknown") for name, p in props.items()}


def _first_title_prop(properties: dict) -> Optional[str]:
    for name, p in properties.items():
        if p.get("type") == "title":
            return name
    return None

def _query_database(database_id: str, page_size: int = 10) -> Dict[str, Any]:
    """
    Query a Notion database. Try SDK methods first; if none available, fallback to REST API via requests.
    Returns a dict expected to contain 'results'.
    """
    # SDK path
    if notion:
        db_endpoint = getattr(notion, "databases", None)
        if db_endpoint is not None:
            for method_name in ("query", "query_database", "query_pages", "query_collection"):
                method = getattr(db_endpoint, method_name, None)
                if callable(method):
                    logger.info("Using notion.databases.%s for database query", method_name)
                    try:
                        return method(database_id=database_id, page_size=page_size)
                    except TypeError:
                        # try without page_size if signature differs
                        return method(database_id=database_id)

    # Fallback: REST API
    token = NOTION_TOKEN or os.getenv("NOTION_TOKEN")
    if not token:
        logger.error("No NOTION_TOKEN available for REST fallback")
        raise RuntimeError("No NOTION_TOKEN available for Notion REST fallback")
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_API_VERSION,
        "Content-Type": "application/json",
    }
    payload = {"page_size": page_size}
    logger.info("Falling back to Notion REST API for database query id=%s page_size=%s", _mask(database_id), page_size)
    resp = requests.post(url, json=payload, headers=headers, timeout=15)
    try:
        resp.raise_for_status()
    except Exception as e:
        logger.exception("Notion REST query failed: status=%s body=%s", getattr(resp, "status_code", None), getattr(resp, "text", None))
        raise RuntimeError(f"Notion REST query failed: {e}")
    return resp.json()

def _create_log_entry(message: str, level: str = "INFO", extra: Optional[Dict[str, Any]] = None) -> Optional[dict]:
    logs_db = DBS.get("logs")
    if not logs_db:
        logger.warning("LOGS_DB_ID not configured; skipping log write")
        return None
    if not (notion or NOTION_TOKEN):
        logger.error("No Notion client/token available; cannot write log")
        return None

    # 1) Lire le schéma pour connaître le vrai nom du champ 'title' + props existantes
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
        title_prop = _first_title_prop(props_meta) or "Name"  # chez toi: "Description du changement"
    except Exception as e:
        logger.exception("Cannot read Logs DB metadata: %s", e)
        return None

    # 2) Construire les propriétés en ne posant QUE celles qui existent
    props: Dict[str, Any] = {title_prop: {"title": [{"text": {"content": message}}]}}

    if "Type" in props_meta and level:
        props["Type"] = {"select": {"name": level}}

    if "Meta" in props_meta and extra:
        props["Meta"] = {"rich_text": [{"text": {"content": str(extra)}}]}

    # Optionnels si tu les as (on ne casse pas si absents)
    from datetime import datetime, timezone
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


# --- Types ---
AllowedDB = Literal["fwk", "agent", "module", "vars", "validations", "outputs", "inputs", "tests", "logs", "kpis"]

# --- Endpoints ---
@app.get("/healthcheck")
def healthcheck():
    return {"status": "ok"}

@app.get("/architecte/analyse")
def analyse_base(db: AllowedDB = Query("fwk", description="Nom court de la base")):
    try:
        database_id = _ensure_db(db)
        logger.info("Retrieving schema for DB %s id=%s", db, _mask(database_id))
        meta = {}
        try:
            if notion:
                meta = notion.databases.retrieve(database_id=database_id)
            else:
                # REST fallback for retrieve
                token = NOTION_TOKEN or os.getenv("NOTION_TOKEN")
                if token:
                    url = f"https://api.notion.com/v1/databases/{database_id}"
                    headers = {"Authorization": f"Bearer {token}", "Notion-Version": NOTION_API_VERSION}
                    resp = requests.get(url, headers=headers, timeout=15)
                    resp.raise_for_status()
                    meta = resp.json()
        except Exception as e:
            logger.exception("Failed to retrieve database meta for %s: %s", database_id, e)
            raise RuntimeError("Failed to retrieve database metadata; check integration permissions")
        props = meta.get("properties", {}) if isinstance(meta, dict) else {}
        schema = {name: details.get("type", "unknown") for name, details in props.items()}
        return {"status": "ok", "base": db, "schema": schema}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error analysing DB %s: %s", db, e)
        return {"status": "error", "message": str(e)}

@app.get("/architecte/rows")
def get_rows(db: AllowedDB = Query("fwk"), limit: int = Query(3, ge=1, le=100)):
    try:
        dbid = _ensure_db(db)
        logger.info("Query rows base=%s limit=%s", db, limit)
        res = _query_database(database_id=dbid, page_size=limit)
        # normalize results
        results = res.get("results") if isinstance(res, dict) and "results" in res else (res if isinstance(res, list) else [])
        # attempt to get title prop
        meta = {}
        try:
            if notion:
                meta = notion.databases.retrieve(database_id=dbid)
            else:
                token = NOTION_TOKEN or os.getenv("NOTION_TOKEN")
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
        logger.info("Detected title property: %s", title_prop)
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
        return {"status": "error", "message": str(e)}


from fastapi import Depends  # (si pas déjà importé)

@app.get("/architecte/compare")
def compare(
    db: AllowedDB = Query("fwk", description="Base courante"),
    ref: str = Query("FWK_DB_ID_REF", description="Nom de la variable d'env de la base de référence"),
    _: None = Depends(require_token)
):
    curr_id = _ensure_db(db)
    ref_id = os.getenv(ref)
    if not ref_id:
        raise HTTPException(status_code=400, detail=f"Référence absente (env var '{ref}')")

    curr = _schema_of(curr_id)
    target = _schema_of(ref_id)

    missing = [k for k in target if k not in curr]
    extra = [k for k in curr if k not in target]
    type_mismatch = [k for k in target if k in curr and target[k] != curr[k]]

    try:
        _create_log_entry(
            f"Compare {db} → {ref}", "INFO",
            {"missing": missing[:5], "extra": extra[:5], "types": type_mismatch[:5]}
        )
    except Exception:
        pass

    return {
        "status": "ok",
        "base": db,
        "ref_env": ref,
        "missing": missing,
        "extra": extra,
        "type_mismatch": type_mismatch,
    }


# --- Debug endpoints (remove or secure in prod) ---
@app.get("/debug/env_status")
def debug_env_status():
    # clés “globales”
    keys = ["NOTION_TOKEN", "API_TOKEN"]
    # ajoute toutes les bases connues depuis le mapping DBS
    keys.extend(list(DBS.keys()))  # "fwk","agent","module","vars","validations","outputs","inputs","tests","logs","kpis"
    # mais _env_status attend des noms d'env, pas les alias; on mappe:
    env_keys = ["NOTION_TOKEN", "API_TOKEN",
                "FWK_DB_ID","AGENT_DB_ID","MODULE_DB_ID","VARS_DB_ID",
                "VALIDATIONS_DB_ID","OUTPUTS_DB_ID","INPUTS_DB_ID","TESTS_DB_ID","LOGS_DB_ID","KPIS_DB_ID"]

    return {
        "dotenv_path": str(env_path),
        "loaded": bool(_loaded),
        "env": _env_status(env_keys)
    }

@app.get("/debug/routes")
def debug_routes():
    routes = [{"path": r.path, "name": r.name, "methods": list(getattr(r, "methods", []))} for r in app.routes]
    return {"routes": routes}

# --- Root redirect to docs ---
@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs")

# --- Local runner ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000, reload=True)


@app.get("/architecte/logtest")
def log_test(_: None = Depends(require_token)):
    _create_log_entry("Test de log manuel depuis /logtest", "INFO", {"test": True})
    return {"status": "sent"}
