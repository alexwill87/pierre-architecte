# =====================================================
# Fichier : pierre.py
# Agent : Pierre – Architecte Exécutif du Panthéon
# Version : 1.3.1 – 2025-11-07
# Auteur : Aurel (coordination par Alexandre Willemetz)
# =====================================================

from fastapi import FastAPI, HTTPException, Request, Query, Body
from datetime import datetime
import os
import logging

# =====================================================
# Initialisation
# =====================================================

print("=== ENV DEBUG ===")
print("NOTION_TOKEN:", bool(os.getenv("NOTION_TOKEN")))
print("AUREL_TOKEN:", bool(os.getenv("AUREL_TOKEN")))
print("FWK_DB_ID:", os.getenv("FWK_DB_ID"))
print("=================")

# Configuration du logger interne
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Pierre")

# =====================================================
# Initialisation dynamique du client Notion
# =====================================================

def get_notion():
    """Initialise le client Notion dynamiquement (utile pour Vercel cold start)."""
    from notion_client import Client
    token = os.getenv("NOTION_TOKEN")
    if not token:
        raise HTTPException(status_code=500, detail="❌ NOTION_TOKEN manquant dans l'environnement")
    return Client(auth=token)

# Bases Notion
DB_IDS = {
    "fwk": os.getenv("FWK_DB_ID"),
    "agent": os.getenv("AGENT_DB_ID"),
    "module": os.getenv("MODULE_DB_ID"),
    "logs": os.getenv("LOGS_DB_ID"),
}

# Application FastAPI
app = FastAPI(
    title="Pierre – Architecte Exécutif du Panthéon",
    version="1.3.1",
    description="API standardisée pour la gestion des bases Notion (analyse, édition, logs, synchronisation)."
)

# =====================================================
# Sécurité
# =====================================================

def verify_token(request: Request):
    token = request.headers.get("X-Aurel-Token")
    if token != os.getenv("AUREL_TOKEN"):
        raise HTTPException(status_code=403, detail="Token invalide")
    return True

# =====================================================
# SECTION 1 : ROUTES GET
# =====================================================

@app.get("/architecte/health")
async def health():
    return {
        "status": "alive",
        "agent": "Pierre",
        "version": "1.3.1",
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/architecte/version")
async def version():
    return {
        "agent": "Pierre",
        "version": "1.3.1",
        "last_update": "2025-11-07T00:00Z"
    }


@app.get("/architecte/analyse")
async def analyse(db: str = Query("fwk", description="Nom abrégé de la base (fwk, agent, module...)")):
    db_id = DB_IDS.get(db)
    if not db_id:
        raise HTTPException(status_code=400, detail=f"Base inconnue : {db}")

    logger.info(f"Analyse de la base : {db}")
    db_info = get_notion().databases.retrieve(db_id)
    schema = {k: v["type"] for k, v in db_info["properties"].items()}
    return {"status": "ok", "base": db, "schema": schema}


@app.get("/architecte/compare")
async def compare(
    db: str = Query("module", description="Base à comparer avec FWK"),
    ref: str = Query("FWK_DB_ID", description="Nom de la variable d'environnement de référence")
):
    base_id = DB_IDS.get(db)
    ref_id = os.getenv(ref)
    if not base_id or not ref_id:
        raise HTTPException(status_code=400, detail="Base ou référence manquante")

    notion = get_notion()
    logger.info(f"Comparaison de {db} avec FWK")
    base_schema = notion.databases.retrieve(base_id)["properties"]
    ref_schema = notion.databases.retrieve(ref_id)["properties"]

    missing = [k for k in ref_schema if k not in base_schema]
    extra = [k for k in base_schema if k not in ref_schema]
    type_mismatch = [
        k for k in base_schema if k in ref_schema and base_schema[k]["type"] != ref_schema[k]["type"]
    ]

    return {"status": "ok", "base": db, "ref_env": ref, "missing": missing, "extra": extra, "type_mismatch": type_mismatch}

# =====================================================
# SECTION 2 : ROUTES POST
# =====================================================

@app.post("/architecte/log")
async def create_log(request: Request, message: str = Body("Log manuel", description="Texte du log à enregistrer")):
    verify_token(request)
    db_id = DB_IDS.get("logs")
    if not db_id:
        raise HTTPException(status_code=500, detail="Base Logs non configurée")

    logger.info(f"Création d'un log : {message}")
    notion = get_notion()
    notion.pages.create(
        parent={"database_id": db_id},
        properties={
            "Description du changement": {"title": [{"text": {"content": message}}]},
            "Date du changement": {"date": {"start": datetime.utcnow().isoformat()}}
        }
    )
    return {"status": "ok", "message": "log envoyé"}


@app.post("/logtest")
async def logtest(request: Request):
    verify_token(request)
    db_id = DB_IDS.get("logs")
    if not db_id:
        raise HTTPException(status_code=500, detail="LOGS_DB_ID non trouvé")

    test_message = f"✅ Test LogTest depuis Pierre – {datetime.utcnow().isoformat()}"
    logger.info(f"[LogTest] Message: {test_message}")

    notion = get_notion()
    notion.pages.create(
        parent={"database_id": db_id},
        properties={
            "Description du changement": {"title": [{"text": {"content": test_message}}]},
            "Date du changement": {"date": {"start": datetime.utcnow().isoformat()}}
        }
    )
    return {"status": "ok", "message": "LogTest réussi"}


@app.post("/architecte/edit")
async def edit_entry(request: Request, db: str = Query(...), data: dict = Body(...)):
    verify_token(request)
    db_id = DB_IDS.get(db)
    if not db_id:
        raise HTTPException(status_code=400, detail=f"Base inconnue : {db}")

    notion = get_notion()
    res = notion.pages.create(parent={"database_id": db_id}, properties=data)
    return {"status": "ok", "db": db, "id": res.get("id")}


@app.post("/architecte/delete")
async def delete_entry(request: Request, page_id: str = Body(...)):
    verify_token(request)
    if not page_id:
        raise HTTPException(status_code=400, detail="page_id manquant")

    get_notion().pages.update(page_id=page_id, archived=True)
    return {"status": "ok", "message": f"Page {page_id} supprimée"}


@app.post("/architecte/update")
async def update_fields(request: Request, page_id: str = Body(...), fields: dict = Body(...)):
    verify_token(request)
    get_notion().pages.update(page_id=page_id, properties=fields)
    return {"status": "ok", "message": f"Page {page_id} mise à jour"}


@app.post("/architecte/sync")
async def sync_schema(request: Request, db: str = Query("module")):
    verify_token(request)
    base_id = DB_IDS.get(db)
    ref_id = DB_IDS.get("fwk")
    if not base_id or not ref_id:
        raise HTTPException(status_code=400, detail="Base ou référence manquante")

    notion = get_notion()
    base = notion.databases.retrieve(base_id)
    ref = notion.databases.retrieve(ref_id)

    missing_props = [k for k in ref["properties"] if k not in base["properties"]]
    logger.info(f"Synchronisation : {len(missing_props)} propriétés manquantes détectées dans {db}")
    return {"status": "ok", "base": db, "missing_properties": missing_props, "message": f"{len(missing_props)} propriétés manquantes détectées"}

# =====================================================
# Fin du module Pierre
# =====================================================
