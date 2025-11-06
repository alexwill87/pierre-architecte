# =====================================================
# Fichier : pierre.py
# Agent : Pierre – Architecte Exécutif du Panthéon
# Version : 1.3.0 – 2025-11-07
# Auteur : Aurel (coordination par Alexandre Willemetz)
# =====================================================

from fastapi import FastAPI, HTTPException, Request, Query, Body
from notion_client import Client
from datetime import datetime
import os
import logging

# =====================================================
# Initialisation
# =====================================================

# Configuration du logger interne
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Pierre")

# Initialisation du client Notion
try:
    notion = Client(auth=os.getenv("NOTION_TOKEN"))
except Exception as e:
    notion = None
    logger.error(f"Erreur d'initialisation Notion : {e}")

# Bases Notion (identifiants récupérés depuis .env)
DB_IDS = {
    "fwk": os.getenv("FWK_DB_ID"),
    "agent": os.getenv("AGENT_DB_ID"),
    "module": os.getenv("MODULE_DB_ID"),
    "logs": os.getenv("LOGS_DB_ID"),
}

# Initialisation de l’application FastAPI
app = FastAPI(
    title="Pierre – Architecte Exécutif du Panthéon",
    version="1.3.0",
    description="API standardisée pour la gestion des bases Notion (analyse, édition, logs, synchronisation)."
)

# =====================================================
# Sécurité
# =====================================================

def verify_token(request: Request):
    """Vérifie la validité du token d'authentification."""
    token = request.headers.get("X-Aurel-Token")
    if token != os.getenv("AUREL_TOKEN"):
        raise HTTPException(status_code=403, detail="Token invalide")
    return True

# =====================================================
# SECTION 1 : ROUTES GET (lecture)
# =====================================================

@app.get("/architecte/health")
async def health():
    """Vérifie que Pierre est vivant et fonctionnel."""
    return {
        "status": "alive",
        "agent": "Pierre",
        "version": "1.3.0",
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/architecte/version")
async def version():
    """Retourne la version actuelle et la date de dernière mise à jour."""
    return {
        "agent": "Pierre",
        "version": "1.3.0",
        "last_update": "2025-11-07T00:00Z"
    }


@app.get("/architecte/analyse")
async def analyse(db: str = Query("fwk", description="Nom abrégé de la base à analyser (fwk, agent, module...)")):
    """Analyse la structure d'une base Notion."""
    db_id = DB_IDS.get(db)
    if not db_id:
        raise HTTPException(status_code=400, detail=f"Base inconnue : {db}")
    if not notion:
        raise HTTPException(status_code=500, detail="Client Notion non initialisé")

    logger.info(f"Analyse de la base : {db}")
    db_info = notion.databases.retrieve(db_id)
    schema = {k: v["type"] for k, v in db_info["properties"].items()}

    return {"status": "ok", "base": db, "schema": schema}


@app.get("/architecte/compare")
async def compare(
    db: str = Query("module", description="Base à comparer avec la référence FWK"),
    ref: str = Query("FWK_DB_ID", description="Variable d'environnement de la base de référence")
):
    """Compare la structure d'une base donnée avec la base FWK."""
    base_id = DB_IDS.get(db)
    ref_id = os.getenv(ref)
    if not base_id or not ref_id:
        raise HTTPException(status_code=400, detail="Base ou référence manquante")

    if not notion:
        raise HTTPException(status_code=500, detail="Client Notion non initialisé")

    logger.info(f"Comparaison de {db} avec référence FWK")
    base_schema = notion.databases.retrieve(base_id)["properties"]
    ref_schema = notion.databases.retrieve(ref_id)["properties"]

    missing = [k for k in ref_schema if k not in base_schema]
    extra = [k for k in base_schema if k not in ref_schema]
    type_mismatch = [
        k for k in base_schema
        if k in ref_schema and base_schema[k]["type"] != ref_schema[k]["type"]
    ]

    return {
        "status": "ok",
        "base": db,
        "ref_env": ref,
        "missing": missing,
        "extra": extra,
        "type_mismatch": type_mismatch
    }

# =====================================================
# SECTION 2 : ROUTES POST (écriture / actions)
# =====================================================

@app.post("/architecte/log")
async def create_log(
    request: Request,
    message: str = Body("Log manuel", description="Texte du log à enregistrer")
):
    """Écrit un log manuel dans la base Logs."""
    verify_token(request)

    db_id = DB_IDS.get("logs")
    if not db_id:
        raise HTTPException(status_code=500, detail="Base Logs non configurée")

    logger.info(f"Création d'un log : {message}")
    notion.pages.create(
        parent={"database_id": db_id},
        properties={
            "Description du changement": {"title": [{"text": {"content": message}}]},
            "Date du changement": {"date": {"start": datetime.utcnow().isoformat()}}
        }
    )
    return {"status": "ok", "message": "log envoyé"}


@app.post("/architecte/edit")
async def edit_entry(
    request: Request,
    db: str = Query(..., description="Nom abrégé de la base Notion (fwk, agent, module, logs...)"),
    data: dict = Body(..., description="Propriétés JSON de la page à créer ou modifier")
):
    """Crée une nouvelle entrée Notion dans la base spécifiée."""
    verify_token(request)

    db_id = DB_IDS.get(db)
    if not db_id:
        raise HTTPException(status_code=400, detail=f"Base inconnue : {db}")
    if not notion:
        raise HTTPException(status_code=500, detail="Client Notion non initialisé")

    try:
        logger.info(f"Création d'une entrée dans {db}")
        res = notion.pages.create(parent={"database_id": db_id}, properties=data)
        return {"status": "ok", "db": db, "id": res.get("id")}
    except Exception as e:
        logger.error(f"Erreur lors de la création dans {db} : {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/architecte/delete")
async def delete_entry(
    request: Request,
    page_id: str = Body(..., description="Identifiant de la page à supprimer (archive)")
):
    """Archive une page Notion existante."""
    verify_token(request)
    if not page_id:
        raise HTTPException(status_code=400, detail="page_id manquant")

    try:
        logger.info(f"Suppression logique de la page {page_id}")
        notion.pages.update(page_id=page_id, archived=True)
        return {"status": "ok", "message": f"Page {page_id} supprimée"}
    except Exception as e:
        logger.error(f"Erreur suppression : {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/architecte/update")
async def update_fields(
    request: Request,
    page_id: str = Body(..., description="ID de la page à modifier"),
    fields: dict = Body(..., description="Champs à mettre à jour")
):
    """Met à jour des champs d'une page existante."""
    verify_token(request)
    if not notion:
        raise HTTPException(status_code=500, detail="Client Notion non initialisé")

    try:
        notion.pages.update(page_id=page_id, properties=fields)
        logger.info(f"Mise à jour de la page {page_id}")
        return {"status": "ok", "message": f"Page {page_id} mise à jour"}
    except Exception as e:
        logger.error(f"Erreur update : {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/architecte/sync")
async def sync_schema(
    request: Request,
    db: str = Query("module", description="Nom de la base à synchroniser")
):
    """Compare et signale les propriétés manquantes entre une base et FWK."""
    verify_token(request)
    if not notion:
        raise HTTPException(status_code=500, detail="Client Notion non initialisé")

    base_id = DB_IDS.get(db)
    ref_id = DB_IDS.get("fwk")
    if not base_id or not ref_id:
        raise HTTPException(status_code=400, detail="Base ou référence manquante")

    base = notion.databases.retrieve(base_id)
    ref = notion.databases.retrieve(ref_id)

    missing_props = [
        k for k in ref["properties"].keys() if k not in base["properties"].keys()
    ]

    logger.info(f"Synchronisation : {len(missing_props)} propriétés manquantes détectées dans {db}")
    return {
        "status": "ok",
        "base": db,
        "missing_properties": missing_props,
        "message": f"{len(missing_props)} propriétés manquantes détectées"
    }

# =====================================================
# Fin du module Pierre
# =====================================================
