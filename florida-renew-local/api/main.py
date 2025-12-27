from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from data import get_con, register_tables, search_entities, get_entity_by_doc

app = FastAPI(title="Florida Renew Local API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # for local dev
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CON = get_con()
register_tables(CON)

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/prefill/search")
def prefill_search(q: str = "", limit: int = 10):
    try:
        return {"results": search_entities(CON, q=q, limit=limit)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"search failed: {type(e).__name__}: {e}")

@app.get("/prefill/by-doc/{doc}")
def prefill_by_doc(doc: str):
    try:
        entity = get_entity_by_doc(CON, doc)
        if not entity:
            raise HTTPException(status_code=404, detail="Not found")
        return entity
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"by-doc failed: {type(e).__name__}: {e}")
