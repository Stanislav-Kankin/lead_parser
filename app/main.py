from fastapi import FastAPI
from storage.db import init_db

app = FastAPI(title="Lead Parser MVP")


@app.on_event("startup")
def startup():
    init_db()


@app.get("/")
def root():
    return {"status": "ok", "service": "lead_parser_google_mvp_v1"}
