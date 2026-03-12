from fastapi import FastAPI
from storage.db import init_db

app = FastAPI(title="Lead Parser MVP v1.2")


@app.on_event("startup")
def startup():
    init_db()


@app.get("/")
def root():
    return {"status": "ok", "service": "lead_parser_google_mvp_v1_2_ddgs"}
