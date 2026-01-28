from fastapi import FastAPI
from pydantic import BaseModel
import sqlite3
from datetime import datetime
from pathlib import Path

app = FastAPI()

DB_PATH = Path(__file__).resolve().parent.parent / "database-demo" / "payments.db"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

class PaymentCreate(BaseModel):
    user_id: int
    amount: float
    idempotency_key: str

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/payments")
def get_payments():
    db = get_db()
    rows = db.execute("SELECT * FROM payments ORDER BY created_at DESC").fetchall()
    db.close()
    return [dict(r) for r in rows]

@app.get("/users/{user_id}/revenue")
def get_revenue(user_id: int):
    db = get_db()
    row = db.execute(
        "SELECT user_id, COALESCE(SUM(amount), 0) AS total FROM payments WHERE user_id = ? AND status = 'success'",
        (user_id,),
    ).fetchone()
    db.close()
    return dict(row)

@app.post("/payments")
def create_payment(p: PaymentCreate):
    db = get_db()
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    try:
        db.execute(
            "INSERT INTO payments (user_id, amount, status, idempotency_key, created_at) VALUES (?, ?, 'success', ?, ?)",
            (p.user_id, p.amount, p.idempotency_key, now),
        )
        db.commit()
    except sqlite3.IntegrityError:
        row = db.execute(
            "SELECT * FROM payments WHERE user_id = ? AND idempotency_key = ?",
            (p.user_id, p.idempotency_key),
        ).fetchone()
        db.close()
        return dict(row)

    row = db.execute("SELECT * FROM payments ORDER BY id DESC LIMIT 1").fetchone()
    db.close()
    return dict(row)
@app.get("/debug-routes")
def debug_routes():
    return [r.path for r in app.routes]