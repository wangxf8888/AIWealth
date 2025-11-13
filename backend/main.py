# /home/AIWealth/backend/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="AIWealth API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/health")
def health_check():
    return {"status": "ok", "message": "AIWealth 后端运行中！"}

@app.get("/api/test")
def test_api():
    return {
        "strategy": "测试策略",
        "annualReturn": 25.5,
        "maxDrawdown": -15.2
    }

