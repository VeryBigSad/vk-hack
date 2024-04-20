from core.webhooks import handlers
from fastapi import FastAPI

app = FastAPI(title="HSE x VK Hackathon solution")
app.include_router(handlers.router)
