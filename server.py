import os, time, logging, uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
from typing import Optional, Union
from adapter import LexikaAdapter

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("lexika")

BASE_URL = os.getenv("LEXIKA_BASE_URL", "https://api.lexika.ai")
JWT_TOKEN = os.getenv("LEXIKA_JWT_TOKEN", "")
WORKSPACE_ID = os.getenv("LEXIKA_WORKSPACE_ID", "")
ORIGIN = os.getenv("LEXIKA_ORIGIN", "https://lexika.ai")
LOCALE = os.getenv("LEXIKA_LOCALE", "en")
MODEL_NAME = os.getenv("MODEL_NAME", "claude-sonnet-4-6")
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))
DSML_ENABLED = os.getenv("DSML_ENABLED", "false").lower() in ("true", "1", "yes")
COOKIES = os.getenv("LEXIKA_COOKIES", "")
PROXY = os.getenv("LEXIKA_PROXY", "")

adapter = LexikaAdapter(base_url=BASE_URL, jwt_token=JWT_TOKEN, workspace_id=WORKSPACE_ID, origin=ORIGIN, locale=LOCALE, default_model=MODEL_NAME, dsml_enabled=DSML_ENABLED, cookies=COOKIES, proxy=PROXY)
app = FastAPI(title="Lexika Proxy", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

class ChatCompletionRequest(BaseModel):
    model: Optional[str] = None
    messages: list
    stream: bool = False
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None
    tools: Optional[list] = None
    tool_choice: Optional[Union[str, dict]] = None

@app.middleware("http")
async def auth_middleware(request, call_next):
    return await call_next(request)

@app.get("/v1/models")
async def list_models():
    models = await adapter.fetch_models()
    return {"object": "list", "data": [{"id": m.get("name", ""), "object": "model", "created": int(time.time()), "owned_by": m.get("company", "unknown")} for m in models]}

@app.post("/v1/chat/completions")
async def chat_completions(request: ChatCompletionRequest):
    messages = [{"role": m.get("role", "user"), "content": m.get("content", "")} for m in request.messages]
    model = request.model or MODEL_NAME
    tools_dict = [t if isinstance(t, dict) else t.model_dump() for t in request.tools] if request.tools else None
    payload = adapter.convert_request(messages, stream=request.stream, model=model, tools=tools_dict, tool_choice=request.tool_choice)
    if request.stream:
        return StreamingResponse(adapter.stream_request(payload), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"})
    try:
        return await adapter.send_request(payload)
    except Exception as e:
        logger.error(str(e))
        return JSONResponse(status_code=502, content={"error": {"message": str(e), "type": "upstream_error", "code": 502}})

@app.get("/health")
async def health():
    return {"status": "ok", "target": BASE_URL, "model": MODEL_NAME}

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}

@app.on_event("startup")
async def startup_event():
    logger.info("Starting Lexika proxy...")
    await adapter.start_token_keepalive()
    models = await adapter.fetch_models()
    logger.info(str(len(models)) + " models loaded")

@app.on_event("shutdown")
async def shutdown_event():
    await adapter.shutdown()

if __name__ == "__main__":
    print("Lexika proxy on http://" + HOST + ":" + str(PORT))
    uvicorn.run(app, host=HOST, port=PORT)
