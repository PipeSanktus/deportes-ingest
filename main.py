import asyncio
import logging

import httpx
from fastapi import FastAPI, HTTPException
from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from app.routes.worker import router as worker_router
from app.routes.tiles import router as tiles_router
from app.schemas import OSMIngestRequest
from app.services.osm_ingest import run_osm_ingest
from app.settings import settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("deportes.app")

# Configure tracer
trace.set_tracer_provider(TracerProvider())
jaeger_exporter = JaegerExporter(
    agent_host_name=settings.JAEGER_HOST,
    agent_port=settings.JAEGER_PORT,
)
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(jaeger_exporter))

# Initialize FastAPI
app = FastAPI(title="Deportes Ingest MVP")

# Add instrumentation
FastAPIInstrumentor.instrument_app(app)

# Routers
app.include_router(worker_router)
app.include_router(tiles_router)


@app.post("/ingest/osm")
async def ingest_osm(payload: OSMIngestRequest):
    try:
        return await run_osm_ingest(payload)
    except httpx.HTTPError as exc:
        logger.exception("Error consultando Overpass bbox=%s", payload.bbox)
        raise HTTPException(status_code=502, detail="Error consultando Overpass") from exc
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("Error procesando ingest")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/test-trace")
async def test_trace():
    logger.debug("Entrando a test-trace")
    tracer = trace.get_tracer(__name__)
    try:
        with tracer.start_as_current_span("test-operation") as span:
            span.set_attribute("test_attribute", "test_value")
            await asyncio.sleep(1)
            logger.debug("Trace completado")
            return {"message": "Trace created!"}
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("Error en test-trace")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
