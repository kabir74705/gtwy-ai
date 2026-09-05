import asyncio

from pymongo.errors import OperationFailure, PyMongoError

from config import Config
from globals import logger
from models.mongo_connection import db
from src.services.commonServices.baseService.utils import send_message
from src.services.utils.load_model_configs import (
    get_model_configurations,
    normalize_model_config,
    reorder_service_models,
)

model_config_model = db["modelconfigurations"]
model_config_document = {}


async def init_model_configuration():
    """Initializes or refreshes the model configuration document."""
    global model_config_document
    try:
        new_document = await get_model_configurations()
        model_config_document.clear()  # Clear old config before updating
        model_config_document.update(new_document)
        logger.info("Model configurations refreshed successfully.")
    except Exception as e:
        logger.error(f"Error refreshing model configurations: {e}")


def _remove_model(service: str | None, model_name: str | None) -> None:
    if not service or not model_name:
        return
    service_map = model_config_document.get(service)
    if not service_map:
        return
    service_map.pop(model_name, None)
    if not service_map:
        model_config_document.pop(service, None)


def apply_model_config_change(change: dict) -> bool:
    """
    Patch model_config_document from a change-stream event.
    Returns False when the event cannot be applied (caller should full-refresh).
    """
    op = change.get("operationType")
    doc = change.get("fullDocument")
    before = change.get("fullDocumentBeforeChange") or {}

    if op == "delete":
        if not before.get("service") or not before.get("model_name"):
            return False
        _remove_model(before.get("service"), before.get("model_name"))
        return True

    if not doc:
        return False

    conf = normalize_model_config(doc)
    service = conf.get("service")
    model_name = conf.get("model_name")
    if not service or not model_name:
        return False

    # Handle rename of service / model_name.
    old_service = before.get("service")
    old_model = before.get("model_name")
    if old_service and old_model and (old_service != service or old_model != model_name):
        _remove_model(old_service, old_model)

    # AI cache only keeps active models (status == 1), matching get_model_configurations.
    status = conf.get("status", 1)
    if status != 1:
        _remove_model(service, model_name)
        return True

    if service not in model_config_document:
        model_config_document[service] = {}
    model_config_document[service][model_name] = conf
    reorder_service_models(model_config_document[service])
    return True


async def _async_change_listener():
    """The core async change stream listener."""
    pipeline = [{"$match": {"operationType": {"$in": ["insert", "update", "replace", "delete"]}}}]
    try:
        async with model_config_model.watch(
            pipeline,
            full_document="updateLookup",
            full_document_before_change="whenAvailable",
        ) as stream:
            logger.info("MongoDB change stream is now listening for model configuration changes.")
            async for change in stream:
                logger.info(f"Change detected in model configurations: {change['operationType']}")
                try:
                    applied = apply_model_config_change(change)
                    if not applied:
                        logger.warning("Could not apply model config change incrementally; full refresh.")
                        await init_model_configuration()
                except Exception as apply_err:
                    logger.error(f"Error applying model config change; full refresh: {apply_err}")
                    await init_model_configuration()

                full_doc = change.get("fullDocument") or change.get("fullDocumentBeforeChange") or {}
                await send_message(
                    cred={"apikey": Config.RTLAYER_AUTH, "ttl": 1, "channel": "global_model_updates"},
                    data={
                        "event": "model_config_updated",
                        "operation": change["operationType"],
                        "model_name": full_doc.get("model_name"),
                        "service": full_doc.get("service"),
                        "timestamp": str(change.get("clusterTime", "")),
                    },
                )
                logger.info("Model configuration change detected and sent to RTLayer successfully.")
    except OperationFailure as e:
        logger.error(f"Change stream operation failed: {e}")
        raise  # Re-raise to be caught by the sync wrapper
    except Exception as e:
        logger.error(f"An unexpected error occurred in the async listener: {e}")
        raise


async def background_listen_for_changes():
    """An asynchronous change stream listener with a retry loop, designed to run as a background task."""
    while True:
        try:
            await _async_change_listener()
        except (OperationFailure, PyMongoError) as e:
            logger.error(f"MongoDB connection error in change stream: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(
                f"An unexpected error occurred in background_listen_for_changes: {e}. Restarting in 10 seconds..."
            )
            await asyncio.sleep(10)
