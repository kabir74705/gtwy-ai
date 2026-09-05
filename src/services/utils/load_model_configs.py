from globals import logger
from models.mongo_connection import db
from src.services.utils.time import with_timeout

modelConfigModel = db["modelconfigurations"]


def _created_at_sort_key(conf: dict) -> float:
    created_at = conf.get("created_at")
    if created_at is None:
        return 0.0
    if hasattr(created_at, "timestamp"):
        try:
            return float(created_at.timestamp())
        except Exception:
            return 0.0
    try:
        from datetime import datetime

        if isinstance(created_at, str):
            return datetime.fromisoformat(created_at.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0
    return 0.0


def normalize_model_config(conf: dict) -> dict:
    """Match get_model_configurations shape (no _id; clean usage._id)."""
    conf_dict = dict(conf)
    conf_dict.pop("_id", None)
    output_config = conf_dict.get("outputConfig")
    if isinstance(output_config, dict):
        usage = output_config.get("usage")
        if isinstance(usage, list) and usage and isinstance(usage[0], dict) and "_id" in usage[0]:
            del usage[0]["_id"]
    return conf_dict


def reorder_service_models(service_map: dict) -> None:
    """Keep dict key order newest created_at first (Python 3.7+)."""
    if not service_map:
        return
    items = sorted(service_map.items(), key=lambda kv: _created_at_sort_key(kv[1]), reverse=True)
    service_map.clear()
    service_map.update(items)


async def get_model_configurations():
    try:
        configurations = await with_timeout(
            modelConfigModel.find({"status": 1}, {"_id": 0}).sort("created_at", -1).to_list(length=None)
        )
        config_dict = {}
        for conf in configurations:
            conf_dict = normalize_model_config(conf)
            service = conf["service"]
            if config_dict.get(service) is None:
                config_dict[service] = {}
            config_dict[service][conf["model_name"]] = conf_dict

        return config_dict
    except Exception as error:
        logger.error(f"Error fetching model configurations:, {error}")
        return {}
