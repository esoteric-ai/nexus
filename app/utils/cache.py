# app/utils/cache.py

import json
from typing import Any, Dict, List, Optional
import redis

# Connect to Redis running at 127.0.0.1:6379.
# decode_responses=True makes sure that Redis returns strings rather than bytes.
redis_client = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)

def compute_cache_key(conversation: List[Dict[str, Any]], model: str) -> str:
    """
    Computes a cache key based on the conversation and the model.
    The key is a JSON string with sorted keys for consistency.
    """
    key_data = {
        "model": model,
        "conversation": conversation
    }
    return json.dumps(key_data, sort_keys=True)

def get_cached_completion(conversation: List[Dict[str, Any]], models: List[str]) -> Optional[Dict[str, Any]]:
    """
    Check Redis for a cached completion for any of the requested models with the given conversation.
    If a cached result exists, return it as a dict.
    """
    for model in models:
        key = compute_cache_key(conversation, model)
        cached_value = redis_client.get(key)
        if cached_value:
            try:
                return json.loads(cached_value)
            except json.JSONDecodeError:
                # If decoding fails, ignore this cached value.
                continue
    return None

def add_completion_to_cache(conversation: List[Dict[str, Any]], model: str, text: Any) -> None:
    """
    Add a completed task (its conversation, the model, and the text)
    to the Redis cache.
    """
    key = compute_cache_key(conversation, model)
    value = {
        "conversation": conversation,
        "model": model,
        "text": text
    }
    # Save the JSON‑encoded value in Redis.
    redis_client.set(key, json.dumps(value))
