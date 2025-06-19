
# Nexus

Центральный узел для системы доступа к LLM. Предоставляет:
- OpenAI-compatible HTTP API
- WebSocket-подключения для клиентских библиотек и воркеров
- Кеширование запросов в Redis

## Требования

- Python 3.10
- Poetry  
- Redis (запущен на стандартном порту, можно изменить в `pyproject.toml`)

## Установка

```bash
poetry lock
```

## Запуск

```bash
poetry run poe start
```

Порты можно изменить в `pyproject.toml`
