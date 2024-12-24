# FastApi_HomeWork

Отправьте POST-запрос на эндпоинт /send с телом:

```curl -X POST http://localhost:8000/send \
   -H "Content-Type: application/json" \
   -d '{"id": "123", "name": "test_name"}'
```

Для проверки сохранённых данных сделайте GET-запрос на /data:
```curl http://localhost:8000/data```
