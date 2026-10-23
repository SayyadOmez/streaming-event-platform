# Distributed Event Processing

**Local demo:** JSONL event producer/export → validated event sink → deduplicated SQLite store + dead-letter table.

```bash
pip install -e '.[dev]'
python -m src.stream
pytest -q
```

**Spark extension:** `spark/streaming.py` uses Structured Streaming file ingestion, event-time watermarks, event deduplication, 5-minute windows, and durable checkpoints. Requires a compatible PySpark installation and new input JSON files. The local example is a batch simulator, **not Kafka streaming**; Kafka/MongoDB/Docker are proposed extensions rather than implemented dependencies.

For a production deployment, implement dead-letter storage in Spark, replay controls, Kafka offsets, schema registry, and monitoring. The 10-minute watermark is an example lateness policy.
