from pathlib import Path
import os

from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers
from prefect import flow, task, get_run_logger
from prefect_dask import DaskTaskRunner

load_dotenv()  # ES_URL, ES_USER, ES_PASS from .env

# ──────────────────────────────────────────────────────────────────────────────
@task(
    retries=3,
    retry_delay_seconds=60,
    tags=["elasticsearch-etl"],
    name="etl-slice-{slice_id}",
)
def etl_slice(
    slice_id: int,
    max_slices: int,
    src_index: str,
    dest_index: str,
) -> int:
    es = Elasticsearch(
        hosts=[{"host": os.getenv("ES_URL"), "port": 9200, "scheme": "https"}],
        basic_auth=(os.getenv("ES_USER"), os.getenv("ES_PASS")),
        verify_certs=False,
        request_timeout=1000,
    )

    log = get_run_logger()
    wrote = 0

    scroll = es.search(
        index=src_index,
        scroll="2m",
        size=1_000,
        body={"slice": {"id": slice_id, "max": max_slices}, "query": {"match_all": {}}},
    )

    while True:
        hits = scroll["hits"]["hits"]
        if not hits:
            break

        actions = [
            {
                "_index": dest_index,
                "_id": d["_id"],
                "_source": transform(d["_source"]),
            }
            for d in hits
        ]
        helpers.bulk(es, actions, request_timeout=120)
        wrote += len(actions)

        scroll = es.scroll(scroll_id=scroll["_scroll_id"], scroll="2m")

    log.info(f"slice {slice_id} finished – wrote {wrote} docs")
    return wrote


def transform(doc: dict) -> dict:
    doc["copied_at"] = "2025-06-11"
    return doc


@flow(name="distributed-reindex", task_runner=DaskTaskRunner())
def reindex(
    max_slices: int = 8,
    src_index: str = "old-index",
    dest_index: str = "new-index",
):
    slice_futures = etl_slice.map(
        list(range(max_slices)),
        [max_slices] * max_slices,
        [src_index] * max_slices,
        [dest_index] * max_slices,
    )

    # ── resolve futures manually (works on every Prefect-3 version)
    slice_counts = [f.result() for f in slice_futures]
    total = sum(slice_counts)

    get_run_logger().info(f"ALL DONE – copied {total} docs 🚀")
    return total
