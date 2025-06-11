"""
distributed_reindex.py
───────────────────────────────────────────────────────────────
A **Prefect-3.x** rewrite of the original “copy one ES index to
another” job, broken into meaningful layers:

• reindex()            - top-level orchestration flow
• etl_slice_flow()     - one Elasticsearch slice (fan-out / fan-in)
• process_document()   - per-document NLP pipeline (sub-flow)
• many small @task     - each real unit of work

All *your* model / NLP logic goes in the TODO blocks.
"""

from __future__ import annotations

import os
from datetime import date
from typing import Any, Dict, List

from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers
from prefect import flow, task, get_run_logger
from prefect_dask import DaskTaskRunner
from prefect.transactions import transaction

# ──────────────────────────────────────────────────────────────
#  Globals & config
# ──────────────────────────────────────────────────────────────
load_dotenv()  # expects ES_URL, ES_USER, ES_PASS in .env
TODAY = date.today().isoformat()


def _get_es() -> Elasticsearch:
    """Helper to build an Elasticsearch client."""
    return Elasticsearch(
        hosts=[{"host": os.getenv("ES_URL"), "port": 9200, "scheme": "https"}],
        basic_auth=(os.getenv("ES_USER"), os.getenv("ES_PASS")),
        verify_certs=False,          
        request_timeout=1000,
    )


# ──────────────────────────────────────────────────────────────
#  Low-level tasks – fill the TODOs with real code
# ──────────────────────────────────────────────────────────────
@task(retries=2, retry_delay_seconds=30, tags=["embeddings", "qwen"])
def generate_qwen_embedding(doc: Dict[str, Any]) -> Dict[str, Any]:
    # TODO: call Qwen model and attach vector to `doc["qwen_embedding"]`
    return doc


@task(retries=2, retry_delay_seconds=30, tags=["embeddings", "e5"])
def generate_e5_embedding(doc: Dict[str, Any]) -> Dict[str, Any]:
    # TODO
    return doc


@task(retries=2, retry_delay_seconds=30, tags=["embeddings", "bge"])
def generate_bge_embedding(doc: Dict[str, Any]) -> Dict[str, Any]:
    # TODO
    return doc


@task(tags=["nlp", "keywords"])
def extract_keywords(doc: Dict[str, Any]) -> Dict[str, Any]:
    # TODO
    return doc


@task(tags=["nlp", "ner"])
def extract_ner(doc: Dict[str, Any]) -> Dict[str, Any]:
    # TODO
    return doc


@task(tags=["augment"])
def generate_question_augmentation(doc: Dict[str, Any]) -> Dict[str, Any]:
    # TODO
    return doc


@task(tags=["translation"])
def translate_question(doc: Dict[str, Any]) -> Dict[str, Any]:
    # TODO
    return doc


@task(tags=["translation"])
def translate_answer(doc: Dict[str, Any]) -> Dict[str, Any]:
    # TODO
    return doc


@task
def add_processed_timestamp(doc: Dict[str, Any]) -> Dict[str, Any]:
    doc["processed_at"] = TODAY
    return doc


# ──────────────────────────────────────────────────────────────
#  Elasticsearch I/O tasks
# ──────────────────────────────────────────────────────────────
@task(tags=["elasticsearch", "read"])
def fetch_slice(
    slice_id: int,
    max_slices: int,
    src_index: str,
) -> List[Dict[str, Any]]:
    """Read one slice of documents from Elasticsearch."""
    es = _get_es()

    scroll = es.search(
        index=src_index,
        scroll="2m",
        size=1_000,
        body={"slice": {"id": slice_id, "max": max_slices}, "query": {"match_all": {}}},
    )

    docs: List[Dict[str, Any]] = []
    while True:
        hits = scroll["hits"]["hits"]
        if not hits:
            break
        docs.extend([{"_id": h["_id"], **h["_source"]} for h in hits])
        scroll = es.scroll(scroll_id=scroll["_scroll_id"], scroll="2m")

    return docs


@task(tags=["elasticsearch", "write"])
def bulk_index(
    docs: List[Dict[str, Any]],
    dest_index: str,
) -> int:
    """Write (index) a batch of docs to ES and return the count."""
    if not docs:
        return 0

    es = _get_es()
    actions = [{"_index": dest_index, "_id": d["_id"], "_source": d} for d in docs]
    helpers.bulk(es, actions, request_timeout=120)
    return len(actions)


# ──────────────────────────────────────────────────────────────
#  Per-document sub-flow
# ──────────────────────────────────────────────────────────────
@flow(name="process-document")
def process_document(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    A mini-DAG for one document.

    • Embeddings (Qwen, E5, BGE)
    • Keyword + NER extraction
    • Conditional augmentation / translation
    • Timestamp
    """
    # Ensures re-runs don't double-process (Prefect 3 transaction API)
    with transaction(key=f"doc-{doc['_id']}"):
        # Chain the tasks, passing Futures (Prefect resolves automatically)
        doc = generate_qwen_embedding.submit(doc)
        doc = generate_e5_embedding.submit(doc)
        doc = generate_bge_embedding.submit(doc)

        doc = extract_keywords.submit(doc)
        doc = extract_ner.submit(doc)

        # 🔸 Example conditional branch – adjust to your real rules
        #    If the language is Farsi, augment & translate.
        @task
        def _needs_translation(d: Dict[str, Any]) -> bool:
            return str(d.get("language", "")).lower() == "fa"

        if _needs_translation.submit(doc):
            doc = generate_question_augmentation.submit(doc)
            doc = translate_question.submit(doc)
            doc = translate_answer.submit(doc)

        doc = add_processed_timestamp.submit(doc)

        # Get the concrete dict to return
        return doc.result()


# ──────────────────────────────────────────────────────────────
#  Slice-level sub-flow
# ──────────────────────────────────────────────────────────────
@flow(name="etl-slice")
def etl_slice_flow(
    slice_id: int,
    max_slices: int,
    src_index: str,
    dest_index: str,
) -> int:
    """
    • Pull one ES slice
    • Fan-out → process each doc in parallel
    • Fan-in  → bulk-index processed docs
    """
    log = get_run_logger()

    raw_docs = fetch_slice(slice_id, max_slices, src_index)
    if not raw_docs:
        log.info(f"slice {slice_id} contains 0 docs – skipping")
        return 0

    # Launch a sub-flow for every doc (concurrently on Dask)
    doc_futures = [process_document.submit(d) for d in raw_docs]
    processed_docs = [f.result() for f in doc_futures]

    wrote = bulk_index(processed_docs, dest_index)
    log.info(f"slice {slice_id} finished – wrote {wrote} docs")

    return wrote


# ──────────────────────────────────────────────────────────────
#  Top-level orchestration
# ──────────────────────────────────────────────────────────────
@flow(
    name="distributed-reindex",
    task_runner=DaskTaskRunner(),   # 🔧 tweak cluster config as needed
)
def reindex(
    max_slices: int = 8,
    src_index: str = "old-index",
    dest_index: str = "new-index",
) -> int:
    """
    Entry point you deploy / schedule.
    Fans out over `max_slices` slices so the whole job is parallelisable.
    """
    slice_futures = [
        etl_slice_flow.submit(sid, max_slices, src_index, dest_index)
        for sid in range(max_slices)
    ]

    totals = [f.result() for f in slice_futures]
    total_copied = sum(totals)

    get_run_logger().info(f"ALL DONE – copied {total_copied} docs 🚀")
    return total_copied


