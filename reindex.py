import os
from datetime import date
from typing import List, Dict, Any

from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers
from prefect import flow, task, get_run_logger
from prefect_dask import DaskTaskRunner

load_dotenv()  # loads ES_URL, ES_USER, ES_PASS from .env
TODAY = date.today().isoformat()


# ──────────────────────────────────────────────────────────────
# ↓↓↓── LOW-LEVEL TASKS (you’ll fill in the TODOs) ──↓↓↓
# ──────────────────────────────────────────────────────────────
@task(retries=2, retry_delay_seconds=30, tags=["embeddings"])
def generate_qwen_embedding(doc: Dict[str, Any]) -> Dict[str, Any]:
    # TODO: call Qwen model and attach to doc
    return doc


@task(retries=2, retry_delay_seconds=30, tags=["embeddings"])
def generate_e5_embedding(doc: Dict[str, Any]) -> Dict[str, Any]:
    # TODO
    return doc


@task(retries=2, retry_delay_seconds=30, tags=["embeddings"])
def generate_bge_embedding(doc: Dict[str, Any]) -> Dict[str, Any]:
    # TODO
    return doc


@task(tags=["nlp"])
def extract_keywords(doc: Dict[str, Any]) -> Dict[str, Any]:
    # TODO
    return doc


@task(tags=["nlp"])
def extract_ner(doc: Dict[str, Any]) -> Dict[str, Any]:
    # TODO
    return doc


@task(tags=["aug"])
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


# ──────────────────────────────────────────────────────────────
#  Sub-flow that handles one document
# ──────────────────────────────────────────────────────────────
@flow(name="process-document")
def process_document(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    All logic for one document lives here.
    Branching/conditionals are just normal Python.
    Each step is an independent Prefect task.
    """
    # ── Always add embeddings
    doc = generate_qwen_embedding(doc)
    doc = generate_e5_embedding(doc)
    doc = generate_bge_embedding(doc)

    # ── Run keyword/NER extraction
    doc = extract_keywords(doc)
    doc = extract_ner(doc)

    # ── Example conditional: only augment & translate if language == "fa"
    lang = doc.get("language")
    if lang and lang.lower() == "fa":
        doc = generate_question_augmentation(doc)
        doc = translate_question(doc)
        doc = translate_answer(doc)

    # mark when we touched it
    doc["processed_at"] = TODAY
    return doc


# ──────────────────────────────────────────────────────────────
#  Tasks for talking to Elasticsearch
# ──────────────────────────────────────────────────────────────
@task(tags=["elasticsearch"])
def fetch_slice(
    slice_id: int, max_slices: int, src_index: str
) -> List[Dict[str, Any]]:
    es = Elasticsearch(
        hosts=[{"host": os.getenv("ES_URL"), "port": 9200, "scheme": "https"}],
        basic_auth=(os.getenv("ES_USER"), os.getenv("ES_PASS")),
        verify_certs=False,
        request_timeout=1000,
    )

    scroll = es.search(
        index=src_index,
        scroll="2m",
        size=1_000,
        body={"slice": {"id": slice_id, "max": max_slices}, "query": {"match_all": {}}},
    )
    all_docs = []

    while True:
        hits = scroll["hits"]["hits"]
        if not hits:
            break
        all_docs.extend([{"_id": h["_id"], **h["_source"]} for h in hits])
        scroll = es.scroll(scroll_id=scroll["_scroll_id"], scroll="2m")

    return all_docs


@task(tags=["elasticsearch"])
def bulk_index(
    docs: List[Dict[str, Any]], dest_index: str
) -> int:  # returns # written
    es = Elasticsearch(
        hosts=[{"host": os.getenv("ES_URL"), "port": 9200, "scheme": "https"}],
        basic_auth=(os.getenv("ES_USER"), os.getenv("ES_PASS")),
        verify_certs=False,
        request_timeout=1000,
    )

    actions = [
        {"_index": dest_index, "_id": d["_id"], "_source": d} for d in docs
    ]
    helpers.bulk(es, actions, request_timeout=120)
    return len(actions)


# ──────────────────────────────────────────────────────────────
#  Sub-flow for one slice
# ──────────────────────────────────────────────────────────────
@flow(name="etl-slice")
def etl_slice_flow(
    slice_id: int,
    max_slices: int,
    src_index: str,
    dest_index: str,
) -> int:
    log = get_run_logger()

    # 1️⃣ Pull raw docs
    raw_docs = fetch_slice(slice_id, max_slices, src_index)

    if not raw_docs:
        log.info(f"slice {slice_id} had 0 docs")
        return 0

    # 2️⃣ Run the per-document pipeline **in parallel**
    processed_docs = process_document.map(raw_docs)

    # 3️⃣ Index back to ES
    wrote = bulk_index(processed_docs, dest_index)
    log.info(f"slice {slice_id} finished – wrote {wrote} docs")

    return wrote


# ──────────────────────────────────────────────────────────────
#  TOP-LEVEL orchestration flow
# ──────────────────────────────────────────────────────────────
@flow(
    name="distributed-reindex",
    task_runner=DaskTaskRunner(),  # pick your cluster config
)
def reindex(
    max_slices: int = 8,
    src_index: str = "old-index",
    dest_index: str = "new-index",
):
    # Launch N → slice sub-flows concurrently
    slice_results = etl_slice_flow.map(
        list(range(max_slices)),
        [max_slices] * max_slices,
        [src_index] * max_slices,
        [dest_index] * max_slices,
    )

    total = sum(slice_results)
    get_run_logger().info(f"ALL DONE – copied {total} docs 🚀")
    return total
