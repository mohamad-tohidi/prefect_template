from prefect import flow, task, get_run_logger
from prefect_dask import DaskTaskRunner          
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv
import os

load_dotenv()


@task(
    retries=3,
    retry_delay_seconds=60,
    tags=["elasticsearch-etl"],        
    task_run_name="slice-{slice_id}"
)
def etl_slice(slice_id: int, max_slices: int,
              src_index: str, dest_index: str) -> int:
    """
    Pull *one* slice from `src_index`, transform, and bulk-index to `dest_index`.
    Returns how many docs we wrote so the flow can log totals.
    """
    es = Elasticsearch(
        hosts=[{
            'host': os.getenv('ES_URL'), 
            'port': 9200, 
            'scheme': 'https' 
        }], 
        basic_auth = (os.getenv('ES_USER'), os.getenv('ES_PASS')), 
        verify_certs = False ,
        timeout=1000
    )
    log = get_run_logger()
    wrote = 0

    # 1️⃣ extract -------------------------------------------------------------
    scroll = es.search(
        index=src_index,
        scroll="2m",
        size=1_000,
        body={
            "slice": {"id": slice_id, "max": max_slices},
            "query": {"match_all": {}}
        },
    )

    # 2️⃣ transform + load loop ----------------------------------------------
    while True:
        hits = scroll["hits"]["hits"]
        if not hits:
            break

        actions = [
            {
                "_index": dest_index,
                "_id": d["_id"],                         # choose your own PK
                "_source": transform(d["_source"]),      # <-- your business logic
            }
            for d in hits
        ]
        helpers.bulk(es, actions, request_timeout=120)
        wrote += len(actions)

        scroll = es.scroll(
            scroll_id=scroll["_scroll_id"],
            scroll="2m"
        )

    log.info(f"slice {slice_id} done (wrote {wrote})")
    return wrote


def transform(doc: dict) -> dict:
    """Very silly transform placeholder"""
    doc["copied_at"] = "2025-06-11"
    return doc


@flow(
    name="distributed-reindex",
    task_runner=DaskTaskRunner()       
)
def reindex(max_slices: int = 8,
           src_index: str = "old-index",
           dest_index: str = "new-index"):
    totals = etl_slice.map(
        list(range(max_slices)),                   
        [max_slices] * max_slices,
        [src_index] * max_slices,
        [dest_index] * max_slices,
    )
    n = sum(totals)
    get_run_logger().info(f"ALL DONE – copied {n} docs 🚀")

