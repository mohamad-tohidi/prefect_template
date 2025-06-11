# deploy.py  – run once from any machine with Prefect installed
from prefect import flow

from reindex import reindex          

if __name__ == "__main__":
    reindex.from_source(             # <-- tells workers *where* to clone from
        source="https://github.com/mohamad-tohidi/prefect_tutorial.git",
        entrypoint="reindex.py:reindex",
    ).deploy(
        name="etl-demo",             
        work_pool_name="etl",        
        parameters={"max_slices": 8},
        tags=["elasticsearch-etl"],
    )
