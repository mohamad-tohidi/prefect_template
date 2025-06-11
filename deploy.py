"""
Run once (or per commit) to register / update the deployment.
"""
from reindex import reindex

if __name__ == "__main__":
    reindex.from_source(
        source="https://github.com/mohamad-tohidi/prefect_tutorial.git",
        entrypoint="reindex.py:reindex",
    ).deploy(
        name="etl-demo",
        work_pool_name="etl",
        parameters={"max_slices": 8},
        tags=["elasticsearch-etl"],
    )
