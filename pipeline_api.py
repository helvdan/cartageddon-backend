from fastapi import APIRouter
from pydantic import BaseModel


class PipelineStage(BaseModel):
    id: str
    target: str | None = None


class PipelineStages(BaseModel):
    # Each group runs in parallel; groups follow pipeline execution order.
    stages: list[list[PipelineStage]]


def create_pipeline_router(pipeline):
    router = APIRouter(prefix="/pipeline", tags=["pipeline"])
    storage_targets = {"loading_db": "Postgres", "loading_s3": "MinIO S3"}

    @router.get("/stages", response_model=PipelineStages)
    async def get_pipeline_stages():
        groups = []
        for item in pipeline.STAGES_ORDER:
            stages = item if isinstance(item, tuple) else (item,)
            groups.append([
                PipelineStage(id=stage.postfix, target=storage_targets.get(stage.postfix))
                for stage in stages
            ])
        return PipelineStages(stages=groups)

    return router
