import logging
import os
import pickle
import sys
from pathlib import Path
from typing import Optional

import anyio
import polars as pl
from fastapi import FastAPI, APIRouter, HTTPException, WebSocket, WebSocketDisconnect, status, Request
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from watchfiles import awatch  # Встроено в uvicorn[standard]

# 1. Путь к общей папке проекта
root_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(root_dir))

# 2. Путь к самой папке соседа
cartageddon_dir = root_dir / "cartageddon"
sys.path.append(str(cartageddon_dir))


from utils import get_context_list, get_runtime_context

from cartageddon.pipeline import Pipeline


log_file = Path("/logs/app.log")

logging.basicConfig(level=logging.DEBUG)

pipeline_formatter = logging.Formatter("%(asctime)s [%(name)s] [%(levelname)s] %(message)s")
file_handler = logging.FileHandler(log_file, encoding="utf-8")
file_handler.setFormatter(pipeline_formatter)
file_handler.setLevel(logging.DEBUG)

WHITELISTED_LOGGERS = ("PIPELINE", "ARTIFACT", "EXTRACT", "DEDUP", "JOIN", "INTEGRITY", "LOAD_S3", "LOAD_DB")

for logger_name in WHITELISTED_LOGGERS:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    if not logger.handlers:
        logger.addHandler(file_handler)


app = FastAPI(title="Parquet Viewer")

app.state.current_context = "sample"

origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,            # Разрешаем запросы с нашего React-сайта
    allow_credentials=True,
    allow_methods=["*"],              # Разрешаем все методы: GET, POST, PUT, DELETE
    allow_headers=["*"],              # Разрешаем любые заголовки
)

STAGE_MAP = {}
ORDERED_POSTFIXES = []
for item in Pipeline.STAGES_ORDER:
    if isinstance(item, tuple):
        for sub_item in item:
            if hasattr(sub_item, 'postfix'):
                ORDERED_POSTFIXES.append(sub_item.postfix)
                STAGE_MAP[sub_item.postfix] = item
    else:
        if hasattr(item, 'postfix'):
            ORDERED_POSTFIXES.append(item.postfix)
            STAGE_MAP[item.postfix] = item


class ContextRequest(BaseModel):
    preset: str  # Ожидает структуру {"context": "sample"}


@app.get("/context", response_class=JSONResponse)
def get_context(request: Request) -> JSONResponse:
    available_contexts = {}

    for context_name in get_context_list():
        available_contexts[context_name] = context_name == request.app.state.current_context

    return JSONResponse(
        content={
            "contexts": available_contexts
        }
    )


# 2. Обновляем эндпоинт
@app.post("/context/change/")
def change_context(request_data: ContextRequest, request: Request):
    context_type = request_data.preset.lower()

    if context_type not in ["sample", "test"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid context type. Must be 'sample' or 'test'."
        )

    request.app.state.current_context = context_type

    return {"success": True, "current_context": context_type}


def _parse_parquet(file_path):
    df = pl.read_parquet(file_path, n_rows=100)
    headers = [[str(name), str(dtype)] for name, dtype in df.schema.items()]
    rows = df.rows()
    return headers, rows


def _parse_pickle(file_path):
    with open(file_path, "rb") as f:
        pickle_data = pickle.load(f)

    rows = pickle_data[:100]
    return [], rows


def _get_stage_files(stage_name: str, data_dir: str) -> list:
    """Вспомогательная функция сбора файлов для конкретной стадии"""
    # Ищем файлы, которые содержат постфикс стадии (например, *.extracted.*)
    data_dir = Path(data_dir).resolve()
    all_files = data_dir.glob(f"*.{stage_name}.*")
    # Возвращаем только имена файлов, отсортированные по алфавиту
    return sorted([f.name for f in all_files])


@app.websocket("/ws/artifacts/{stage_name}")
async def websocket_artifacts_endpoint(websocket: WebSocket, stage_name: str):
    context = websocket.app.state.current_context
    ctx = get_runtime_context(context)

    await websocket.accept()
    try:
        # 1. Сразу при подключении отправляем текущее состояние папки
        await websocket.send_json(_get_stage_files(stage_name, ctx.work_dir))

        # 2. Запускаем асинхронную слежку за директорией
        async for changes in awatch(ctx.work_dir):
            # Как только watchfiles заметил изменения (создание, удаление, изменение файлов)
            # отправляем свежий отфильтрованный список на фронтенд
            await websocket.send_json(_get_stage_files(stage_name, ctx.work_dir))

    except WebSocketDisconnect:
        print(f"[WS Artifacts] Отключился клиент для стадии: {stage_name}")
    except Exception as e:
        print(f"[WS Artifacts] Ошибка: {e}")


@app.websocket("/ws/logs")
async def websocket_logs_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        # Открываем файл в асинхронном режиме чтения
        async with await anyio.open_file(log_file, mode="r") as f:
            # Сначала сдвигаем указатель в конец файла,
            # чтобы читать только новые логи (или уберите seek, если нужны старые тоже)
            await f.seek(0, 2)

            while True:
                line = await f.readline()
                if not line:
                    # Если новых строк нет, делаем микро-паузу, чтобы не вешать процессор
                    await anyio.sleep(0.1)
                    continue

                # Отправляем строчку на фронтенд (убираем лишние переносы \n)
                await websocket.send_text(line.strip())

    except WebSocketDisconnect:
        print("[WS] Клиент отключился от логов")
    except Exception as e:
        print(f"[WS] Ошибка стриминга логов: {e}")


@app.get("/artifacts/{file}", response_class=JSONResponse)
async def view_file(request: Request, file: str):
    ctx = get_runtime_context(request.app.state.current_context)
    file_path = (Path(ctx.work_dir) / file).resolve()

    headers = []
    rows = []
    if file_path.suffix.lower() == ".parquet":
        headers, rows = _parse_parquet(file_path)
    elif file_path.suffix.lower() == ".pkl":
        headers, rows = _parse_pickle(file_path)

    return JSONResponse(
        content=jsonable_encoder(
            {
                "headers": headers,
                "rows": rows
            }
        )
    )


@app.delete("/artifacts/{file}", response_class=JSONResponse)
async def delete_file(request: Request, file: str):
    ctx = get_runtime_context(request.app.state.current_context)
    file_path = (Path(ctx.work_dir) / file).resolve()
    os.remove(file_path)
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"success": True, "message": f"Файл {file} успешно удален."}
    )


@app.post("/pipeline/run", response_class=JSONResponse)
@app.post("/pipeline/run/{stage_name}/{artifact}", response_class=JSONResponse)
def run_pipeline(
    request: Request,
    stage_name: Optional[str] = None,
    artifact: Optional[str] = None,
):
    logger = logging.getLogger("PIPELINE")
    logger.debug("Это сообщение запишется в файл")

    oc_table_names = None
    active_stage_names = None

    if artifact:
        oc_table_name = artifact.split(".")[0]
        oc_table_names = {oc_table_name}

    if stage_name:
        i = ORDERED_POSTFIXES.index(stage_name)
        active_stage_names = set()
        for stage_postfix in ORDERED_POSTFIXES[:i + 1]:
            active_stage_names.add(STAGE_MAP[stage_postfix].name)

    try:
        ctx = get_runtime_context(request.app.state.current_context)
        pipeline = Pipeline(ctx, run_default_hooks=False)
        pipeline.execute(
            oc_table_names=oc_table_names,
            active_stage_names=active_stage_names
        )
    except Exception as e:
        return JSONResponse(
            {
                "error": str(e)
            },
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
    return JSONResponse(
        {"success": True},
        status_code=status.HTTP_200_OK
    )


@app.get("/artifacts/{file}/previous", response_class=JSONResponse)
async def get_table_history(file: str):
    oc_table_name = file.split(".")[0]
    all_files = list(DATA_DIR.glob(f"{oc_table_name}.*"))

    def get_stage_index(filename: str) -> int:
        """Вспомогательная функция для определения веса стадии при сортировке"""
        # Разбиваем имя (например, oc_product.extracted.pkl) и ищем совпадение с ORDERED_POSTFIXES
        parts = filename.split(".")
        for postfix in ORDERED_POSTFIXES:
            if postfix in parts:
                return ORDERED_POSTFIXES.index(postfix)
        return float('inf') # Если стадия неизвестна, убираем её в самый конец

    # 2. Сортируем все найденные файлы согласно нашему ORDERED_POSTFIXES
    all_files.sort(key=lambda x: get_stage_index(x.name))

    data = []
    for artifact in all_files:
        # Пропускаем текущий файл, так как нам нужны только *предыдущие*
        if artifact.name == file:
            continue

        headers = []
        rows = []
        if artifact.name.endswith('.pkl'):
            headers, rows = _parse_pickle(artifact)
        elif artifact.name.endswith('.parquet'):
            headers, rows = _parse_parquet(artifact)

        data.append({
            "artifact": artifact.name,
            "data": {
                "headers": headers,
                "rows": rows
            }
        })

    return JSONResponse(
        content=jsonable_encoder(data)
    )


router = APIRouter(prefix="/stages")


STAGE_MAPPING = {
    "extracted": "EXTRACT",
    "normalized": "NORMALIZE",
    "deduplicated": "DEDUP",
    "checked": "INTEGRITY",
    "joined": "JOIN",
    "loading_s3": "LOAD",
    "loading_db": "LOAD"
}


STAGE_ORDER = ["EXTRACT", "NORMALIZE", "DEDUP", "INTEGRITY", "JOIN", "LOAD"]


class StageController:

    @staticmethod
    @router.get("/{stage_name}/")
    async def get_stage_files(stage_name: str):
        extensions = ("*.parquet", "*.pkl")
        all_files = []
        for ext in extensions:
            all_files.extend(DATA_DIR.glob(ext))

        stage_files = []
        for file_path in all_files:
            if stage_name == str(file_path).split('.')[-2]:
                stage_files.append(file_path.name)

        stage_files.sort()
        return stage_files


app.include_router(router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True, log_level="debug")
