import logging
import pickle
from pathlib import Path

import polars as pl
from fastapi import FastAPI, Request, APIRouter, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import os
import sys
from pathlib import Path

# 1. Путь к общей папке проекта (~/zoovostorg/)
root_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(root_dir))

# 2. Путь к самой папке соседа (~/zoovostorg/cartageddon/)
cartageddon_dir = root_dir / "cartageddon"
sys.path.append(str(cartageddon_dir))


from cartageddon.pipeline import Pipeline
from cartageddon.common import RunTimeContext


app = FastAPI(title="Parquet Viewer")

# Подставляем ваш актуальный путь к папке с результатами ETL
DATA_DIR = Path("/tmp/cartageddon/").resolve()
TEMPLATES_DIR = Path("./templates").resolve()

app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory=TEMPLATES_DIR)


def get_structured_menu():
    """
    Сканирует папку и распределяет файлы по ETL-стадиям.
    Возвращает словарь, где ключи — это стадии в строгом порядке.
    """
    # 1. Задаем строгий порядок стадий
    stages = ["extracted", "normalized", "deduplicated", "checked", "joined", "loading_s3", "loading_db"]

    # Инициализируем пустую структуру меню
    menu = {stage: [] for stage in stages}
    menu["other"] = []  # Для файлов, которые не подошли ни под одну стадию

    # 2. Сканируем директорию (берем и parquet, и pkl)
    extensions = ("*.parquet", "*.pkl")
    all_files = []
    for ext in extensions:
        all_files.extend(DATA_DIR.glob(ext))

    # Сортируем все файлы по алфавиту для красоты
    all_files = sorted(all_files, key=lambda f: f.name)

    # 3. Распределяем файлы по бакетам на основе их имен
    for file_path in all_files:
        name_lower = file_path.name.lower()
        matched = False

        for stage in stages:
            # Ищем маркер стадии в имени файла (например, '.extracted.')
            if f".{stage}." in name_lower or name_lower.endswith(f".{stage}"):
                menu[stage].append(file_path.name)
                matched = True
                break

        if not matched:
            menu["other"].append(file_path.name)

    # Удаляем пустые стадии, чтобы не плодить пустые блоки в интерфейсе
    return {stage: files for stage, files in menu.items() if files}


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="index_schema.html",
        context={}
    )


@app.get("/menu", response_class=JSONResponse)
async def get_menu():
    structured_data = get_structured_menu()
    return JSONResponse(structured_data)


@app.get("/view", response_class=HTMLResponse)
async def view_file(request: Request, file: str):
    headers = None
    rows = None
    error_message = None

    # Безопасно собираем и проверяем путь к файлу
    file_path = (DATA_DIR / file).resolve()
    print(file_path)

    if not str(file_path).startswith(str(DATA_DIR)):
        error_message = "Доступ к файлу запрещен."
    elif not file_path.exists():
        error_message = f"Файл {file} не найден."
    elif file_path.suffix.lower() not in [".parquet", ".pkl"]:
        error_message = "Неподдерживаемый формат файла. Разрешены только .parquet и .pkl"
    else:
        try:
            df = None

            # Чтение PARQUET
            if file_path.suffix.lower() == ".parquet":
                df = pl.read_parquet(file_path, n_rows=100)

            # Чтение PICKLE
            elif file_path.suffix.lower() == ".pkl":
                with open(file_path, "rb") as f:
                    pickle_data = pickle.load(f)

                raw_rows = pickle_data[:100]

                # Рендерим ОСОБЫЙ шаблон для сырых pickle-данных
                return templates.TemplateResponse(
                    request=request,
                    name="table_pickle.html",
                    context={
                        "rows": raw_rows
                    }
                )

            if df is not None:
                # df = fix_cyrillic(df)
                # Готовим заголовки и строки для шаблона table.html
                headers = [(name, str(dtype)) for name, dtype in df.schema.items()]
                rows = df.rows()
            else:
                error_message = "Файл пуст или его структура не поддерживается."

        except Exception as e:
            error_message = f"Ошибка при чтении файла: {str(e)}"

    # Возвращаем исключительно фрагмент table.html с нужным контекстом
    return templates.TemplateResponse(
        request=request,
        name="table.html",
        context={
            "headers": headers,
            "rows": rows,
            "error_message": error_message
        }
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
    @router.delete("/{stage_name}/")
    async def delete_stage_files(stage_name: str):
        print(stage_name)
        allowed_stages = ["extracted", "normalized", "deduplicated", "checked", "joined"]
        if stage_name not in allowed_stages:
            raise HTTPException(status_code=400, detail=f"Стадию '{stage_name}' нельзя удалить.")

        structured_menu = get_structured_menu()
        print('******************')
        print(structured_menu)
        print('******************')
        files_to_delete = structured_menu.get(stage_name, [])

        if not files_to_delete:
            raise HTTPException(status_code=404, detail="Файлов для удаления не найдено.")

        deleted_count = 0
        for file_name in files_to_delete:
            file_path = (DATA_DIR / file_name).resolve()
            if str(file_path).startswith(str(DATA_DIR)) and file_path.exists():
                file_path.unlink()
                deleted_count += 1

        return {"status": "success", "message": f"Удалено файлов: {deleted_count}"}

    @staticmethod
    @router.put("/{stage_name}/")
    async def run_pipeline_from_stage(stage_name: str):
        if stage_name == "source-db":
            active_stages = None
            target_stage_internal = "SOURCE"
        else:
            if stage_name not in STAGE_MAPPING:
                raise HTTPException(status_code=400, detail=f"Неизвестная стадия: {stage_name}")

            # 1. Логика отсечения: определяем, с какого момента запускать пайплайн
            target_stage_internal = STAGE_MAPPING[stage_name]
            try:
                target_index = STAGE_ORDER.index(target_stage_internal)
                # Берем целевую стадию и ВСЕ, что идет строго ПОСЛЕ нее
                active_stages = set(STAGE_ORDER[target_index:])
            except ValueError:
                active_stages = {"NORMALIZE", "DEDUP", "INTEGRITY", "JOIN", "LOAD"}

        # 2. Инициализация логирования внутри PUT метода
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s [%(name)s] [%(levelname)s] %(message)s",
            force=True  # Перезапишет глобальный конфиг логов FastAPI на время выполнения таски
        )
        logger = logging.getLogger("FastAPI_ETL_Runner")
        logger.info(f"Запуск ETL пайплайна. Активные стадии: {active_stages}")

        try:
            # 3. Конфигурация контекста из вашего запроса
            ctx = RunTimeContext(
                work_dir="/tmp/cartageddon",
                single_language=True,
                chunk_size=5000,
                oc_user="opencart_user",
                oc_password="opencart_password",
                oc_host="localhost",
                oc_port=3307,
                oc_database="opencart_db",
                pg_user="django_user",
                pg_password="django_password",
                pg_host="localhost",
                pg_port=5438,
                pg_database="django_db",
                aws_access_key_id="minio_admin",
                aws_secret_access_key="minio_strong_password",
                aws_endpoint_url="http://localhost:9000",
                aws_region_name="us-east-1",
                aws_bucket_name="dev",
                open_cart_image_dir="/home/lmakeev/zoovostorg/cartageddon/sample/data",
                s3_max_workers=2
            )

            # 4. Сборка и исполнение пайплайна с динамическим набором стадий
            pipeline = Pipeline(ctx, run_default_hooks=False)
            pipeline.execute(
                oc_table_names={
                    'oc_product',
                    'oc_product_description',
                    'oc_review',
                    'oc_manufacturer',
                    'oc_stock_status',
                    'oc_tax_class',
                    'oc_length_class',
                    'oc_weight_class'
                },
                active_stage_names=active_stages
            )

            return JSONResponse({
                "status": "success",
                "message": f"Пайплайн успешно выполнен начиная со стадии {target_stage_internal}.",
                "executed_stages": list(active_stages)
            })

        except Exception as e:
            logger.error(f"Ошибка при выполнении пайплайна: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=500,
                detail=f"Ошибка выполнения ETL: {str(e)}"
            )


app.include_router(router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
