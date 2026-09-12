import asyncio

from fastapi import Depends, HTTPException, status, APIRouter, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import sessionmaker

from cartageddon.common import RunTimeContext
from cartageddon.schema import OpenCartSchema
from utils import get_runtime_context

# 1. Настройка подключения к PostgreSQL (замените на свои данные)
DATABASE_URL = "postgresql+asyncpg://user:password@localhost:5432/dbname"

engine = create_async_engine(DATABASE_URL, echo=True)
async_session = async_sessionmaker(engine, expire_on_commit=False)

router = APIRouter(
    prefix="/infra",
    tags=["infra"]
)


POSTGRES_CHECK_EMPTY_SQL = text("""
    SELECT COALESCE(SUM(n_live_tup), 0) 
    FROM pg_stat_user_tables 
    WHERE schemaname = 'public' AND relname = 'catalog_product';
""")


async def create_dynamic_session(ctx: RunTimeContext):
    """
    Внутренний контекстный менеджер для создания сессии на основе RunTimeContext.
    Используется как для HTTP, так и для WebSocket.
    """
    if not all([ctx.pg_host, ctx.pg_user, ctx.pg_password, ctx.pg_database]):
        raise ValueError("Конфигурация подключения к PostgreSQL не инициализирована в контексте.")

    database_url = (
        f"postgresql+asyncpg://{ctx.pg_user}:{ctx.pg_password}@"
        f"{ctx.pg_host}:{ctx.pg_port}/{ctx.pg_database}"
        f"?command_timeout={ctx.pg_timeout}"
    )

    engine = create_async_engine(
        database_url,
        connect_args={"timeout": ctx.pg_connect_timeout},
        echo=False
    )

    async_session_factory = sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False
    )

    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await engine.dispose()


async def get_db(request: Request):
    ctx: RunTimeContext = get_runtime_context(request.app.state.current_context)

    if not all([ctx.pg_host, ctx.pg_user, ctx.pg_password, ctx.pg_database]):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Конфигурация подключения к PostgreSQL не инициализирована в контексте."
        )

    database_url = (
        f"postgresql+asyncpg://{ctx.pg_user}:{ctx.pg_password}@"
        f"{ctx.pg_host}:{ctx.pg_port}/{ctx.pg_database}"
        f"?command_timeout={ctx.pg_timeout}"
    )

    # Создаем асинхронный движок.
    # Аргумент connect_args передает системный connect_timeout драйверу asyncpg
    engine = create_async_engine(
        database_url,
        connect_args={"timeout": ctx.pg_connect_timeout},
        echo=False  # Поставьте True, если нужно логировать SQL-запросы в консоль
    )

    # Создаем фабрику сессий для текущего движка
    async_session_factory = sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False
    )

    # Открываем сессию, отдаем ее эндпоинту и гарантируем закрытие ресурсов
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            # Важно закрыть движок, так как мы создаем его динамически,
            # чтобы не оставлять висящие пулы соединений.
            await engine.dispose()


# 3. Эндпоинт для выполнения TRUNCATE
@router.post("/postgres/truncate", status_code=status.HTTP_200_OK)
async def truncate_tables(db: AsyncSession = Depends(get_db)):
    oc_schema = OpenCartSchema()
    TABLES_TO_TRUNCATE = oc_schema.get_pg_tables()

    try:
        result = await db.execute(
            text(
                "SELECT table_name "
                "FROM information_schema.tables "
                "WHERE table_schema = 'public';"
            )
        )
        existing_tables = {row[0] for row in result.fetchall()}

        # 3. Фильтруем наш список: оставляем только те таблицы, которые РЕАЛЬНО существуют
        tables_to_truncate = [t for t in TABLES_TO_TRUNCATE if t in existing_tables]

        if tables_to_truncate:
            tables_str = ", ".join(tables_to_truncate)
            query = text(f"TRUNCATE TABLE {tables_str} RESTART IDENTITY CASCADE;")
            await db.execute(query)

        return JSONResponse(
            content={"tables": tables_to_truncate}, status_code=status.HTTP_200_OK
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Критическая ошибка при выполнении операции TRUNCATE: {str(e)}"
        )


@router.websocket("/postgres/ws/health")
async def watch_postgres_health(websocket: WebSocket):
    await websocket.accept()
    ctx: RunTimeContext = get_runtime_context(websocket.app.state.current_context)

    try:
        while True:
            state_response = {}
            try:
                async for db in create_dynamic_session(ctx):
                    result = await db.execute(POSTGRES_CHECK_EMPTY_SQL)
                    total_rows = result.scalar()
                    state_response = {"state": "empty" if total_rows == 0 else "filled"}

            except (OSError, Exception) as err:
                err_msg = str(err).lower()
                is_connection_error = any(
                    word in err_msg
                    for word in
                    ("connection refused", "cannot connect", "timeout", "interfaceerror", "operationalerror", "call failed")
                )
                if is_connection_error:
                    state_response = {"state": "disconnected"}
                else:
                    state_response = {"error": f"Ошибка PostgreSQL: {str(err)}"}

            await websocket.send_json(state_response)
            await asyncio.sleep(3)

    except WebSocketDisconnect:
        pass


if __name__ == "__main__":
    import asyncio
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    import os
    from pathlib import Path

    project_root = Path(__file__).resolve().parent.parent
    os.chdir(project_root)

    # 1. Создаем тестовое приложение FastAPI и регистрируем в нем наш роутер
    test_app = FastAPI()
    test_app.include_router(router)

    # Инициализируем state, который запрашивает ваш вебсокет
    test_app.state.current_context = 'zoovostorg (test)'

    print("--- Запуск теста WebSocket соединения ---")

    # 3. Используем TestClient в контекстном менеджере для работы с websocket
    with TestClient(test_app) as client:
        # Подключаемся к нашему эндпоинту (префикс роутера + путь вебсокета)
        with client.websocket_connect("/infra/postgres/ws/health") as websocket:
            print("Успешно подключились к WebSocket!")

            # Читаем первые 3 сообщения от вебсокета (с интервалом в 3 секунды, как заложено в коде)
            for i in range(1, 4):
                # Получаем JSON-ответ от сервера
                data = websocket.receive_json()
                print(f"[Сообщение {i}] Получен статус от БД: {data}")

    print("--- Тест завершен, соединение закрыто ---")
