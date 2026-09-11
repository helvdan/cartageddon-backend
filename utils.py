import yaml
from pathlib import Path
from typing import Any, Dict, List

from cartageddon.common import RunTimeContext


CONTEXT_DIR = Path(".context")


def get_context_list() -> List[str]:
    """Возвращает список имен доступных контекстов.

    Ищет скрытые файлы в папке .contexts (например, '.sample', '.test')
    и возвращает их чистые имена ['sample', 'test'].
    """

    context_names = []

    # Итерируемся по всем файлам в папке
    for file_path in CONTEXT_DIR.iterdir():
        # Нам нужны только файлы, имя которых начинается с точки
        if file_path.is_file():
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    yaml.safe_load(f)
            except (yaml.YAMLError, UnicodeDecodeError):
                continue  # Пропускаем битые файлы

            context_names.append(file_path.name.split('.')[0])

    return sorted(context_names)


def get_runtime_context(context_name: str) -> RunTimeContext:
    """Достает YAML-файл из .context/.

    <context_name> и парсит в словарь.
    """
    contexts_dir = Path(".context")
    file_name = f"{context_name}.yml"
    file_path = contexts_dir / file_name

    if not file_path.is_file():
        raise FileNotFoundError(
            f"Файл контекста '{file_name}' не найден в папке '{contexts_dir}'."
        )

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            return RunTimeContext(**data)
    except yaml.YAMLError as e:
        raise ValueError(
            f"Ошибка парсинга YAML в файле '{file_path}': {e}"
        ) from e


if __name__ == '__main__':
    from cartageddon.common import RunTimeContext

    ctx = get_runtime_context("sample")

    # Разворачиваем словарь прямо в ваш класс контекста
    print(ctx)
