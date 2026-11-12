from contextlib import contextmanager


@contextmanager
def s3_client(ctx):
    import boto3
    from botocore.config import Config

    if not all((ctx.aws_access_key_id, ctx.aws_secret_access_key, ctx.aws_bucket_name)):
        raise ValueError("Настройки подключения к S3 не заполнены в контексте.")

    config = {key: value for key, value in ctx.get_s3_config().items() if value is not None}
    client = boto3.session.Session().client(
        "s3",
        **config,
        config=Config(
            connect_timeout=3,
            read_timeout=5,
            retries={"total_max_attempts": 1},
            s3={"addressing_style": "path"},
        ),
    )
    try:
        yield client
    finally:
        client.close()


def get_s3_state(ctx):
    with s3_client(ctx) as client:
        result = client.list_objects_v2(Bucket=ctx.aws_bucket_name, MaxKeys=1)
        return "filled" if result.get("Contents") else "empty"


def clear_s3_objects(ctx):
    """Delete current objects, preserving the bucket and historical versions."""
    deleted = 0
    with s3_client(ctx) as client:
        pages = client.get_paginator("list_objects_v2").paginate(
            Bucket=ctx.aws_bucket_name, PaginationConfig={"PageSize": 1000},
        )
        for page in pages:
            objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if not objects:
                continue
            result = client.delete_objects(
                Bucket=ctx.aws_bucket_name, Delete={"Objects": objects, "Quiet": True},
            )
            if result.get("Errors"):
                raise RuntimeError("Не удалось удалить часть объектов S3.")
            deleted += len(objects)
    return deleted
