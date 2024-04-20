from configs.settings import env_parameters

if env_parameters.PROD_MODE:
    from core.wi18n import RedisI18nMiddleware, WI18N
    from redis.asyncio import Redis

    r = Redis(
        host=env_parameters.REDIS_DB_HOST,
        port=env_parameters.REDIS_DB_PORT,
        db=env_parameters.REDIS_DB_DATABASE,
    )
    i18n = RedisI18nMiddleware(
        i18n=WI18N(
            default_locale="ru", domain="CONTENT_STORAGE", redis=r, ab_default="ab_0"
        )
    )
else:
    from aiogram.utils.i18n import I18n, FSMI18nMiddleware

    i18n = FSMI18nMiddleware(
        I18n(path="locales", default_locale="ru", domain="messages")
    )
