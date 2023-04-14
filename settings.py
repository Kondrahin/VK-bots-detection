class Settings:
    # API
    BASE_URL = "https://api.vk.com/method"
    ACCESS_TOKEN = ""
    VERSION = "5.131"
    SLEEP_TIME_IN_SECONDS = 0.3


def get_settings() -> Settings:
    return Settings()


settings = get_settings()
