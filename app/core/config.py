import os

class Settings:
    TEST = os.getenv("TEST", "null")

settings = Settings()