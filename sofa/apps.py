from django.apps import AppConfig


class SofaConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'sofa'

    def ready(self):
        from .loader import load
        load()