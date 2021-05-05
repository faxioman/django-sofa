from django.urls import path
from .views import database, index, replication_log, changes, bulk_get

urlpatterns = [
    path('', index),
    path('db/', database),
    path('db/_local/<replication_id>', replication_log),
    path('db/_changes', changes),
    path('db/_bulk_get', bulk_get),
]
