from django.urls import path
from .views import database, index, replication_log, changes, all_docs, bulk_get, revs_diff, document

urlpatterns = [
    path('', index),
    path('db/', database),
    path('db/_local/<replication_id>', replication_log),
    path('db/__replog.<replication_id>', replication_log),
    path('db/_changes', changes),
    path('db/_all_docs', all_docs),
    path('db/_bulk_get', bulk_get),
    path('db/_revs_diff', revs_diff),
    path('db/<document_id>', document)
]
