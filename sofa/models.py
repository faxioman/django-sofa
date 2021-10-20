from django.contrib.contenttypes.models import ContentType
from django.db import models

from sofa.loader import get_class_by_document_id
from .managers import ChangeManager


class Change(models.Model):
    document_id = models.CharField(max_length=128, db_index=True)
    revision = models.CharField(max_length=64)

    #TODO: what to do when the document class is deleted. Probably we should set deleted to true... how?
    deleted = models.PositiveIntegerField(default=0)

    objects = ChangeManager()

    class Meta:
        index_together = (('document_id', 'revision'),)

    def get_document(self, request):
        document_class = get_class_by_document_id(self.document_id)
        return document_class.get_document_content(self.document_id, self.revision, [], request)


class ReplicationLog(models.Model):
    document_id = models.CharField(max_length=128, unique=True)
    replicator = models.CharField(max_length=64)
    version = models.PositiveIntegerField()


class ReplicationHistory(models.Model):
    replication_log = models.ForeignKey(ReplicationLog, related_name='history', on_delete=models.CASCADE)
    session_id = models.CharField(max_length=64)
    last_seq = models.PositiveIntegerField()
