from django.contrib.contenttypes.models import ContentType
from django.db import models


class Change(models.Model):
    document_id = models.CharField(max_length=128, db_index=True)
    revision = models.CharField(max_length=64)

    #TODO: what to do when the document class is deleted. Probably we should set deleted to true... is this possible?
    deleted = models.BooleanField(default=False)

    class Meta:
        index_together = (('document_id', 'revision'),)
