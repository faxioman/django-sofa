from django.contrib.contenttypes.models import ContentType
from django.db import models


class Change(models.Model):
    document_id = models.CharField(max_length=128)
    revision = models.BigIntegerField()

    #TODO: what to do when the document class is deleted. Probably we should set deleted to true... is this possible?
    document_class_id = models.CharField(max_length=16)
    object_id = models.TextField(null=True, db_index=True)

    deleted = models.BooleanField(default=False)
