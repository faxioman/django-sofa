from rest_framework.serializers import ModelSerializer
from .models import Change
from django.db.models.fields import BigIntegerField
import random


class DocumentBase(ModelSerializer):
    @classmethod
    def get_document_id(cls, instance):
        if cls.Meta.single_document:
            return cls.Meta.document_id
        else:
            return "{}:{}".format(cls.Meta.document_id, instance.pk)

    @classmethod
    def on_change(cls, instance, **kwargs):
        Change.objects.create(
            document_id=cls.get_document_id(instance),
            revision=random.randint(-BigIntegerField.MAX_BIGINT, BigIntegerField.MAX_BIGINT),
            document_class_id=cls.Meta.document_id,
            object_id=instance.pk,
        )

    @classmethod
    def on_delete(cls, instance, **kwargs):
        Change.objects.create(
            document_id=cls.get_document_id(instance),
            revision=random.randint(-BigIntegerField.MAX_BIGINT, BigIntegerField.MAX_BIGINT),
            document_class_id=cls.Meta.document_id,
            object_id=instance.pk,
            deleted=True
        )
