from secrets import token_hex

from rest_framework.serializers import ModelSerializer
from .models import Change


class DocumentBase(ModelSerializer):
    @classmethod
    def get_document_id(cls, instance):
        if cls.Meta.single_document:
            return cls.Meta.document_id
        else:
            return "{}:{}".format(cls.Meta.document_id, instance.pk)

    @classmethod
    def wrap_content_with_metadata(cls, document_id, doc, revision, revisions):
        if isinstance(doc, list):
            doc = {
                "content": doc
            }

        doc["_id"] = document_id
        doc["_rev"] = revision
        if revisions:
            doc["_revisions"] = {
                "ids": [r.split('-')[1] for r in revisions],
                "start": 1  # TODO: what the ?
            }

        return doc

    @classmethod
    def get_document_content(cls, doc_id, revision, revisions):
        Model = cls.Meta.model
        if cls.Meta.single_document:
            serializer = cls(Model.objects.all(), many=True)
            return cls.wrap_content_with_metadata(doc_id, serializer.data, revision, revisions)
        else:
            entity_id = ":".join(doc_id.split(':')[1:])
            entity = Model.objects.get(pk=entity_id)
            serializer = cls(entity)
            return cls.wrap_content_with_metadata(doc_id, serializer.data, revision, revisions)

    @classmethod
    def on_change(cls, instance, **kwargs):
        Change.objects.create(
            document_id=cls.get_document_id(instance),
            revision=f'1-{token_hex(8)}',
        )

    @classmethod
    def on_delete(cls, instance, **kwargs):
        Change.objects.create(
            document_id=cls.get_document_id(instance),
            revision=f'1-{token_hex(8)}',
            deleted=True
        )
