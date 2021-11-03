from secrets import token_hex

from rest_framework.serializers import ModelSerializer
from rest_framework.renderers import JSONRenderer
from .models import Change

document_renderer = JSONRenderer()


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
                "value": doc
            }

        doc["_id"] = document_id
        doc["_rev"] = revision
        if revisions:
            doc["_revisions"] = {
                "ids": [r for r in revisions],
                "start": len(revisions)
            }

        return doc

    @classmethod
    def get_document_content(cls, doc_id, revision, revisions, request):
        if cls.Meta.single_document:
            serializer = cls(cls.get_queryset(request), many=True)
            return cls.wrap_content_with_metadata(doc_id, serializer.data, revision, revisions)
        else:
            entity_id = ":".join(doc_id.split(':')[1:])
            entity = cls.get_queryset(request).get(pk=entity_id)
            serializer = cls(entity)
            return cls.wrap_content_with_metadata(doc_id, serializer.data, revision, revisions)

    @classmethod
    def get_document_content_as_json(cls, doc_id, revision, revisions, request):
        document_content = cls.get_document_content(doc_id, revision, revisions, request)
        return document_renderer.render(document_content).decode('utf-8')

    @classmethod
    def on_change(cls, instance, **kwargs):
        Change.objects.create(
            document_id=cls.get_document_id(instance),
            revision=instance.__ds_revision if hasattr(instance, '__ds_revision') and instance.__ds_revision else token_hex(8),
        )

    @classmethod
    def on_delete(cls, instance, **kwargs):
        Change.objects.create(
            document_id=cls.get_document_id(instance),
            revision=instance.__ds_revision if hasattr(instance, '__ds_revision') and instance.__ds_revision else token_hex(8),
            deleted=True
        )

    @classmethod
    def get_queryset(cls, request=None):
        Model = cls.Meta.model
        return Model.objects.all()
