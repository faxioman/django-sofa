from secrets import token_hex

from rest_framework.serializers import ModelSerializer
from rest_framework.renderers import JSONRenderer
from .models import Change

document_renderer = JSONRenderer()


class DocumentBase(ModelSerializer):
    @classmethod
    def is_single_document(cls):
        if hasattr(cls.Meta, 'single_document'):
            return cls.Meta.single_document
        return False

    @classmethod
    def get_replica_field(cls):
        if hasattr(cls.Meta, 'replica_field'):
            return cls.Meta.replica_field
        return 'pk'

    @classmethod
    def get_instance_id_value(cls, instance):
        return getattr(instance, cls.get_replica_field())

    @classmethod
    def get_document_id(cls, instance):
        if cls.is_single_document():
            return cls.Meta.document_id
        else:
            return "{}:{}".format(cls.Meta.document_id, cls.get_instance_id_value(instance))

    @classmethod
    def wrap_content_with_metadata(cls, document_id, doc, revision, revisions):
        if isinstance(doc, list):
            doc = {
                "value": doc
            }

        doc["_id"] = document_id
        doc["_rev"] = f"1-{revision}"
        if revisions:
            doc["_revisions"] = {
                "ids": revisions,
                "start": len(revisions)
            }

        return doc

    @classmethod
    def get_document_instance(cls, doc_id, request):
        if cls.is_single_document():
            return cls.get_queryset(request)
        else:
            entity_id = ":".join(doc_id.split(':')[1:])
            return cls.get_queryset(request).get(**{cls.get_replica_field(): entity_id})

    @classmethod
    def get_document_content(cls, doc_id, revision, revisions, request):
        instance = cls.get_document_instance(doc_id, request)
        if cls.is_single_document():
            serializer = cls(instance, many=True)
            return cls.wrap_content_with_metadata(doc_id, serializer.data, revision, revisions)
        else:
            serializer = cls(instance)
            return cls.wrap_content_with_metadata(doc_id, serializer.data, revision, revisions)

    @classmethod
    def get_document_content_as_json(cls, doc_id, revision, revisions, request):
        document_content = cls.get_document_content(doc_id, revision, revisions, request)
        return document_renderer.render(document_content).decode('utf-8')

    @classmethod
    def on_change(cls, instance, **kwargs):
        doc_id = cls.get_document_id(instance)
        rev_id = getattr(instance, '__ds_revision', token_hex(8))
        Change.objects.create(
            document_id=doc_id,
            revision=rev_id,
        )

    @classmethod
    def on_delete(cls, instance, **kwargs):
        doc_id = cls.get_document_id(instance)
        rev_id = instance.__ds_revision if hasattr(instance, '__ds_revision') else token_hex(8)
        Change.objects.create(
            document_id=doc_id,
            revision=rev_id,
            deleted=True
        )

    @classmethod
    def get_queryset(cls, request=None):
        Model = cls.Meta.model
        return Model.objects.all()
