from secrets import token_hex

from django.core.exceptions import ObjectDoesNotExist
from rest_framework.serializers import ModelSerializer
from rest_framework.renderers import JSONRenderer
from .models import Change
import logging


document_renderer = JSONRenderer()
logger = logging.getLogger("django-sofa")


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
        rev_id = getattr(instance, '__ds_revision', token_hex(16))
        Change.objects.create(
            document_id=doc_id,
            revision=rev_id,
        )

    @classmethod
    def on_delete(cls, instance, **kwargs):
        doc_id = cls.get_document_id(instance)
        rev_id = getattr(instance, '__ds_revision', token_hex(16))
        Change.objects.create(
            document_id=doc_id,
            revision=rev_id,
            deleted=True
        )

    @classmethod
    def init_revision(cls):
        if cls.is_single_document():
            Change.objects.create(
                document_id=cls.Meta.document_id,
                revision=token_hex(16)
            )
        else:
            Model = cls.Meta.model
            models = Model.objects.all()
            for model in models:
                Change.objects.create(
                    document_id=cls.get_document_id(model),
                    revision=token_hex(16)
                )

    @classmethod
    def get_queryset(cls, request=None):
        Model = cls.Meta.model
        return Model.objects.all()

    @classmethod
    def apply_changes(cls, doc_id, rev_id, content, request):
        delete_doc = content.get('_deleted', False)

        if cls.is_single_document():
            # single document updated is not yet implemented
            return

        try:
            current_instance = cls.get_document_instance(doc_id, request)
            if delete_doc:
                current_instance.delete()
        except ObjectDoesNotExist:
            if not delete_doc:
                id_field = cls.get_replica_field()
                current_instance = cls.Meta.model(**{id_field: doc_id.split(':')[1]})

        if delete_doc:
            return {
                "id": doc_id,
                "rev": rev_id
            }

        doc_serializer = cls(current_instance, data=content, partial=True)

        try:
            doc_serializer.is_valid(raise_exception=True)
        except Exception as ex:
            logging.error(f'Error updating doc {doc_id}.', exc_info=ex)
        else:
            doc_serializer.save(__ds_revision=rev_id.split('-')[1])
            return {
                "id": doc_id,
                "rev": rev_id
            }
