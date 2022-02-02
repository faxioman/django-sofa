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
    def get_document_content(cls, doc_id, revision, revisions, request, force_delete=False):

        if force_delete:
            return cls.wrap_content_with_metadata(doc_id, {"_deleted": True}, revision, revisions)

        try:
            instance = cls.get_document_instance(doc_id, request)
        except ObjectDoesNotExist:
            return cls.wrap_content_with_metadata(doc_id, {"_deleted": True}, revision, revisions)
        doc_serializer = cls(instance, many=cls.is_single_document(), context={'request': request})
        return cls.wrap_content_with_metadata(doc_id, doc_serializer.data, revision, revisions)

    @classmethod
    def get_document_content_as_json(cls, doc_id, revision, revisions, request, force_delete=False):
        document_content = cls.get_document_content(doc_id, revision, revisions, request, force_delete)
        return document_renderer.render(document_content).decode('utf-8')

    @classmethod
    def on_change(cls, instance, **kwargs):
        token = token_hex(16)
        doc_id = cls.get_document_id(instance)
        rev_id = getattr(instance, '__ds_revision', token)
        Change.objects.create(
            document_id=doc_id,
            revision=rev_id or token,
        )

    @classmethod
    def on_delete(cls, instance, **kwargs):
        token = token_hex(16)
        doc_id = cls.get_document_id(instance)
        rev_id = getattr(instance, '__ds_revision', token)
        if cls.is_single_document():
            Change.objects.create(
                document_id=doc_id,
                revision=rev_id or token,
            )
        else:
            Change.objects.create(
                document_id=doc_id,
                revision=rev_id or token,
                deleted=True
            )

    @classmethod
    def add_revision(cls, instance=None):
        if cls.is_single_document():
            Change.objects.create(
                document_id=cls.Meta.document_id,
                revision=token_hex(16)
            )
        else:
            if instance:
                Change.objects.create(
                    document_id=cls.get_document_id(instance),
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
    def apply_delete(cls, doc_id, rev_id, request):
        try:
            current_instance = cls.get_document_instance(doc_id, request)
            if not cls.can_delete(current_instance, request):
                return
            current_instance.delete()
        except ObjectDoesNotExist:
            pass

        return {
            "id": doc_id,
            "rev": rev_id
        }

    @classmethod
    def apply_update(cls, instance, doc_id, rev_id, content, request):

        if not cls.can_change(instance, request):
            return

        doc_serializer = cls(instance, data=content, partial=True, context={'request': request})

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

    @classmethod
    def apply_create(cls, doc_id, rev_id, content, request):

        if not cls.can_add(request):
            return

        doc_serializer = cls(data=content, partial=True, context={'request': request})

        try:
            doc_serializer.is_valid(raise_exception=True)
        except Exception as ex:
            logging.error(f'Error creating doc {doc_id}.', exc_info=ex)
        else:
            id_field = cls.get_replica_field()
            additional_fields = {
                id_field: doc_id.split(':')[1],
                "__ds_revision": rev_id.split('-')[1]
            }
            doc_serializer.save(**additional_fields)
            return {
                "id": doc_id,
                "rev": rev_id
            }

    @classmethod
    def apply_changes(cls, doc_id, rev_id, content, request):

        if cls.is_single_document():
            # single document are always readonly
            return

        if content.get('_deleted', False):
            return cls.apply_delete(doc_id, rev_id, request)

        try:
            current_instance = cls.get_document_instance(doc_id, request)
            return cls.apply_update(current_instance, doc_id, rev_id, content, request)
        except ObjectDoesNotExist:
            return cls.apply_create(doc_id, rev_id, content, request)

    @classmethod
    def can_change(cls, obj, request):
        return True

    @classmethod
    def can_delete(cls, obj, request):
        return True

    @classmethod
    def can_add(cls, request):
        return True
