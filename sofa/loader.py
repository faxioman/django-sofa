from importlib import import_module

from django.db import transaction
from django.db.models.signals import post_save, post_delete


_DOCUMENT_ID_TO_CLASS = {}


def get_apps_packages():
    from django.apps import apps
    return [config.name for config in apps.get_app_configs()]


def load_document_classes(packages):
    from django.conf import settings
    for package in packages:
        try:
            # TODO: add default module name
            import_module('{}.{}'.format(package, settings.SOFA_MODULE_NAME))
        except ImportError:
            pass


def register_to_model_signals(cls):
    Model = cls.Meta.model

    post_save.connect(cls.on_change, sender=Model, dispatch_uid="change_{}".format(Model._meta.label_lower))
    post_delete.connect(cls.on_delete, sender=Model, dispatch_uid="delete_{}".format(Model._meta.label_lower))


def get_class_by_document_id(document_id):
    return _DOCUMENT_ID_TO_CLASS.get(document_id.split(':')[0])


def patch_model(model_class):

    def get_rev(self):
        return self.__private_ds_revision

    def set_rev(self, val):
        self.__private_ds_revision = val

    model_class.__ds_revision = property(fget=get_rev, fset=set_rev)


def load():
    load_document_classes(get_apps_packages())
    _DOCUMENT_ID_TO_CLASS.clear()
    from .base import DocumentBase
    for cls in DocumentBase.__subclasses__():
        document_id = cls.Meta.document_id
        if document_id in _DOCUMENT_ID_TO_CLASS:
            raise Exception("Duplicated document_id found in class: {} and {}".format(cls, _DOCUMENT_ID_TO_CLASS[document_id]))
        _DOCUMENT_ID_TO_CLASS[document_id] = cls
        register_to_model_signals(cls)
        patch_model(cls.Meta.model)


def init_revisions():
    from .models import Change
    with transaction.atomic():
        Change.objects.all().delete()
        for document_class in _DOCUMENT_ID_TO_CLASS.values():
            document_class.add_revision()

