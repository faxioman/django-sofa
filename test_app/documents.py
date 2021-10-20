from django.contrib.auth.models import User, Group
from sofa.base import DocumentBase


class UserDocument(DocumentBase):

    def get_queryset(cls, request=None):
        return User.objects.filter(pk=6)

    class Meta:
        model = User
        single_document = False
        document_id = 'user'
        exclude = ('password',)


class GroupsDocument(DocumentBase):

    class Meta:
        model = Group
        single_document = True
        document_id = 'groups'
        exclude = ('permissions',)
