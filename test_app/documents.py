from django.contrib.auth.models import User, Group
from sofa.base import DocumentBase


class UserDocument(DocumentBase):

    class Meta:
        model = User
        replica_field = 'username'
        document_id = 'user'
        exclude = ('password',)


class GroupsDocument(DocumentBase):

    class Meta:
        model = Group
        single_document = True
        document_id = 'groups'
        exclude = ('permissions',)
