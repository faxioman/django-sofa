from django.contrib.auth.models import User, Group
from sofa.base import DocumentBase


class UserDocument(DocumentBase):

    class Meta:
        model = User
        single_document = False
        document_id = 'user'


class GroupsDocument(DocumentBase):

    class Meta:
        model = Group
        single_document = True
        document_id = 'groups'
