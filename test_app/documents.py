from django.contrib.auth.models import User, Group
from sofa.base import DocumentBase


class UserDocument(DocumentBase):

    def update(self, instance, validated_data):
        u = super().update(instance, validated_data)
        return u

    def create(self, validated_data):
        request = self.context['request']
        u = super().create(validated_data)
        return u

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
