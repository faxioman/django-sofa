from django.db import models
from django.db.models import Subquery, Max


class ChangeManager(models.Manager):
    def get_latest_changes(self, ids):
        # only the latest revision is available, so load only the available revisions
        # a future django-reversion integration could be planned
        return self.filter(pk__in=Subquery(self.model.objects.filter(document_id__in=ids).values('document_id').annotate(last_id=Max('pk')).values('last_id')))

    def get_revisions_for_document(self, id):
        return self.filter(document_id=id).values_list('revision', flat=True).order_by('-id')
