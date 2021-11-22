from django.core.management.base import BaseCommand
from django.utils.translation import ugettext_lazy as _
from sofa.loader import init_revisions


class Command(BaseCommand):
    help = _('Add initial revision for all sofa documents')

    def handle(self, *args, **options):
        init_revisions()
