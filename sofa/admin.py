from django.contrib import admin
from .models import Change


@admin.register(Change)
class ChangeAdmin(admin.ModelAdmin):
    list_display = ('id', 'document_id', 'revision')
