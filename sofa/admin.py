from django.contrib import admin
from .models import Change


@admin.register(Change)
class ChangeAdmin(admin.ModelAdmin):
    pass
