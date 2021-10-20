# Generated by Django 3.2 on 2021-10-20 15:32

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('sofa', '0004_auto_20210901_1549'),
    ]

    operations = [
        migrations.AlterField(
            model_name='change',
            name='deleted',
            field=models.PositiveIntegerField(default=0),
        ),
        migrations.AlterField(
            model_name='replicationlog',
            name='document_id',
            field=models.CharField(max_length=128, unique=True),
        ),
    ]
