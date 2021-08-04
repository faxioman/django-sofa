# Generated by Django 3.2 on 2021-08-04 09:43

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('sofa', '0002_auto_20210502_2022'),
    ]

    operations = [
        migrations.CreateModel(
            name='ReplicationLog',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('document_id', models.CharField(db_index=True, max_length=128)),
                ('revision', models.CharField(max_length=64)),
            ],
        ),
    ]