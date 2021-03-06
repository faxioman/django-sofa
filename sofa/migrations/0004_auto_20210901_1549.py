# Generated by Django 3.2 on 2021-09-01 15:49

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('sofa', '0003_replicationlog'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='replicationlog',
            name='revision',
        ),
        migrations.AddField(
            model_name='replicationlog',
            name='replicator',
            field=models.CharField(default='unknown', max_length=16),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='replicationlog',
            name='version',
            field=models.PositiveIntegerField(default=1),
            preserve_default=False,
        ),
        migrations.CreateModel(
            name='ReplicationHistory',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('session_id', models.CharField(max_length=16)),
                ('last_seq', models.PositiveIntegerField()),
                ('replication_log', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='history', to='sofa.replicationlog')),
            ],
        ),
    ]
