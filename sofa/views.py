import json

from django.db import transaction
from django.db.models import Max, CharField, Q, Value, Subquery
from django.db.models.functions import Cast, Concat
from django.http import JsonResponse, HttpResponseForbidden, HttpResponse, HttpResponseNotFound, StreamingHttpResponse, HttpResponseBadRequest
from django.views.decorators.cache import cache_control
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.utils import timezone
import hashlib
from .loader import get_class_by_document_id
from .models import Change, ReplicationLog, ReplicationHistory
from django.conf import settings


start_time = int(timezone.now().timestamp() * 1000 * 1000)
server_uuid = hashlib.sha1(settings.SECRET_KEY.encode()).hexdigest()[:32]


@require_http_methods(["GET"])
@cache_control(must_revalidate=True)
def index(request):
    return JsonResponse({
        'couchdb': 'Welcome',
        'vendor': {
            'name': 'Django Sofa Sync Gateway',
            'version': '1.0',  # TODO: version from package
        },
        'version': 'Django Sofa Sync Gateway/1.0',
    })


@require_http_methods(['HEAD', 'PUT', 'GET'])
@csrf_exempt
@cache_control(must_revalidate=True)
def database(request):
    if request.method == 'HEAD':
        return HttpResponse(content_type='application/json')
    if request.method == 'PUT':
        return HttpResponseForbidden(json.dumps({
            "error": "unauthorized",
            "reason": "unauthorized to create database {}".format(request.build_absolute_uri())
        }), content_type='application/json')
    if request.method == 'GET':
        last_id = 0
        try:
            last_id = Change.objects.latest('id').id
        except Change.DoesNotExist:
            pass

        return JsonResponse({
            "instance_start_time": start_time,
            "update_seq": last_id,
            "committed_update_seq": last_id,
            "compact_running": False,
            "purge_seq": 0,
            "disk_format_version": 0,
            "state": "Online",
            "server_uuid": server_uuid
        })


@require_http_methods(['GET', 'PUT'])
@csrf_exempt
@cache_control(must_revalidate=True)
def replication_log(request, replication_id):
    # TODO: generate ETag
    if request.method == 'PUT':
        body = json.loads(request.body.decode('utf-8'))
        with transaction.atomic():
            rep_log, _ = ReplicationLog.objects.get_or_create(
                document_id=replication_id,
                defaults={
                    'version': body['version'],
                    'replicator': body['replicator']
                }
            )
            last_history = ReplicationHistory.objects.create(
                    replication_log=rep_log,
                    session_id=body['session_id'],
                    last_seq=body['last_seq']
            )

        return JsonResponse({
            "_id": f"_local/{rep_log.document_id}",
            "_rev": f"1-{last_history.pk}",
            "ok": True
        }, status=201)

    try:
        rep_log = ReplicationLog.objects.prefetch_related('history').get(document_id=replication_id)
        last_history = rep_log.history.latest('id')
        return JsonResponse({
            "_id": f"_local/{rep_log.document_id}",
            "_rev": f"1-{last_history.pk}",
            "history": [{"last_seq": h.last_seq, "session_id": h.session_id} for h in rep_log.history.all()],
            "session_id": last_history.session_id,
            "last_seq": last_history.last_seq,
            "replicator": rep_log.replicator,
            "version": rep_log.version
        })
    except ReplicationLog.DoesNotExist:
        return HttpResponseNotFound(json.dumps({"error": "not_found", "reason": "missing"}), content_type='application/json')


@require_http_methods(['GET'])
@cache_control(must_revalidate=True)
def changes(request):
    style = request.GET.get('style')  # all_docs
    since = int(request.GET.get('since', '0'))
    limit = int(request.GET.get('limit', '1000'))  # TODO: make default max limit configurable
    feed = request.GET.get('feed', 'normal')  # (continuous, normal, longpoll)
    # TODO: filter

    results = []

    # TODO: stream
    last_change = 0
    for change in Change.objects.filter(pk__gt=since).values('document_id').annotate(id=Max('pk'), deleted=Max('deleted')).order_by('id')[:limit]:
        # TODO: instead of querying, use ArrayAgg for revisions if db is postgres
        change_id = change['id']
        revisions = Change.objects.filter(pk__gt=since, document_id=change['document_id']).annotate(rev=Concat(Value('1-'), Cast('revision', CharField()))).values('rev').order_by('pk')
        row = {
            "seq": change_id, "id": change['document_id'], "changes": revisions[::-1]
        }
        if change["deleted"] == 1:
            row["deleted"] = True

        results.append(row)
        last_change = change_id
    if feed == 'normal':
        return JsonResponse({
            "results": results,
            "last_seq": str(last_change if last_change > 0 else Change.objects.latest('id').id)
        })
    else:
        return HttpResponseBadRequest('{"error": "sync style not implemented"}', content_type='application/json')


@require_http_methods(['POST'])
@csrf_exempt
@cache_control(must_revalidate=True)
def all_docs(request):
    body = json.loads(request.body.decode('utf-8'))
    keys = body.get('keys', [])
    include_docs = request.GET.get('include_docs') == 'true'

    docs_changes = Change.objects.filter(pk__in=Subquery(Change.objects.filter(document_id__in=keys).values('document_id').annotate(id=Max('pk')).values('id'))).all()

    # never returning the doc and forcing the rev to empty string is the only way I found to force bulk_get.
    # I also found sync gateway returning the rev as empty string
    # we need to change 1- ... is checked in pouchdb :(

    rows = [
        {
            "id": d.document_id,
            "key": d.document_id,
            "value": {
                "rev": "" # f"1-{d.revision}"
            },
            # "doc": Change(document_id=d.document_id, revision=d.revision).get_document(request) if include_docs else None
        } for d in docs_changes
    ]

    return JsonResponse({
        "rows": rows,
        "total_rows": len(rows),
        "update_seq": Change.objects.latest('id').id
    })


def iter_documents(request, requested_docs, return_revisions):

    ids = {d['id'] for d in requested_docs['docs']}

    latest_changes = Change.objects.get_latest_changes(ids)

    docs_map = {}

    for change in latest_changes:
        docs_map[change.document_id] = {
            "rev": str(change.revision),
            "deleted": change.deleted == 1,
            "revisions": Change.objects.get_revisions_for_document(change.document_id)
        }

    yield '{"results": ['

    first = True

    for key, value in docs_map.items():

        if not first:
            yield ","

        first = False

        yield f'{{"id": "{key}", "docs": ['

        document_class = get_class_by_document_id(key)

        if not value['deleted']:
            content = document_class.get_document_content_as_json(key, value['rev'], value["revisions"] if return_revisions else [], request)
            yield f'{{"ok": {content}}}'

        else:
            content = document_class.get_document_content_as_json(key, value['rev'], value["revisions"] if return_revisions else [], request, force_delete=True)
            yield f'{{"ok": {content}}}'

        yield ']}'

    yield ']}'


@require_http_methods(['POST'])
@csrf_exempt
@cache_control(must_revalidate=True)
def bulk_get(request):
    only_latest = request.GET.get('latest') == 'true'

    if not only_latest:
        return HttpResponseBadRequest('Only latest revision are allowed')

    if not request.accepts('application/json'):
        return HttpResponseBadRequest('Only application/json type is supported as response content')

    return_revisions = request.GET.get('revs') == 'true'
    body = json.loads(request.body.decode('utf-8'))

    return StreamingHttpResponse(
        streaming_content=(iter_documents(request, body, return_revisions)),
        content_type='application/json',
    )


@require_http_methods(['POST'])
@csrf_exempt
@cache_control(must_revalidate=True)
def revs_diff(request):
    # TODO: and existing deleted document?
    changed_docs = json.loads(request.body.decode('utf-8'))

    docs_filter = Q()

    for doc_id, revisions in changed_docs.items():
        for revision in revisions:
            docs_filter |= Q(document_id=doc_id, revision=revision.split('-')[1])

    existing_docs = Change.objects.filter(docs_filter)

    # clean existing doc from changed_docs
    for existing_doc in existing_docs:
        changed_docs[existing_doc.document_id] = [i for i in changed_docs[existing_doc.document_id] if not i.endswith(f"-{existing_doc.revision}")]

    # clean doc without revisions
    changed_docs = {k: v for (k, v) in changed_docs.items() if v}

    return JsonResponse({k: {"missing": v} for (k, v) in changed_docs.items()})


def update_doc(request):
    affected = []
    # TODO: the request body should be read as stream
    body = json.loads(request.body.decode('utf-8'))

    if body.get('new_edits', True):
        return HttpResponseBadRequest('Docs without revision are not supported')

    for doc in body['docs']:
        doc_id = doc.pop('_id')
        rev_id = doc.pop('_rev')

        doc_class = get_class_by_document_id(doc_id)
        if not doc_class:
            # no related doc in django
            continue

        res = doc_class.apply_changes(doc_id, rev_id, doc, request)
        if res:
            affected.append(res)

    return affected


@require_http_methods(['GET', 'POST'])
@csrf_exempt
@cache_control(must_revalidate=True)
def document(request, document_id):

    if request.method == 'GET':

        only_latest = request.GET.get('latest') == 'true'
        return_revisions = request.GET.get('revs') == 'true'
        open_revs = json.loads(request.GET.get('open_revs', '[]'))

        if not only_latest:
            # open_revs could not be ignored
            raise NotImplementedError

        latest_changes = Change.objects.get_latest_changes(ids=[document_id])
        return JsonResponse([latest_changes[0].get_document(request)], safe=False)

    if request.method == 'POST':
        affected = update_doc(request)
        return JsonResponse(affected, safe=False)


@require_http_methods(['POST'])
@csrf_exempt
@cache_control(must_revalidate=True)
def bulk_docs(request):
    affected = update_doc(request)
    return JsonResponse(affected, safe=False)
