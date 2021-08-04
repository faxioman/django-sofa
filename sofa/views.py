import json

from django.db.models import Max, CharField, Subquery
from django.db.models.functions import Cast
from django.http import JsonResponse, HttpResponseForbidden, HttpResponse, HttpResponseNotFound, StreamingHttpResponse
from django.views.decorators.cache import cache_control
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
import secrets

from .loader import get_class_by_document_id
from .models import Change, ReplicationLog
from rest_framework.renderers import JSONRenderer


document_renderer = JSONRenderer()


@require_http_methods(["GET"])
@cache_control(must_revalidate=True)
def index(request):
    return JsonResponse({
        'couchdb': 'Welcome',
        'vendor/name': 'Django Sofa Sync Gateway',
        'vendor/version': '1.0',  # TODO: version from package
        'version': '1.0',
    })


@require_http_methods(['HEAD', 'PUT', 'GET'])
@csrf_exempt
@cache_control(must_revalidate=True)
def database(request):
    if request.method == 'HEAD':
        return HttpResponse(content_type='application/json')  # TODO: remove content length
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
            "instance_start_time": "0",
            "update_seq": last_id
        })


@require_http_methods(['GET', 'PUT'])
@csrf_exempt
@cache_control(must_revalidate=True)
def replication_log(request, replication_id):
    # TODO: generate ETag
    if request.method == 'PUT':
        ReplicationLog.objects.create(document_id=replication_id)
        return HttpResponse()

    try:
        rep_log = ReplicationLog.objects.get(document_id=replication_id)
        return JsonResponse({
            "_id": f"__replog.{rep_log.document_id}",
            "_rev": "1-1",
            "history": [],
            "replication_id_version": 3,
            # "session_id": "d5a34cbbdafa70e0db5cb57d02a6b955",
            "source_last_seq": Change.objects.latest('id').id
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
        revisions = Change.objects.filter(pk__gt=since, document_id=change['document_id']).annotate(rev=Cast('revision', CharField())).values('rev').order_by('pk')
        row = {
            "seq": change_id, "id": change['document_id'], "changes": list(revisions)
        }
        if change["deleted"]:
            row["deleted"] = True

        results.append(row)
        last_change = change_id
    if feed == 'normal':
        return JsonResponse({
            "results": results,
            "last_seq": last_change
        })
    else:
        pass  # stupid format


@require_http_methods(['POST'])
@csrf_exempt
@cache_control(must_revalidate=True)
def all_docs(request):
    body = json.loads(request.body.decode('utf-8'))
    keys = body.get('keys', [])
    include_docs = request.GET.get('include_docs') == 'true'
    changes = Change.objects.filter(document_id__in=keys)
    return JsonResponse({
        "offset": 0,
        "rows": [{
            "id": d.document_id,
            "key": d.document_id,
            "value": {
                "rev": d.revision
            },
            "doc": d.get_document() if include_docs else None,
        } for d in changes],
        "total_rows": len(changes)
    })


def iter_documents(requested_docs, initial_boundary, return_revisions):
    ids = {d['id'] for d in requested_docs['docs']}

    latest_changes = Change.objects.get_latest_changes(ids)

    docs_map = {}

    for change in latest_changes:
        docs_map[change.document_id] = {
            "rev": str(change.revision),
            "deleted": change.deleted,
            "revisions": [d['rev'] for d in requested_docs['docs'] if d['id'] == change.document_id]
        }

    for doc in requested_docs['docs']:
        error = False
        try:
            if doc['rev'] == docs_map[doc['id']]['rev'] and not docs_map[doc['id']]["deleted"]:
                document_class = get_class_by_document_id(doc['id'])
                content = document_renderer.render(document_class.get_document_content(doc['id'], doc['rev'], docs_map[doc['id']]["revisions"] if return_revisions else [])).decode('utf-8')
            else:
                error = True
                content = '{"missing": "%s"}' % doc['rev']
        except KeyError:
            error = True
            content = '{"missing": "%s"}' % doc['rev']

        if error:
            error_content = '; error="true"'
        else:
            error_content = ''

        yield f'--{initial_boundary}\r\nContent-Type: application/json{error_content}\r\n\r\n{content}\r\n'

    yield f'--{initial_boundary}--\r\n'


@require_http_methods(['POST'])
@csrf_exempt
@cache_control(must_revalidate=True)
def bulk_get(request):
    only_latest = request.GET.get('latest') == 'true'
    return_revisions = request.GET.get('revs') == 'true'
    body = json.loads(request.body.decode('utf-8'))

    initial_boundary = secrets.token_hex(16)

    return StreamingHttpResponse(
        streaming_content=(iter_documents(body, initial_boundary, return_revisions)),
        content_type='multipart/mixed; boundary="{}"'.format(initial_boundary),
    )


@require_http_methods(['POST'])
@csrf_exempt
@cache_control(must_revalidate=True)
def revs_diff(request):
    return JsonResponse({})


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
        last_change = latest_changes[0]

        if last_change.deleted:
            return JsonResponse([{
                "missing": last_change.revision
            }], safe=False)
        return JsonResponse([latest_changes[0].get_document()], safe=False)

    raise NotImplementedError


# https://docs.couchdb.org/en/stable/replication/protocol.html#retrieve-replication-logs-from-source-and-target