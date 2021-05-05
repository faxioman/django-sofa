import json

from django.db.models import Max, CharField, Subquery
from django.db.models.functions import Cast
from django.http import JsonResponse, HttpResponseForbidden, HttpResponse, HttpResponseNotFound, StreamingHttpResponse
from django.views.decorators.cache import cache_control
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
import secrets

from .loader import get_class_by_document_id
from .models import Change


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
        # TODO
        return HttpResponse()
    # TODO: GET FROM DB OR 404
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
        pass # stupid format


def iter_documents(requested_docs, initial_boundary):
    ids = {d['id'] for d in requested_docs['docs']}

    # only the latest revision is available, so load only the available revisions
    # a future django-reversion integration could be planned
    latest_changes = Change.objects.filter(pk__in=Subquery(Change.objects.filter(document_id__in=ids).values('document_id').annotate(last_id=Max('pk')).values('last_id')))

    docs_map = {}

    for change in latest_changes:
        docs_map[change.document_id] = {
            "rev": str(change.revision),
            "deleted": change.deleted
        }

    for doc in requested_docs['docs']:
        error = False
        try:
            if doc['rev'] == docs_map[doc['id']]['rev'] and not docs_map[doc['id']]["deleted"]:
                document_class = get_class_by_document_id(doc['id'])
                content = document_class.get_document_content(doc['id'], doc['rev'], [])
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


@require_http_methods(['POST'])
@csrf_exempt
@cache_control(must_revalidate=True)
def bulk_get(request):
    only_latest = request.GET.get('latest') == 'true'
    return_revisions = request.GET.get('revs') == 'true'
    body = json.loads(request.body.decode('utf-8'))

    initial_boundary = secrets.token_hex(16)

    return StreamingHttpResponse(
        streaming_content=(iter_documents(body, initial_boundary)),
        content_type='multipart/mixed; boundary="{}"'.format(initial_boundary),
    )


# https://docs.couchdb.org/en/stable/replication/protocol.html#retrieve-replication-logs-from-source-and-target