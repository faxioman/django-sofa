from django.views.decorators.cache import cache_control
from rest_framework.decorators import api_view, renderer_classes, parser_classes, authentication_classes
from rest_framework.parsers import JSONParser
from rest_framework.renderers import JSONRenderer
from rest_framework.response import Response
from rest_framework.exceptions import PermissionDenied, NotFound
from .models import Change


@api_view(['GET'])
@renderer_classes([JSONRenderer])
@parser_classes([JSONParser])
@authentication_classes([])
@cache_control(must_revalidate=True)
def index(request):
    return Response({'message': 'hi!'})


@api_view(['HEAD', 'PUT', 'GET'])
@renderer_classes([JSONRenderer])
@parser_classes([JSONParser])
@authentication_classes([])
@cache_control(must_revalidate=True)
def database(request):
    if request.method == 'HEAD':
        return Response('')  # TODO: remove content length
    if request.method == 'PUT':
        raise PermissionDenied({
            "error": "unauthorized",
            "reason": "unauthorized to create database {}".format(request.build_absolute_uri())
        })
    if request.method == 'GET':
        last_id = 0
        try:
            last_id = Change.objects.latest('id').id
        except Change.DoesNotExist:
            pass

        return Response({
            "instance_start_time": "0",
            "update_seq": last_id
        })


@api_view(['GET', 'PUT'])
@renderer_classes([JSONRenderer])
@parser_classes([JSONParser])
@authentication_classes([])
@cache_control(must_revalidate=True)
def replication_log(request, replication_id):
    # TODO: generate ETag
    if request.method == 'PUT':
        # TODO
        return Response('')
    # TODO: GET FROM DB OR 404
    raise NotFound({"error": "not_found", "reason": "missing"})


@api_view(['GET'])
@renderer_classes([JSONRenderer])
@parser_classes([JSONParser])
@authentication_classes([])
@cache_control(must_revalidate=True)
def changes(request):
    style = request.GET.get('style')  # all_docs
    since = int(request.GET.get('since', '0'))
    limit = int(request.GET.get('limit', '1000'))  #TODO: make default max limit configurable
    feed = request.GET.get('feed', 'normal')  # (continuous, normal)
    # TODO: filter

    results = []

    # TODO: stream
    last_change = 0
    for change in Change.objects.filter(pk__gt=since)[:limit]:
        # TODO: each change as always only one rev ... we need to fix this?
        results.append({
            "seq": change.pk, "id": change.document_id, "changes": [{"rev": str(change.revision)}], "deleted": change.deleted
        })
        last_change = change.pk
    return Response({
        "results": results,
        "last_seq": last_change
    })




# https://docs.couchdb.org/en/stable/replication/protocol.html#retrieve-replication-logs-from-source-and-target