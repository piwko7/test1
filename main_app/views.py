import json

import snap7
from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt

from .models import Test


def home(request):
    return HttpResponse("Hello world!")


def count_entries(request):
    count = Test.objects.count()
    return HttpResponse(f"Db count: {count}")


@csrf_exempt
def add_test_entry(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            name = data.get('name')
            value = data.get('value')
        except json.JSONDecodeError:
            return JsonResponse({'success': False, 'error': 'Invalid JSON'}, status=400)

        if not name or value is None:
            return JsonResponse({'success': False, 'error': 'Both name and value are required.'}, status=400)

        try:
            value = int(value)
        except ValueError:
            return JsonResponse({'success': False, 'error': 'Value must be an integer.'}, status=400)

        new_entry = Test.objects.create(name=name, value=value)
        return JsonResponse({'success': True})

    return JsonResponse({'success': False, 'error': 'Invalid request method.'}, status=405)


@csrf_exempt
def add_string_entry(request):
    if request.method == 'POST':
        data = request.body.decode("utf-8")
        params = data.split(",")
        name = params[0].split(':')[1].strip()
        value = int(params[1].split(':')[1].strip())

        if not name or value is None:
            return JsonResponse({'success': False, 'error': 'Both name and value are required.'}, status=400)

        try:
            value = int(value)
        except ValueError:
            return JsonResponse({'success': False, 'error': 'Value must be an integer.'}, status=400)

        new_entry = Test.objects.create(name=name, value=value)
        return JsonResponse({'success': True})

    return JsonResponse({'success': False, 'error': 'Invalid request method.'}, status=405)


@csrf_exempt
def add_string_entry(request):
    if request.method == 'POST':
        data = request.body.decode("utf-8")
        params = data.split(",")
        name = params[0].split(':')[1].strip()
        value = int(params[1].split(':')[1].strip())

        if not name or value is None:
            return JsonResponse({'success': False, 'error': 'Both name and value are required.'}, status=400)

        try:
            value = int(value)
        except ValueError:
            return JsonResponse({'success': False, 'error': 'Value must be an integer.'}, status=400)

        new_entry = Test.objects.create(name=name, value=value)
        return JsonResponse({'success': True})

    return JsonResponse({'success': False, 'error': 'Invalid request method.'}, status=405)


PLC_IP = "192.168.0.1"

def connect_plc(ip_address, rack=0, slot=1):
    client = snap7.client.Client()
    client.connect(ip_address, rack, slot)
    return client

def write_data_to_plc(client, db_number, start, data):
    bytearray_data = bytearray(data)
    client.db_write(db_number, start, bytearray_data)


@csrf_exempt
def send_data_to_plc(request, data):
    if request.method == 'GET':
        if data == 1:
            data_values = [1]  # True
        elif data == 0:
            data_values = [0]  # False
        else:
            return JsonResponse({'status': 'error', 'message': 'Invalid data value'}, status=400)
        try:
            client = connect_plc(PLC_IP)
            write_data_to_plc(client, 1, 0, data_values)
            client.disconnect()

            return JsonResponse({'status': 'success', 'message': 'Data written to PLC'})

        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=400)

    return JsonResponse({'status': 'error', 'message': 'Invalid method'}, status=405)