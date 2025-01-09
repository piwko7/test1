from . import views
from django.urls import path

urlpatterns = [
       path("", views.home, name="home"),
       path('measurment', views.add_test_entry, name='add_measurment'),
       path('string', views.add_string_entry, name='add_str'),
       path('sendplc/<int:data>', views.send_data_to_plc, name='send_to_plc'),
]