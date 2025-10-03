from django.urls import path
from . import views

urlpatterns = [
    path('', views.dashboard_view, name='dashboard'),
    path('run/<str:script_name>/', views.run_script_view, name='run_script'),
    path('api/status/', views.dashboard_status_api, name='dashboard_status_api'),
]