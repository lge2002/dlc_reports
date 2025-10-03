# report_dashboard/admin.py

from django.contrib import admin
from .models import AutomationJob

@admin.register(AutomationJob)
class AutomationJobAdmin(admin.ModelAdmin):
    list_display = (
        'script_name', 
        'status', 
        'is_data_available_today', 
        'last_run_time', 
        'last_success_time'
    )
    list_filter = ('status', 'is_data_available_today')
    search_fields = ('script_name',)
    ordering = ('script_name',)