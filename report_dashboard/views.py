from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.utils import timezone
from .models import AutomationJob
import subprocess
import sys
import os

# Import your data models (with correct Posoco models)
from nrldc_app.models import Nrldc2AData, Nrldc2CData
from srldc_app.models import Srldc2AData, Srldc2CData 
from wrldc_app.models import Wrldc2AData, Wrldc2CData
from posoco.models import PosocoTableA, PosocoTableG # <-- Your correct models


def dashboard_view(request):
    """
    Displays the main dashboard page. (No changes here)
    """
    jobs = AutomationJob.objects.all()
    
    script_names = ['nrldc_report', 'srldc_report', 'wrldc_report', 'posoco_report', 'merge_reports']
    if jobs.count() < len(script_names):
        existing_scripts = list(jobs.values_list('script_name', flat=True))
        for name in script_names:
            if name not in existing_scripts:
                AutomationJob.objects.create(script_name=name)
        jobs = AutomationJob.objects.all()

    context = {
        'jobs': jobs
    }
    return render(request, 'report_dashboard/dashboard.html', context)

def run_script_view(request, script_name):
    """
    Triggers a management command. (No changes here)
    """
    if request.method == 'POST':
        job = AutomationJob.objects.get(script_name=script_name)
        job.status = AutomationJob.Status.RUNNING
        job.last_run_time = timezone.now()
        job.log_message = "Execution started manually..."
        job.save()

        python_executable = sys.executable
        manage_py_path = 'manage.py'
        run_date = request.POST.get('run_date')
        
        command = [python_executable, manage_py_path, script_name]
        if run_date:
            command.extend(['--date', run_date])
            
        subprocess.Popen(command)
        return redirect('dashboard')
    
    return redirect('dashboard')


def dashboard_status_api(request):
    """
    API endpoint to provide job statuses.
    --- THIS FUNCTION IS UPDATED ---
    """
    today = timezone.now().date()
    jobs = AutomationJob.objects.all()

    for job in jobs:
        data_exists = False
        if job.script_name == 'nrldc_report':
            data_exists = (Nrldc2AData.objects.filter(report_date=today).exists() or 
                           Nrldc2CData.objects.filter(report_date=today).exists())
        
        elif job.script_name == 'srldc_report':
            data_exists = (Srldc2AData.objects.filter(report_date=today).exists() or 
                           Srldc2CData.objects.filter(report_date=today).exists())

        elif job.script_name == 'wrldc_report':
            data_exists = (Wrldc2AData.objects.filter(report_date=today).exists() or 
                           Wrldc2CData.objects.filter(report_date=today).exists())

        elif job.script_name == 'posoco_report':
            # This now uses your correct Posoco models
            data_exists = (PosocoTableA.objects.filter(report_date=today).exists() or 
                           PosocoTableG.objects.filter(report_date=today).exists())

        # --- UPDATED: Directory search logic for merge_reports ---
        elif job.script_name == 'merge_reports':
            directory_path = "downloads/overall_json/"
            file_prefix = f"merged_reports_{today.strftime('%Y-%m-%d')}"
            
            # This searches the directory for a file starting with today's date
            if os.path.exists(directory_path):
                for filename in os.listdir(directory_path):
                    if filename.startswith(file_prefix):
                        data_exists = True
                        break # Found a match, no need to search further
            # --- End of updated logic ---

        # Save the updated status if it has changed
        if job.is_data_available_today != data_exists:
            job.is_data_available_today = data_exists
            job.save()
    
    # The rest of the function remains the same
    job_values = jobs.values(
        'script_name', 'status', 'last_run_time', 'last_success_time', 
        'is_data_available_today', 'log_message'
    )
    
    job_list = list(job_values)
    for job_data in job_list:
        if job_data['last_run_time']:
            job_data['last_run_time'] = job_data['last_run_time'].strftime('%b %d, %Y, %I:%M %p')
        if job_data['last_success_time']:
            job_data['last_success_time'] = job_data['last_success_time'].strftime('%b %d, %Y, %I:%M %p')

    return JsonResponse(job_list, safe=False)