from django.shortcuts import render
from django.db.models import Max
import os

from nrldc_app.models import Nrldc2AData, Nrldc2CData
from srldc_app.models import Srldc2AData, Srldc2CData
from wrldc_app.models import Wrldc2AData, Wrldc2CData


def read_log_file(project_name):
    """
    Safely read the content of the log file for the given project.
    Returns error messages if the file is missing or unreadable.
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_file_path = os.path.join(base_dir, 'logs', f"{project_name.lower()}.log")
    try:
        with open(log_file_path, 'r', encoding='utf-8', errors='replace') as f:
            return f.read()
    except FileNotFoundError:
        return f"Log file '{project_name.lower()}.log' not found. Run the management command for {project_name} to generate logs."
    except Exception as e:
        return f"Error reading log file for {project_name}: {e}"


def dashboard(request):
    projects = [
        {'name': 'NRLDC', 'model_2A': Nrldc2AData, 'model_2C': Nrldc2CData},
        {'name': 'SRLDC', 'model_2A': Srldc2AData, 'model_2C': Srldc2CData},
        {'name': 'WRLDC', 'model_2A': Wrldc2AData, 'model_2C': Wrldc2CData},
    ]

    status = []
    log_content = {}

    for project in projects:
        latest_2A_date = project['model_2A'].objects.aggregate(latest_date=Max('report_date'))['latest_date']
        latest_2C_date = project['model_2C'].objects.aggregate(latest_date=Max('report_date'))['latest_date']

        count_2A = project['model_2A'].objects.filter(report_date=latest_2A_date).count() if latest_2A_date else 0
        count_2C = project['model_2C'].objects.filter(report_date=latest_2C_date).count() if latest_2C_date else 0

        status.append({
            'name': project['name'],
            'latest_2A_date': latest_2A_date,
            'latest_2C_date': latest_2C_date,
            'count_2A': count_2A,
            'count_2C': count_2C,
        })

        log_content[project['name']] = read_log_file(project['name'])

    context = {
        'status': status,
        'log_content': log_content,
    }

    print(log_content)

    return render(request, 'report_dashboard/dashboard.html', context)
