from django.db import models

class AutomationJob(models.Model):
    class Status(models.TextChoices):
        IDLE = 'IDLE', 'Idle'
        RUNNING = 'RUNNING', 'Running'
        SUCCESS = 'SUCCESS', 'Success'
        FAILED = 'FAILED', 'Failed'

    script_name = models.CharField(max_length=100, primary_key=True)
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.IDLE)
    last_run_time = models.DateTimeField(null=True, blank=True)
    last_success_time = models.DateTimeField(null=True, blank=True)
    is_data_available_today = models.BooleanField(default=False)
    log_message = models.TextField(blank=True, null=True)

    def __str__(self):
        return self.script_name

    class Meta:
        ordering = ['script_name']