from django.db import models


class PosocoTableA(models.Model):
    """Table A - Demand, Energy, Hydro, Wind, etc. by Region"""
    category = models.CharField(max_length=255)
    nr = models.CharField(max_length=50, null=True, blank=True)
    wr = models.CharField(max_length=50, null=True, blank=True)
    sr = models.CharField(max_length=50, null=True, blank=True)
    er = models.CharField(max_length=50, null=True, blank=True)
    ner = models.CharField(max_length=50, null=True, blank=True)
    total = models.CharField(max_length=50, null=True, blank=True)
    report_date = models.DateField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'posoco_posocotablea'  # 👈 Add this line to specify the exact table name

    def __str__(self):
        return f"TableA | {self.category} | {self.report_date}"


class PosocoTableG(models.Model):
    """Table G - Generation mix by fuel type"""
    fuel_type = models.CharField(max_length=255)
    nr = models.CharField(max_length=50, null=True, blank=True)
    wr = models.CharField(max_length=50, null=True, blank=True)
    sr = models.CharField(max_length=50, null=True, blank=True)
    er = models.CharField(max_length=50, null=True, blank=True)
    ner = models.CharField(max_length=50, null=True, blank=True)
    all_india = models.CharField(max_length=50, null=True, blank=True)
    share_percent = models.CharField(max_length=50, null=True, blank=True)
    report_date = models.DateField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'posoco_posocotableg'  # 👈 Add this line for the second table as well

    def __str__(self):
        return f"TableG | {self.fuel_type} | {self.report_date}"