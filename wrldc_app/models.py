from django.db import models
from datetime import date

class Wrldc2AData(models.Model):
    report_date = models.DateField()  # Multiple states per day allowed
    state = models.CharField(max_length=100, null=True, blank=True)

    thermal = models.CharField(max_length=50, null=True, blank=True)      
    hydro = models.CharField(max_length=50, null=True, blank=True)        
    gas = models.CharField(max_length=50, null=True, blank=True)         
    solar = models.CharField(max_length=50, null=True, blank=True)        
    wind = models.CharField(max_length=50, null=True, blank=True)         
    others = models.CharField(max_length=50, null=True, blank=True)       
    total = models.CharField(max_length=50, null=True, blank=True)        
    net_sch = models.CharField(max_length=50, null=True, blank=True)      
    drawal = models.CharField(max_length=50, null=True, blank=True)       
    ui = models.CharField(max_length=50, null=True, blank=True)           
    availability = models.CharField(max_length=50, null=True, blank=True) 
    consumption = models.CharField(max_length=50, null=True, blank=True)  
    shortage = models.CharField(max_length=50, null=True, blank=True)     
    requirement = models.CharField(max_length=50, null=True, blank=True)  

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Table 2A Data for {self.report_date} - {self.state}"

    class Meta:
        verbose_name = "Table 2A Data"
        verbose_name_plural = "Table 2A Data"
        unique_together = ('report_date', 'state')


class Wrldc2CData(models.Model):
    report_date = models.DateField(default=date.today)
    state = models.CharField(max_length=100, null=True, blank=True)

    max_demand_day = models.FloatField(null=True, blank=True)
    time = models.CharField(max_length=50, null=True, blank=True)
    shortage_max_demand = models.CharField(max_length=50, null=True, blank=True)   
    req_max_demand = models.CharField(max_length=50, null=True, blank=True)    
    ace_max = models.CharField(max_length=50, null=True, blank=True)                      
    time_ace_max = models.CharField(max_length=50, null=True, blank=True)
    ace_min = models.CharField(max_length=50, null=True, blank=True)                      
    time_ace_min = models.CharField(max_length=50, null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)


    def __str__(self):
        return f"Table 2C Data for {self.report_date} - {self.state}"

    class Meta:
        verbose_name = "Table 2C Data"
        verbose_name_plural = "Table 2C Data"
        unique_together = ('report_date', 'state')