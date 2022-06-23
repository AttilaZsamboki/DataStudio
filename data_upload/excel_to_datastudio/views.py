from distutils.command.upload import upload
from re import template
from django.shortcuts import render

# Create your views here.
def home(request):
    context = {}
    return render(request, 'excel_to_datastudio/home.html')