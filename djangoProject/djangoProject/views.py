from django.shortcuts import render


def runoob(request):
    context = {}
    context['hello'] = "Hello world!"
    return render(request, 'runoob.html', context)
