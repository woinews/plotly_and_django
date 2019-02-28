from visual_plotly.views import charts,charts2
from django.conf.urls import url


urlpatterns = [
    url(r'^$', charts),
    url(r'^charts2$', charts2),
]
