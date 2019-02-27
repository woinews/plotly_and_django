from visual_plotly.views import charts
from django.conf.urls import url


urlpatterns = [
    url(r'^$', charts),
]
