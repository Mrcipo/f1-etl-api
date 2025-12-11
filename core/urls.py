"""
URL configuration for F1 API.
"""
from django.urls import path, include
from rest_framework.routers import DefaultRouter

from core.views import (
    DriverViewSet,
    ConstructorViewSet,
    CircuitViewSet,
    RaceViewSet,
    ResultViewSet,
    QualifyingViewSet,
    DriverMetricsViewSet,
    ConstructorMetricsViewSet,
    DriverStandingsView,
    ConstructorStandingsView,
    CompareDriversView,
)

# Create router and register viewsets
router = DefaultRouter()
router.register(r'drivers', DriverViewSet, basename='driver')
router.register(r'constructors', ConstructorViewSet, basename='constructor')
router.register(r'circuits', CircuitViewSet, basename='circuit')
router.register(r'races', RaceViewSet, basename='race')
router.register(r'results', ResultViewSet, basename='result')
router.register(r'qualifying', QualifyingViewSet, basename='qualifying')
router.register(r'metrics/drivers', DriverMetricsViewSet, basename='driver-metrics')
router.register(r'metrics/constructors', ConstructorMetricsViewSet, basename='constructor-metrics')

# Custom URL patterns
urlpatterns = [
    # Router URLs
    path('', include(router.urls)),
    
    # Custom endpoints
    path('standings/drivers/', DriverStandingsView.as_view(), name='driver-standings'),
    path('standings/constructors/', ConstructorStandingsView.as_view(), name='constructor-standings'),
    path('analytics/compare/drivers/', CompareDriversView.as_view(), name='compare-drivers'),
]