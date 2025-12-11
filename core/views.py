"""
API views for F1 data.
"""
from rest_framework import viewsets, filters, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.views import APIView
from django_filters.rest_framework import DjangoFilterBackend

from core.models import (
    Driver,
    Constructor,
    Circuit,
    Race,
    Result,
    Qualifying,
    DriverStanding,
    ConstructorStanding,
    DriverMetrics,
    ConstructorMetrics,
)
from core.serializers import (
    DriverSerializer,
    ConstructorSerializer,
    CircuitSerializer,
    RaceSerializer,
    ResultSerializer,
    ResultDetailSerializer,
    QualifyingSerializer,
    DriverStandingSerializer,
    ConstructorStandingSerializer,
    DriverMetricsSerializer,
    ConstructorMetricsSerializer,
    RaceCompleteSerializer,
)


# ============================================================================
# VIEWSETS
# ============================================================================

class DriverViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for drivers.
    
    list: Get all drivers with optional filtering and search
    retrieve: Get a specific driver by driver_id
    """
    queryset = Driver.objects.all()
    serializer_class = DriverSerializer
    lookup_field = 'driver_id'
    filter_backends = [DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter]
    filterset_fields = ['nationality', 'code']
    search_fields = ['forename', 'surname', 'driver_id']
    ordering_fields = ['surname', 'forename', 'date_of_birth']
    ordering = ['surname']


class ConstructorViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for constructors.
    
    list: Get all constructors with optional filtering and search
    retrieve: Get a specific constructor by constructor_id
    """
    queryset = Constructor.objects.all()
    serializer_class = ConstructorSerializer
    lookup_field = 'constructor_id'
    filter_backends = [DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter]
    filterset_fields = ['nationality']
    search_fields = ['name', 'constructor_id']
    ordering_fields = ['name']
    ordering = ['name']


class CircuitViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for circuits.
    
    list: Get all circuits with optional filtering and search
    retrieve: Get a specific circuit by circuit_id
    """
    queryset = Circuit.objects.all()
    serializer_class = CircuitSerializer
    lookup_field = 'circuit_id'
    filter_backends = [DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter]
    filterset_fields = ['country']
    search_fields = ['name', 'location', 'country']
    ordering_fields = ['name', 'country']
    ordering = ['name']


class RaceViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for races.
    
    list: Get all races with optional filtering
    retrieve: Get a specific race by race_id
    complete: Get complete race data including results, qualifying, standings
    """
    queryset = Race.objects.select_related('circuit').all()
    serializer_class = RaceSerializer
    filter_backends = [DjangoFilterBackend, filters.OrderingFilter]
    filterset_fields = ['season', 'round', 'circuit']
    ordering_fields = ['season', 'round', 'race_date']
    ordering = ['-season', 'round']
    
    @action(detail=True, methods=['get'])
    def complete(self, request, pk=None):
        """
        Get complete race data with results, qualifying, and standings.
        
        Returns all related data for a race in a single response.
        """
        race = self.get_object()
        serializer = RaceCompleteSerializer(race)
        return Response(serializer.data)


class ResultViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for race results.
    
    list: Get race results with filtering
    retrieve: Get detailed result
    """
    queryset = Result.objects.select_related('race', 'driver', 'constructor').all()
    filter_backends = [DjangoFilterBackend, filters.OrderingFilter]
    filterset_fields = {
        'race__season': ['exact'],
        'race__round': ['exact'],
        'driver__driver_id': ['exact'],
        'constructor__constructor_id': ['exact'],
        'position': ['exact', 'lte', 'gte'],
        'grid': ['exact', 'lte', 'gte'],
        'points': ['gte'],
    }
    ordering_fields = ['race__season', 'race__round', 'position', 'points', 'grid']
    ordering = ['race__season', 'race__round', 'position_order']
    
    def get_serializer_class(self):
        if self.action == 'retrieve':
            return ResultDetailSerializer
        return ResultSerializer


class QualifyingViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for qualifying results.
    
    list: Get qualifying results with filtering
    retrieve: Get specific qualifying result
    """
    queryset = Qualifying.objects.select_related('race', 'driver', 'constructor').all()
    serializer_class = QualifyingSerializer
    filter_backends = [DjangoFilterBackend, filters.OrderingFilter]
    filterset_fields = {
        'race__season': ['exact'],
        'race__round': ['exact'],
        'driver__driver_id': ['exact'],
        'position': ['exact', 'lte'],
    }
    ordering_fields = ['race__season', 'race__round', 'position']
    ordering = ['race__season', 'race__round', 'position']


class DriverMetricsViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for driver metrics.
    
    list: Get driver metrics with filtering by season
    retrieve: Get specific driver metrics
    """
    queryset = DriverMetrics.objects.select_related('driver').all()
    serializer_class = DriverMetricsSerializer
    filter_backends = [DjangoFilterBackend, filters.OrderingFilter]
    filterset_fields = {
        'season': ['exact'],
        'driver__driver_id': ['exact'],
        'wins': ['gte'],
        'podiums': ['gte'],
        'total_points': ['gte'],
    }
    ordering_fields = ['season', 'total_points', 'wins', 'podiums', 'avg_finish_position']
    ordering = ['-season', '-total_points']


class ConstructorMetricsViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for constructor metrics.
    
    list: Get constructor metrics with filtering by season
    retrieve: Get specific constructor metrics
    """
    queryset = ConstructorMetrics.objects.select_related('constructor').all()
    serializer_class = ConstructorMetricsSerializer
    filter_backends = [DjangoFilterBackend, filters.OrderingFilter]
    filterset_fields = {
        'season': ['exact'],
        'constructor__constructor_id': ['exact'],
        'wins': ['gte'],
        'total_points': ['gte'],
    }
    ordering_fields = ['season', 'total_points', 'wins', 'reliability_rate']
    ordering = ['-season', '-total_points']


# ============================================================================
# CUSTOM VIEWS
# ============================================================================

class DriverStandingsView(APIView):
    """
    Get driver championship standings.
    
    Query parameters:
        - season (required): Season year
        - round (optional): Specific round (default: latest)
    """
    
    def get(self, request):
        season = request.query_params.get('season')
        round_number = request.query_params.get('round')
        
        if not season:
            return Response(
                {'error': 'season parameter is required'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            season = int(season)
        except ValueError:
            return Response(
                {'error': 'season must be an integer'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Get standings
        queryset = DriverStanding.objects.filter(
            race__season=season
        ).select_related('driver', 'race')
        
        if round_number:
            try:
                round_number = int(round_number)
                queryset = queryset.filter(race__round=round_number)
            except ValueError:
                return Response(
                    {'error': 'round must be an integer'},
                    status=status.HTTP_400_BAD_REQUEST
                )
        else:
            # Get latest round for the season
            latest_round = queryset.values_list('race__round', flat=True).order_by('-race__round').first()
            if latest_round:
                queryset = queryset.filter(race__round=latest_round)
        
        queryset = queryset.order_by('position')
        serializer = DriverStandingSerializer(queryset, many=True)
        
        return Response({
            'season': season,
            'round': round_number,
            'standings': serializer.data
        })


class ConstructorStandingsView(APIView):
    """
    Get constructor championship standings.
    
    Query parameters:
        - season (required): Season year
        - round (optional): Specific round (default: latest)
    """
    
    def get(self, request):
        season = request.query_params.get('season')
        round_number = request.query_params.get('round')
        
        if not season:
            return Response(
                {'error': 'season parameter is required'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            season = int(season)
        except ValueError:
            return Response(
                {'error': 'season must be an integer'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Get standings
        queryset = ConstructorStanding.objects.filter(
            race__season=season
        ).select_related('constructor', 'race')
        
        if round_number:
            try:
                round_number = int(round_number)
                queryset = queryset.filter(race__round=round_number)
            except ValueError:
                return Response(
                    {'error': 'round must be an integer'},
                    status=status.HTTP_400_BAD_REQUEST
                )
        else:
            # Get latest round for the season
            latest_round = queryset.values_list('race__round', flat=True).order_by('-race__round').first()
            if latest_round:
                queryset = queryset.filter(race__round=latest_round)
        
        queryset = queryset.order_by('position')
        serializer = ConstructorStandingSerializer(queryset, many=True)
        
        return Response({
            'season': season,
            'round': round_number,
            'standings': serializer.data
        })


class CompareDriversView(APIView):
    """
    Compare statistics between multiple drivers.
    
    Query parameters:
        - driver_ids (required): Comma-separated driver IDs (e.g., 'hamilton,verstappen')
        - season (required): Season year for comparison
    """
    
    def get(self, request):
        driver_ids_param = request.query_params.get('driver_ids')
        season = request.query_params.get('season')
        
        if not driver_ids_param or not season:
            return Response(
                {'error': 'driver_ids and season parameters are required'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            season = int(season)
        except ValueError:
            return Response(
                {'error': 'season must be an integer'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        driver_ids = [d.strip() for d in driver_ids_param.split(',')]
        
        if len(driver_ids) < 2:
            return Response(
                {'error': 'At least 2 drivers are required for comparison'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Get drivers and their metrics
        drivers_data = []
        for driver_id in driver_ids:
            try:
                driver = Driver.objects.get(driver_id=driver_id)
                metrics = DriverMetrics.objects.get(driver=driver, season=season)
                
                # Calculate head-to-head stats
                results = Result.objects.filter(
                    driver=driver,
                    race__season=season,
                    position__isnull=False
                ).order_by('race__round')
                
                drivers_data.append({
                    'driver': DriverSerializer(driver).data,
                    'metrics': DriverMetricsSerializer(metrics).data,
                    'results_count': results.count(),
                })
            except Driver.DoesNotExist:
                return Response(
                    {'error': f'Driver {driver_id} not found'},
                    status=status.HTTP_404_NOT_FOUND
                )
            except DriverMetrics.DoesNotExist:
                return Response(
                    {'error': f'No metrics found for driver {driver_id} in season {season}'},
                    status=status.HTTP_404_NOT_FOUND
                )
        
        # Calculate comparison stats
        comparison = {
            'avg_points_gap': abs(drivers_data[0]['metrics']['avg_points_per_race'] - 
                                drivers_data[1]['metrics']['avg_points_per_race']),
            'total_points_gap': abs(drivers_data[0]['metrics']['total_points'] - 
                                   drivers_data[1]['metrics']['total_points']),
            'wins_difference': drivers_data[0]['metrics']['wins'] - drivers_data[1]['metrics']['wins'],
        }
        
        return Response({
            'season': season,
            'drivers': drivers_data,
            'comparison': comparison,
        })