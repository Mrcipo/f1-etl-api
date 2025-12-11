"""
Serializers for F1 API.
"""
from rest_framework import serializers
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


# ============================================================================
# DRIVER SERIALIZERS
# ============================================================================

class DriverSummarySerializer(serializers.ModelSerializer):
    """Lightweight driver serializer for nested use."""
    
    full_name = serializers.SerializerMethodField()
    
    class Meta:
        model = Driver
        fields = [
            'driver_id',
            'code',
            'number',
            'forename',
            'surname',
            'full_name',
            'nationality',
        ]
    
    def get_full_name(self, obj):
        return f"{obj.forename} {obj.surname}"


class DriverSerializer(serializers.ModelSerializer):
    """Complete driver serializer."""
    
    full_name = serializers.SerializerMethodField()
    age = serializers.SerializerMethodField()
    
    class Meta:
        model = Driver
        fields = [
            'driver_id',
            'driver_ref',
            'number',
            'code',
            'forename',
            'surname',
            'full_name',
            'date_of_birth',
            'age',
            'nationality',
            'url',
            'created_at',
            'updated_at',
        ]
    
    def get_full_name(self, obj):
        return f"{obj.forename} {obj.surname}"
    
    def get_age(self, obj):
        if obj.date_of_birth:
            from datetime import date
            today = date.today()
            return today.year - obj.date_of_birth.year - (
                (today.month, today.day) < (obj.date_of_birth.month, obj.date_of_birth.day)
            )
        return None


# ============================================================================
# CONSTRUCTOR SERIALIZERS
# ============================================================================

class ConstructorSummarySerializer(serializers.ModelSerializer):
    """Lightweight constructor serializer for nested use."""
    
    class Meta:
        model = Constructor
        fields = [
            'constructor_id',
            'name',
            'nationality',
        ]


class ConstructorSerializer(serializers.ModelSerializer):
    """Complete constructor serializer."""
    
    class Meta:
        model = Constructor
        fields = [
            'constructor_id',
            'constructor_ref',
            'name',
            'nationality',
            'url',
            'created_at',
            'updated_at',
        ]


# ============================================================================
# CIRCUIT SERIALIZERS
# ============================================================================

class CircuitSerializer(serializers.ModelSerializer):
    """Circuit serializer."""
    
    location_display = serializers.SerializerMethodField()
    
    class Meta:
        model = Circuit
        fields = [
            'circuit_id',
            'circuit_ref',
            'name',
            'location',
            'country',
            'location_display',
            'latitude',
            'longitude',
            'altitude',
            'url',
            'created_at',
            'updated_at',
        ]
    
    def get_location_display(self, obj):
        return f"{obj.location}, {obj.country}"


# ============================================================================
# RACE SERIALIZERS
# ============================================================================

class RaceSummarySerializer(serializers.ModelSerializer):
    """Lightweight race serializer for nested use."""
    
    circuit_name = serializers.CharField(source='circuit.name', read_only=True)
    
    class Meta:
        model = Race
        fields = [
            'race_id',
            'season',
            'round',
            'race_name',
            'race_date',
            'circuit_name',
        ]


class RaceSerializer(serializers.ModelSerializer):
    """Complete race serializer with nested circuit."""
    
    circuit = CircuitSerializer(read_only=True)
    
    class Meta:
        model = Race
        fields = [
            'race_id',
            'season',
            'round',
            'race_name',
            'race_date',
            'race_time',
            'circuit',
            'url',
            'created_at',
            'updated_at',
        ]


# ============================================================================
# RESULT SERIALIZERS
# ============================================================================

class ResultSerializer(serializers.ModelSerializer):
    """Result serializer for list view."""
    
    driver = DriverSummarySerializer(read_only=True)
    constructor = ConstructorSummarySerializer(read_only=True)
    race = RaceSummarySerializer(read_only=True)
    
    class Meta:
        model = Result
        fields = [
            'result_id',
            'race',
            'driver',
            'constructor',
            'grid',
            'position',
            'position_text',
            'points',
            'laps',
            'status',
        ]


class ResultDetailSerializer(serializers.ModelSerializer):
    """Detailed result serializer."""
    
    driver = DriverSerializer(read_only=True)
    constructor = ConstructorSerializer(read_only=True)
    race = RaceSerializer(read_only=True)
    position_change = serializers.SerializerMethodField()
    
    class Meta:
        model = Result
        fields = [
            'result_id',
            'race',
            'driver',
            'constructor',
            'number',
            'grid',
            'position',
            'position_text',
            'position_order',
            'position_change',
            'points',
            'laps',
            'time_milliseconds',
            'fastest_lap',
            'fastest_lap_rank',
            'fastest_lap_time',
            'fastest_lap_speed',
            'status',
            'created_at',
            'updated_at',
        ]
    
    def get_position_change(self, obj):
        if obj.position:
            return obj.grid - obj.position
        return None


# ============================================================================
# QUALIFYING SERIALIZERS
# ============================================================================

class QualifyingSerializer(serializers.ModelSerializer):
    """Qualifying serializer."""
    
    driver = DriverSummarySerializer(read_only=True)
    constructor = ConstructorSummarySerializer(read_only=True)
    race = RaceSummarySerializer(read_only=True)
    
    class Meta:
        model = Qualifying
        fields = [
            'qualifying_id',
            'race',
            'driver',
            'constructor',
            'position',
            'q1_time',
            'q2_time',
            'q3_time',
            'created_at',
            'updated_at',
        ]


# ============================================================================
# STANDINGS SERIALIZERS
# ============================================================================

class DriverStandingSerializer(serializers.ModelSerializer):
    """Driver championship standing serializer."""
    
    driver = DriverSummarySerializer(read_only=True)
    race = RaceSummarySerializer(read_only=True)
    
    class Meta:
        model = DriverStanding
        fields = [
            'standing_id',
            'race',
            'driver',
            'position',
            'position_text',
            'points',
            'wins',
            'created_at',
            'updated_at',
        ]


class ConstructorStandingSerializer(serializers.ModelSerializer):
    """Constructor championship standing serializer."""
    
    constructor = ConstructorSummarySerializer(read_only=True)
    race = RaceSummarySerializer(read_only=True)
    
    class Meta:
        model = ConstructorStanding
        fields = [
            'standing_id',
            'race',
            'constructor',
            'position',
            'position_text',
            'points',
            'wins',
            'created_at',
            'updated_at',
        ]


# ============================================================================
# METRICS SERIALIZERS
# ============================================================================

class DriverMetricsSerializer(serializers.ModelSerializer):
    """Driver season metrics serializer."""
    
    driver = DriverSummarySerializer(read_only=True)
    finish_rate = serializers.SerializerMethodField()
    podium_rate = serializers.SerializerMethodField()
    
    class Meta:
        model = DriverMetrics
        fields = [
            'metric_id',
            'driver',
            'season',
            'races_entered',
            'races_finished',
            'finish_rate',
            'podiums',
            'podium_rate',
            'wins',
            'poles',
            'dnf_count',
            'avg_finish_position',
            'avg_grid_position',
            'avg_points_per_race',
            'total_points',
            'position_changes_sum',
            'consistency_score',
            'calculated_at',
        ]
    
    def get_finish_rate(self, obj):
        if obj.races_entered > 0:
            return round((obj.races_finished / obj.races_entered) * 100, 2)
        return 0
    
    def get_podium_rate(self, obj):
        if obj.races_entered > 0:
            return round((obj.podiums / obj.races_entered) * 100, 2)
        return 0


class ConstructorMetricsSerializer(serializers.ModelSerializer):
    """Constructor season metrics serializer."""
    
    constructor = ConstructorSummarySerializer(read_only=True)
    podium_rate = serializers.SerializerMethodField()
    
    class Meta:
        model = ConstructorMetrics
        fields = [
            'metric_id',
            'constructor',
            'season',
            'races_entered',
            'podiums',
            'podium_rate',
            'wins',
            'one_two_finishes',
            'double_dnf',
            'avg_finish_position',
            'total_points',
            'reliability_rate',
            'calculated_at',
        ]
    
    def get_podium_rate(self, obj):
        if obj.races_entered > 0:
            return round((obj.podiums / obj.races_entered) * 100, 2)
        return 0


# ============================================================================
# COMPLEX RESPONSE SERIALIZERS
# ============================================================================

class RaceCompleteSerializer(serializers.ModelSerializer):
    """Complete race data with results, qualifying, and standings."""
    
    circuit = CircuitSerializer(read_only=True)
    results = ResultSerializer(many=True, read_only=True, source='results.all')
    qualifying_results = QualifyingSerializer(many=True, read_only=True, source='qualifying_results.all')
    driver_standings = DriverStandingSerializer(many=True, read_only=True, source='driver_standings.all')
    constructor_standings = ConstructorStandingSerializer(many=True, read_only=True, source='constructor_standings.all')
    
    class Meta:
        model = Race
        fields = [
            'race_id',
            'season',
            'round',
            'race_name',
            'race_date',
            'race_time',
            'circuit',
            'url',
            'results',
            'qualifying_results',
            'driver_standings',
            'constructor_standings',
        ]