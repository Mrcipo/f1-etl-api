from django.db import models


class TimestampedModel(models.Model):
    """Abstract base model with timestamp fields."""
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class Driver(TimestampedModel):
    """Represents a Formula 1 driver."""
    driver_id = models.CharField(max_length=100, primary_key=True)
    driver_ref = models.CharField(max_length=100)
    number = models.IntegerField(null=True, blank=True)
    code = models.CharField(max_length=3, null=True, blank=True)
    forename = models.CharField(max_length=100)
    surname = models.CharField(max_length=100)
    date_of_birth = models.DateField(null=True, blank=True)
    nationality = models.CharField(max_length=100, null=True, blank=True)
    url = models.URLField(max_length=500)

    class Meta:
        ordering = ['surname', 'forename']
        indexes = [
            models.Index(fields=['surname']),
        ]

    def __str__(self) -> str:
        return f"{self.forename} {self.surname}"


class Constructor(TimestampedModel):
    """Represents a Formula 1 team/constructor."""
    constructor_id = models.CharField(max_length=100, primary_key=True)
    constructor_ref = models.CharField(max_length=100)
    name = models.CharField(max_length=200)
    nationality = models.CharField(max_length=100, null=True, blank=True)
    url = models.URLField(max_length=500)

    class Meta:
        ordering = ['name']
        indexes = [
            models.Index(fields=['name']),
        ]

    def __str__(self) -> str:
        return self.name


class Circuit(TimestampedModel):
    """Represents a Formula 1 circuit/track."""
    circuit_id = models.CharField(max_length=100, primary_key=True)
    circuit_ref = models.CharField(max_length=100)
    name = models.CharField(max_length=200)
    location = models.CharField(max_length=200)
    country = models.CharField(max_length=100)
    latitude = models.FloatField(null=True, blank=True)
    longitude = models.FloatField(null=True, blank=True)
    altitude = models.IntegerField(null=True, blank=True)
    url = models.URLField(max_length=500)

    class Meta:
        ordering = ['name']
        indexes = [
            models.Index(fields=['country']),
        ]

    def __str__(self) -> str:
        return f"{self.name} ({self.country})"


class Race(TimestampedModel):
    """Represents a specific race in a season."""
    race_id = models.AutoField(primary_key=True)
    season = models.IntegerField()
    round = models.IntegerField()
    circuit = models.ForeignKey(Circuit, on_delete=models.PROTECT, related_name='races')
    race_name = models.CharField(max_length=200)
    race_date = models.DateField()
    race_time = models.TimeField(null=True, blank=True)
    url = models.URLField(max_length=500)

    class Meta:
        ordering = ['-season', 'round']
        indexes = [
            models.Index(fields=['season']),
            models.Index(fields=['round']),
            models.Index(fields=['season', 'round']),
        ]
        constraints = [
            models.UniqueConstraint(fields=['season', 'round'], name='unique_season_round'),
        ]

    def __str__(self) -> str:
        return f"{self.season} - Round {self.round}: {self.race_name}"


class Qualifying(TimestampedModel):
    """Qualifying results (grid positions)."""
    qualifying_id = models.AutoField(primary_key=True)
    race = models.ForeignKey(Race, on_delete=models.CASCADE, related_name='qualifying_results')
    driver = models.ForeignKey(Driver, on_delete=models.PROTECT, related_name='qualifying_results')
    constructor = models.ForeignKey(Constructor, on_delete=models.PROTECT, related_name='qualifying_results')
    position = models.IntegerField()
    q1_time = models.CharField(max_length=50, null=True, blank=True)
    q2_time = models.CharField(max_length=50, null=True, blank=True)
    q3_time = models.CharField(max_length=50, null=True, blank=True)

    class Meta:
        ordering = ['race', 'position']
        indexes = [
            models.Index(fields=['race']),
            models.Index(fields=['driver']),
            models.Index(fields=['race', 'driver']),
        ]
        constraints = [
            models.UniqueConstraint(fields=['race', 'driver'], name='unique_qualifying_race_driver'),
        ]
        verbose_name_plural = 'Qualifying results'

    def __str__(self) -> str:
        return f"{self.race} - {self.driver} (P{self.position})"


class Result(TimestampedModel):
    """Race result for a driver."""
    result_id = models.AutoField(primary_key=True)
    race = models.ForeignKey(Race, on_delete=models.CASCADE, related_name='results')
    driver = models.ForeignKey(Driver, on_delete=models.PROTECT, related_name='results')
    constructor = models.ForeignKey(Constructor, on_delete=models.PROTECT, related_name='results')
    number = models.IntegerField()
    grid = models.IntegerField()
    position = models.IntegerField(null=True, blank=True)
    position_text = models.CharField(max_length=10)
    position_order = models.IntegerField()
    points = models.FloatField()
    laps = models.IntegerField()
    time_milliseconds = models.BigIntegerField(null=True, blank=True)
    fastest_lap = models.IntegerField(null=True, blank=True)
    fastest_lap_rank = models.IntegerField(null=True, blank=True)
    fastest_lap_time = models.CharField(max_length=50, null=True, blank=True)
    fastest_lap_speed = models.FloatField(null=True, blank=True)
    status = models.CharField(max_length=100)

    class Meta:
        ordering = ['race', 'position_order']
        indexes = [
            models.Index(fields=['race']),
            models.Index(fields=['driver']),
            models.Index(fields=['constructor']),
            models.Index(fields=['position']),
            models.Index(fields=['race', 'driver']),
        ]
        constraints = [
            models.UniqueConstraint(fields=['race', 'driver'], name='unique_result_race_driver'),
        ]

    def __str__(self) -> str:
        return f"{self.race} - {self.driver} ({self.position_text})"


class DriverStanding(TimestampedModel):
    """Driver championship standings after each race."""
    standing_id = models.AutoField(primary_key=True)
    race = models.ForeignKey(Race, on_delete=models.CASCADE, related_name='driver_standings')
    driver = models.ForeignKey(Driver, on_delete=models.PROTECT, related_name='standings')
    points = models.FloatField()
    position = models.IntegerField()
    position_text = models.CharField(max_length=10)
    wins = models.IntegerField()

    class Meta:
        ordering = ['race', 'position']
        indexes = [
            models.Index(fields=['race']),
            models.Index(fields=['driver']),
            models.Index(fields=['race', 'driver']),
        ]
        constraints = [
            models.UniqueConstraint(fields=['race', 'driver'], name='unique_driverstanding_race_driver'),
        ]

    def __str__(self) -> str:
        return f"{self.race} - {self.driver} (P{self.position}, {self.points} pts)"


class ConstructorStanding(TimestampedModel):
    """Constructor championship standings after each race."""
    standing_id = models.AutoField(primary_key=True)
    race = models.ForeignKey(Race, on_delete=models.CASCADE, related_name='constructor_standings')
    constructor = models.ForeignKey(Constructor, on_delete=models.PROTECT, related_name='standings')
    points = models.FloatField()
    position = models.IntegerField()
    position_text = models.CharField(max_length=10)
    wins = models.IntegerField()

    class Meta:
        ordering = ['race', 'position']
        indexes = [
            models.Index(fields=['race']),
            models.Index(fields=['constructor']),
            models.Index(fields=['race', 'constructor']),
        ]
        constraints = [
            models.UniqueConstraint(fields=['race', 'constructor'], name='unique_constructorstanding_race_constructor'),
        ]

    def __str__(self) -> str:
        return f"{self.race} - {self.constructor} (P{self.position}, {self.points} pts)"


class DriverMetrics(TimestampedModel):
    """Aggregated metrics per driver per season."""
    metric_id = models.AutoField(primary_key=True)
    driver = models.ForeignKey(Driver, on_delete=models.PROTECT, related_name='metrics')
    season = models.IntegerField()
    races_entered = models.IntegerField()
    races_finished = models.IntegerField()
    podiums = models.IntegerField()
    wins = models.IntegerField()
    poles = models.IntegerField()
    dnf_count = models.IntegerField()
    avg_finish_position = models.FloatField(null=True, blank=True)
    avg_grid_position = models.FloatField(null=True, blank=True)
    avg_points_per_race = models.FloatField()
    total_points = models.FloatField()
    position_changes_sum = models.IntegerField()
    consistency_score = models.FloatField()
    calculated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-season', '-total_points']
        indexes = [
            models.Index(fields=['driver']),
            models.Index(fields=['season']),
            models.Index(fields=['driver', 'season']),
        ]
        constraints = [
            models.UniqueConstraint(fields=['driver', 'season'], name='unique_drivermetrics_driver_season'),
        ]
        verbose_name_plural = 'Driver metrics'

    def __str__(self) -> str:
        return f"{self.driver} - {self.season} ({self.total_points} pts)"


class ConstructorMetrics(TimestampedModel):
    """Aggregated metrics per constructor per season."""
    metric_id = models.AutoField(primary_key=True)
    constructor = models.ForeignKey(Constructor, on_delete=models.PROTECT, related_name='metrics')
    season = models.IntegerField()
    races_entered = models.IntegerField()
    podiums = models.IntegerField()
    wins = models.IntegerField()
    one_two_finishes = models.IntegerField()
    double_dnf = models.IntegerField()
    avg_finish_position = models.FloatField(null=True, blank=True)
    total_points = models.FloatField()
    reliability_rate = models.FloatField()
    calculated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-season', '-total_points']
        indexes = [
            models.Index(fields=['constructor']),
            models.Index(fields=['season']),
            models.Index(fields=['constructor', 'season']),
        ]
        constraints = [
            models.UniqueConstraint(fields=['constructor', 'season'], name='unique_constructormetrics_constructor_season'),
        ]
        verbose_name_plural = 'Constructor metrics'

    def __str__(self) -> str:
        return f"{self.constructor} - {self.season} ({self.total_points} pts)"

class ETLRunStatus(models.TextChoices):
    SUCCESS = "SUCCESS", "Success"
    FAILED = "FAILED", "Failed"
    PARTIAL = "PARTIAL", "Partial"

class ETLRun(TimestampedModel):
    """Records each ETL pipeline execution."""
    started_at = models.DateTimeField()
    finished_at = models.DateTimeField(null=True, blank=True)
    status = models.CharField(
        max_length=20,
        choices=ETLRunStatus.choices,
    )
    seasons_processed = models.JSONField(default=list)
    races_added = models.IntegerField(default=0)
    error_log = models.TextField(null=True, blank=True)

    class Meta:
        ordering = ['-started_at']
        indexes = [
            models.Index(fields=['status']),
            models.Index(fields=['-started_at']),
        ]

    def __str__(self) -> str:
        return f"ETL Run {self.started_at.strftime('%Y-%m-%d %H:%M')} - {self.status}"