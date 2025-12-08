from django.contrib import admin
from .models import (
    Driver,
    Constructor,
    Circuit,
    Race,
    Qualifying,
    Result,
    DriverStanding,
    ConstructorStanding,
    DriverMetrics,
    ConstructorMetrics,
    ETLRun,
)


@admin.register(Driver)
class DriverAdmin(admin.ModelAdmin):
    list_display = ("driver_id", "code", "forename", "surname", "nationality")
    search_fields = ("driver_id", "forename", "surname", "code", "nationality")
    list_filter = ("nationality",)
    ordering = ("surname", "forename")


@admin.register(Constructor)
class ConstructorAdmin(admin.ModelAdmin):
    list_display = ("constructor_id", "name", "nationality")
    search_fields = ("constructor_id", "name", "nationality")
    list_filter = ("nationality",)
    ordering = ("name",)


@admin.register(Circuit)
class CircuitAdmin(admin.ModelAdmin):
    list_display = ("circuit_id", "name", "location", "country")
    search_fields = ("circuit_id", "name", "location", "country")
    list_filter = ("country",)
    ordering = ("country", "name")


@admin.register(Race)
class RaceAdmin(admin.ModelAdmin):
    list_display = ("race_id", "season", "round", "race_name", "circuit", "race_date")
    list_filter = ("season", "circuit__country")
    search_fields = ("race_name", "circuit__name", "circuit__country")
    ordering = ("-season", "round")
    autocomplete_fields = ("circuit",)


@admin.register(Qualifying)
class QualifyingAdmin(admin.ModelAdmin):
    list_display = ("race", "position", "driver", "constructor", "q1_time", "q2_time", "q3_time")
    list_filter = ("race__season", "race__circuit__country", "constructor")
    search_fields = ("driver__forename", "driver__surname", "constructor__name", "race__race_name")
    ordering = ("race", "position")
    autocomplete_fields = ("race", "driver", "constructor")


@admin.register(Result)
class ResultAdmin(admin.ModelAdmin):
    list_display = (
        "race",
        "driver",
        "constructor",
        "grid",
        "position",
        "position_text",
        "points",
        "laps",
        "status",
    )
    list_filter = (
        "race__season",
        "constructor",
        "status",
    )
    search_fields = (
        "driver__forename",
        "driver__surname",
        "constructor__name",
        "race__race_name",
    )
    ordering = ("race", "position_order")
    autocomplete_fields = ("race", "driver", "constructor")


@admin.register(DriverStanding)
class DriverStandingAdmin(admin.ModelAdmin):
    list_display = ("race", "driver", "position", "points", "wins")
    list_filter = ("race__season",)
    search_fields = ("driver__forename", "driver__surname", "race__race_name")
    ordering = ("race", "position")
    autocomplete_fields = ("race", "driver")


@admin.register(ConstructorStanding)
class ConstructorStandingAdmin(admin.ModelAdmin):
    list_display = ("race", "constructor", "position", "points", "wins")
    list_filter = ("race__season",)
    search_fields = ("constructor__name", "race__race_name")
    ordering = ("race", "position")
    autocomplete_fields = ("race", "constructor")


@admin.register(DriverMetrics)
class DriverMetricsAdmin(admin.ModelAdmin):
    list_display = (
        "driver",
        "season",
        "races_entered",
        "races_finished",
        "wins",
        "podiums",
        "total_points",
        "avg_finish_position",
        "avg_grid_position",
        "avg_points_per_race",
    )
    list_filter = ("season",)
    search_fields = ("driver__forename", "driver__surname")
    ordering = ("-season", "-total_points")
    autocomplete_fields = ("driver",)


@admin.register(ConstructorMetrics)
class ConstructorMetricsAdmin(admin.ModelAdmin):
    list_display = (
        "constructor",
        "season",
        "races_entered",
        "wins",
        "podiums",
        "total_points",
        "avg_finish_position",
        "reliability_rate",
    )
    list_filter = ("season",)
    search_fields = ("constructor__name",)
    ordering = ("-season", "-total_points")
    autocomplete_fields = ("constructor",)


@admin.register(ETLRun)
class ETLRunAdmin(admin.ModelAdmin):
    list_display = ("started_at", "finished_at", "status", "races_added")
    list_filter = ("status",)
    ordering = ("-started_at",)
    readonly_fields = ("created_at", "updated_at")
    