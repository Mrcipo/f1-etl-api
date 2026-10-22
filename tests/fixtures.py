"""
Synthetic Ergast/Jolpi API responses for a tiny two-race season.

The shapes mirror the real API so the parsers, cleaners and loaders can be
exercised end to end without network access.
"""
SEASON = 2023

DRIVERS = {
    "max_verstappen": {
        "driverId": "max_verstappen", "permanentNumber": "1", "code": "VER",
        "givenName": "Max", "familyName": "Verstappen", "dateOfBirth": "1997-09-30",
        "nationality": "Dutch", "url": "http://en.wikipedia.org/wiki/Max_Verstappen",
    },
    "perez": {
        "driverId": "perez", "permanentNumber": "11", "code": "PER",
        "givenName": "Sergio", "familyName": "Pérez", "dateOfBirth": "1990-01-26",
        "nationality": "Mexican", "url": "http://en.wikipedia.org/wiki/Sergio_P%C3%A9rez",
    },
    "alonso": {
        "driverId": "alonso", "permanentNumber": "14", "code": "ALO",
        "givenName": "Fernando", "familyName": "Alonso", "dateOfBirth": "1981-07-29",
        "nationality": "Spanish", "url": "http://en.wikipedia.org/wiki/Fernando_Alonso",
    },
}

CONSTRUCTORS = {
    "red_bull": {
        "constructorId": "red_bull", "name": "Red Bull", "nationality": "Austrian",
        "url": "http://en.wikipedia.org/wiki/Red_Bull_Racing",
    },
    "aston_martin": {
        "constructorId": "aston_martin", "name": "Aston Martin", "nationality": "British",
        "url": "http://en.wikipedia.org/wiki/Aston_Martin_in_Formula_One",
    },
}

TEAM_OF = {"max_verstappen": "red_bull", "perez": "red_bull", "alonso": "aston_martin"}

CIRCUITS = {
    1: {
        "circuitId": "bahrain", "circuitName": "Bahrain International Circuit",
        "url": "http://en.wikipedia.org/wiki/Bahrain_International_Circuit",
        "Location": {"lat": "26.0325", "long": "50.5106", "locality": "Sakhir", "country": "Bahrain"},
    },
    2: {
        "circuitId": "jeddah", "circuitName": "Jeddah Corniche Circuit",
        "url": "http://en.wikipedia.org/wiki/Jeddah_Street_Circuit",
        "Location": {"lat": "21.6319", "long": "39.1044", "locality": "Jeddah", "country": "Saudi Arabia"},
    },
}

# (driver_id, grid, position or None for DNF, positionText, points, status)
RESULTS = {
    1: [
        ("max_verstappen", 1, "1", "1", "25", "Finished"),
        ("perez", 2, "2", "2", "18", "Finished"),
        ("alonso", 5, None, "R", "0", "Engine"),
    ],
    2: [
        ("perez", 1, "1", "1", "25", "Finished"),
        ("max_verstappen", 15, "2", "2", "19", "Finished"),
        ("alonso", 2, "3", "3", "15", "Finished"),
    ],
}


def _mrdata(table_name, table):
    return {"MRData": {table_name: table}}


def races_json():
    races = [
        {
            "season": str(SEASON), "round": str(rnd),
            "raceName": f"Round {rnd} Grand Prix",
            "url": f"http://en.wikipedia.org/wiki/{SEASON}_round_{rnd}",
            "Circuit": CIRCUITS[rnd],
            "date": f"{SEASON}-03-0{rnd + 4}", "time": "15:00:00Z",
        }
        for rnd in sorted(RESULTS)
    ]
    return _mrdata("RaceTable", {"season": str(SEASON), "Races": races})


def results_json(rnd):
    results = []
    for order, (driver_id, grid, position, position_text, points, status) in enumerate(RESULTS[rnd], 1):
        row = {
            "number": DRIVERS[driver_id]["permanentNumber"],
            "positionText": position_text,
            "positionOrder": str(order),
            "points": points,
            "Driver": DRIVERS[driver_id],
            "Constructor": CONSTRUCTORS[TEAM_OF[driver_id]],
            "grid": str(grid),
            "laps": "57" if position else "30",
            "status": status,
        }
        if position:
            row["position"] = position
            row["Time"] = {"millis": str(5_400_000 + order * 1000)}
            row["FastestLap"] = {
                "rank": str(order), "lap": "40",
                "Time": {"time": f"1:3{order}.000"},
                "AverageSpeed": {"units": "kph", "speed": "206.018"},
            }
        results.append(row)
    return _mrdata("RaceTable", {"Races": [{"season": str(SEASON), "round": str(rnd), "Results": results}]})


def qualifying_json(rnd):
    by_grid = sorted(RESULTS[rnd], key=lambda r: r[1])
    rows = [
        {
            "number": DRIVERS[driver_id]["permanentNumber"],
            "position": str(pos),
            "Driver": DRIVERS[driver_id],
            "Constructor": CONSTRUCTORS[TEAM_OF[driver_id]],
            "Q1": "1:31.000", "Q2": "1:30.000", "Q3": f"1:29.{pos:03d}",
        }
        for pos, (driver_id, *_rest) in enumerate(by_grid, 1)
    ]
    return _mrdata("RaceTable", {"Races": [{"season": str(SEASON), "round": str(rnd), "QualifyingResults": rows}]})


def driver_standings_json():
    standings = [
        {"position": "1", "positionText": "1", "points": "44", "wins": "1", "Driver": DRIVERS["max_verstappen"]},
        {"position": "2", "positionText": "2", "points": "43", "wins": "1", "Driver": DRIVERS["perez"]},
        {"position": "3", "positionText": "3", "points": "15", "wins": "0", "Driver": DRIVERS["alonso"]},
    ]
    return _mrdata("StandingsTable", {"StandingsLists": [
        {"season": str(SEASON), "round": "2", "DriverStandings": standings}
    ]})


def constructor_standings_json():
    standings = [
        {"position": "1", "positionText": "1", "points": "87", "wins": "2", "Constructor": CONSTRUCTORS["red_bull"]},
        {"position": "2", "positionText": "2", "points": "15", "wins": "0", "Constructor": CONSTRUCTORS["aston_martin"]},
    ]
    return _mrdata("StandingsTable", {"StandingsLists": [
        {"season": str(SEASON), "round": "2", "ConstructorStandings": standings}
    ]})


class FakeErgastClient:
    """Drop-in replacement for ErgastClient that serves the fixtures above."""

    def __init__(self, *args, **kwargs):
        self.calls = []

    def get(self, path, params=None):
        self.calls.append(path)
        parts = path.strip("/").split("/")
        if parts == [str(SEASON)]:
            return races_json()
        if len(parts) == 3 and parts[2] == "results":
            return results_json(int(parts[1]))
        if len(parts) == 3 and parts[2] == "qualifying":
            return qualifying_json(int(parts[1]))
        if parts[-1] == "driverStandings":
            return driver_standings_json()
        if parts[-1] == "constructorStandings":
            return constructor_standings_json()
        raise AssertionError(f"Unexpected path requested: {path}")
