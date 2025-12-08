"""
Low-level bulk operations for Django models.

Here we will centralize:
- bulk_create
- bulk_update
- custom upsert patterns
"""

from typing import Iterable, Type

from django.db import models


def bulk_upsert(
    model: Type[models.Model],
    objects: Iterable[models.Model],
    unique_fields: list[str],
) -> None:
    """
    Stub for a future bulk upsert operation.

    For now it does nothing; later this will:
    - Compare existing records based on unique_fields
    - Insert new ones, update existing ones
    """
    # To be implemented in a later phase
    return