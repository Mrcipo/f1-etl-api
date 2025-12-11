"""
Bulk operations helpers for efficient database operations.
"""
import logging
from typing import List, Any, Dict
from django.db import models, transaction

logger = logging.getLogger(__name__)


def safe_bulk_create(
    model: models.Model,
    objects: List[models.Model],
    batch_size: int = 500
) -> int:
    """
    Safely create multiple model instances in bulk with batching.
    
    This function splits the objects into batches and creates them
    efficiently using Django's bulk_create. Includes error handling
    and logging.
    
    Args:
        model: Django model class
        objects: List of model instances to create
        batch_size: Number of objects to create per batch (default: 500)
        
    Returns:
        Number of objects successfully created
        
    Example:
        >>> drivers = [Driver(driver_id='ham', ...), Driver(driver_id='ver', ...)]
        >>> count = safe_bulk_create(Driver, drivers)
        >>> print(f"Created {count} drivers")
    """
    if not objects:
        logger.warning(f"No objects provided for bulk_create on {model.__name__}")
        return 0
    
    total_created = 0
    model_name = model.__name__
    
    try:
        # Split into batches
        for i in range(0, len(objects), batch_size):
            batch = objects[i:i + batch_size]
            
            with transaction.atomic():
                created = model.objects.bulk_create(batch, batch_size=batch_size)
                batch_count = len(created)
                total_created += batch_count
                
                logger.debug(
                    f"Created batch of {batch_count} {model_name} objects "
                    f"(batch {i//batch_size + 1})"
                )
        
        logger.info(f"Successfully bulk created {total_created} {model_name} objects")
        return total_created
        
    except Exception as e:
        logger.error(
            f"Error during bulk_create for {model_name}: {e}",
            exc_info=True
        )
        raise


def safe_bulk_update(
    model: models.Model,
    objects: List[models.Model],
    fields: List[str],
    batch_size: int = 500
) -> int:
    """
    Safely update multiple model instances in bulk with batching.
    
    Updates only the specified fields for each object. Uses Django's
    bulk_update for efficiency.
    
    Args:
        model: Django model class
        objects: List of model instances to update (must have PKs set)
        fields: List of field names to update
        batch_size: Number of objects to update per batch (default: 500)
        
    Returns:
        Number of objects successfully updated
        
    Example:
        >>> drivers = Driver.objects.filter(nationality='British')
        >>> for driver in drivers:
        >>>     driver.updated_field = 'new_value'
        >>> count = safe_bulk_update(Driver, list(drivers), ['updated_field'])
    """
    if not objects:
        logger.warning(f"No objects provided for bulk_update on {model.__name__}")
        return 0
    
    if not fields:
        logger.warning("No fields provided for bulk_update")
        return 0
    
    total_updated = 0
    model_name = model.__name__
    
    try:
        # Split into batches
        for i in range(0, len(objects), batch_size):
            batch = objects[i:i + batch_size]
            
            with transaction.atomic():
                model.objects.bulk_update(batch, fields, batch_size=batch_size)
                batch_count = len(batch)
                total_updated += batch_count
                
                logger.debug(
                    f"Updated batch of {batch_count} {model_name} objects "
                    f"(batch {i//batch_size + 1})"
                )
        
        logger.info(
            f"Successfully bulk updated {total_updated} {model_name} objects "
            f"(fields: {', '.join(fields)})"
        )
        return total_updated
        
    except Exception as e:
        logger.error(
            f"Error during bulk_update for {model_name}: {e}",
            exc_info=True
        )
        raise


def safe_bulk_delete(queryset, model_name: str = None) -> int:
    """
    Safely delete objects from a queryset with logging.
    
    Args:
        queryset: Django queryset to delete
        model_name: Optional model name for logging (extracted if not provided)
        
    Returns:
        Number of objects deleted
    """
    if model_name is None:
        model_name = queryset.model.__name__
    
    try:
        with transaction.atomic():
            count, _ = queryset.delete()
            
        logger.info(f"Deleted {count} {model_name} objects")
        return count
        
    except Exception as e:
        logger.error(
            f"Error during delete for {model_name}: {e}",
            exc_info=True
        )
        raise