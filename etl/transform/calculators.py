"""
Metric calculation functions for F1 ETL pipeline.
"""
import logging
from typing import Tuple
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def compute_driver_metrics(
    results_df: pd.DataFrame,
    qualifying_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Calculate aggregated driver metrics per season.
    
    Metrics calculated:
        - races_entered: Total races participated
        - races_finished: Races with valid finishing position
        - podiums: Finishes in top 3
        - wins: First place finishes
        - poles: Pole positions from qualifying
        - dnf_count: Did not finish count
        - avg_finish_position: Average finishing position (only finished races)
        - avg_grid_position: Average starting position
        - avg_points_per_race: Average points per race
        - total_points: Sum of all points
        - position_changes_sum: Sum of (grid - finish) for all races
        - consistency_score: Standard deviation of finish positions
    
    Args:
        results_df: Cleaned results DataFrame
        qualifying_df: Cleaned qualifying DataFrame
        
    Returns:
        DataFrame with columns aligned to DriverMetrics model
    """
    if results_df.empty:
        logger.warning("Empty results DataFrame provided for metrics calculation")
        return pd.DataFrame()
    
    # Calculate poles from qualifying
    if not qualifying_df.empty:
        poles_df = qualifying_df[qualifying_df['position'] == 1].copy()
        poles_count = poles_df.groupby(['driver_id', 'season']).size().reset_index(name='poles')
    else:
        poles_count = pd.DataFrame(columns=['driver_id', 'season', 'poles'])
    
    # Group by driver and season for basic aggregations
    metrics = results_df.groupby(['driver_id', 'season']).agg({
        'round': 'count',  # races_entered
        'grid': 'mean',  # avg_grid_position
        'points': ['sum', 'mean'],  # total_points, avg_points_per_race
        'position_change': 'sum',  # position_changes_sum
    }).reset_index()
    
    # Flatten column names
    metrics.columns = [
        'driver_id', 'season', 'races_entered',
        'avg_grid_position', 'total_points',
        'avg_points_per_race', 'position_changes_sum'
    ]
    
    # Calculate races_finished (where position is not null)
    races_finished = results_df[results_df['position'].notna()].groupby(
        ['driver_id', 'season']
    ).size().reset_index(name='races_finished')
    metrics = metrics.merge(races_finished, on=['driver_id', 'season'], how='left')
    metrics['races_finished'] = metrics['races_finished'].fillna(0).astype(int)
    
    # Calculate avg_finish_position (only for finished races)
    avg_finish = results_df[results_df['position'].notna()].groupby(
        ['driver_id', 'season']
    )['position'].mean().reset_index(name='avg_finish_position')
    metrics = metrics.merge(avg_finish, on=['driver_id', 'season'], how='left')
    
    # Calculate podiums (position <= 3)
    podiums = results_df[results_df['position'] <= 3].groupby(
        ['driver_id', 'season']
    ).size().reset_index(name='podiums')
    metrics = metrics.merge(podiums, on=['driver_id', 'season'], how='left')
    metrics['podiums'] = metrics['podiums'].fillna(0).astype(int)
    
    # Calculate wins (position == 1)
    wins = results_df[results_df['position'] == 1].groupby(
        ['driver_id', 'season']
    ).size().reset_index(name='wins')
    metrics = metrics.merge(wins, on=['driver_id', 'season'], how='left')
    metrics['wins'] = metrics['wins'].fillna(0).astype(int)
    
    # Calculate DNF count (position is null)
    dnf = results_df[results_df['position'].isna()].groupby(
        ['driver_id', 'season']
    ).size().reset_index(name='dnf_count')
    metrics = metrics.merge(dnf, on=['driver_id', 'season'], how='left')
    metrics['dnf_count'] = metrics['dnf_count'].fillna(0).astype(int)
    
    # Add poles
    metrics = metrics.merge(poles_count, on=['driver_id', 'season'], how='left')
    metrics['poles'] = metrics['poles'].fillna(0).astype(int)
    
    # Calculate consistency score (std dev of finish positions)
    # Only for races where driver finished
    consistency = results_df[results_df['position'].notna()].groupby(
        ['driver_id', 'season']
    )['position'].std().reset_index(name='consistency_score')
    metrics = metrics.merge(consistency, on=['driver_id', 'season'], how='left')
    metrics['consistency_score'] = metrics['consistency_score'].fillna(0)
    
    # Clean up data types and round floats
    metrics['races_entered'] = metrics['races_entered'].astype(int)
    metrics['position_changes_sum'] = metrics['position_changes_sum'].fillna(0).astype(int)
    
    # Round float columns to 3 decimals
    float_columns = ['avg_finish_position', 'avg_grid_position', 
                     'avg_points_per_race', 'consistency_score', 'total_points']
    for col in float_columns:
        if col in metrics.columns:
            metrics[col] = metrics[col].round(3)
    
    logger.info(f"Calculated metrics for {len(metrics)} driver-season combinations")
    
    return metrics


def compute_constructor_metrics(results_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate aggregated constructor metrics per season.
    
    Metrics calculated:
        - races_entered: Total races participated (unique races)
        - podiums: Total podium finishes for the constructor
        - wins: Total wins
        - one_two_finishes: Races where both drivers finished 1-2
        - double_dnf: Races where both/all drivers DNF
        - avg_finish_position: Average finish position across all drivers
        - total_points: Sum of all points
        - reliability_rate: Percentage of entries that finished the race
    
    Args:
        results_df: Cleaned results DataFrame
        
    Returns:
        DataFrame with columns aligned to ConstructorMetrics model
    """
    if results_df.empty:
        logger.warning("Empty results DataFrame provided for metrics calculation")
        return pd.DataFrame()
    
    # Basic aggregations
    metrics = results_df.groupby(['constructor_id', 'season']).agg({
        'round': 'nunique',  # races_entered (unique races)
        'points': 'sum',  # total_points
    }).reset_index()
    
    metrics.columns = ['constructor_id', 'season', 'races_entered', 'total_points']
    
    # Calculate podiums (position <= 3)
    podiums = results_df[results_df['position'] <= 3].groupby(
        ['constructor_id', 'season']
    ).size().reset_index(name='podiums')
    metrics = metrics.merge(podiums, on=['constructor_id', 'season'], how='left')
    metrics['podiums'] = metrics['podiums'].fillna(0).astype(int)
    
    # Calculate wins (position == 1)
    wins = results_df[results_df['position'] == 1].groupby(
        ['constructor_id', 'season']
    ).size().reset_index(name='wins')
    metrics = metrics.merge(wins, on=['constructor_id', 'season'], how='left')
    metrics['wins'] = metrics['wins'].fillna(0).astype(int)
    
    # Calculate average finish position (only finished races)
    avg_pos = results_df[results_df['position'].notna()].groupby(
        ['constructor_id', 'season']
    )['position'].mean().reset_index(name='avg_finish_position')
    metrics = metrics.merge(avg_pos, on=['constructor_id', 'season'], how='left')
    
    # Calculate one-two finishes
    # For each race, check if constructor has drivers in positions 1 and 2
    one_two_list = []
    for (constructor, season), group in results_df.groupby(['constructor_id', 'season']):
        one_two_count = 0
        for round_num, race_group in group.groupby('round'):
            # Get positions for this constructor in this race, sorted
            positions = race_group['position'].dropna().sort_values().tolist()
            # Check if first two positions are 1 and 2
            if len(positions) >= 2 and positions[0] == 1 and positions[1] == 2:
                one_two_count += 1
        one_two_list.append({
            'constructor_id': constructor,
            'season': season,
            'one_two_finishes': one_two_count
        })
    
    one_two_df = pd.DataFrame(one_two_list)
    metrics = metrics.merge(one_two_df, on=['constructor_id', 'season'], how='left')
    metrics['one_two_finishes'] = metrics['one_two_finishes'].fillna(0).astype(int)
    
    # Calculate double DNF
    # For each race, check if all drivers from constructor DNF
    double_dnf_list = []
    for (constructor, season), group in results_df.groupby(['constructor_id', 'season']):
        double_dnf_count = 0
        for round_num, race_group in group.groupby('round'):
            # Check if all drivers DNF (position is null) and there are at least 2 drivers
            if race_group['position'].isna().all() and len(race_group) >= 2:
                double_dnf_count += 1
        double_dnf_list.append({
            'constructor_id': constructor,
            'season': season,
            'double_dnf': double_dnf_count
        })
    
    double_dnf_df = pd.DataFrame(double_dnf_list)
    metrics = metrics.merge(double_dnf_df, on=['constructor_id', 'season'], how='left')
    metrics['double_dnf'] = metrics['double_dnf'].fillna(0).astype(int)
    
    # Calculate reliability rate
    # Percentage of all entries that finished the race (position not null)
    reliability_list = []
    for (constructor, season), group in results_df.groupby(['constructor_id', 'season']):
        total_entries = len(group)
        finished_entries = group['position'].notna().sum()
        reliability_rate = (finished_entries / total_entries * 100) if total_entries > 0 else 0
        reliability_list.append({
            'constructor_id': constructor,
            'season': season,
            'reliability_rate': round(reliability_rate, 2)
        })
    
    reliability_df = pd.DataFrame(reliability_list)
    metrics = metrics.merge(reliability_df, on=['constructor_id', 'season'], how='left')
    
    # Ensure proper data types
    metrics['races_entered'] = metrics['races_entered'].astype(int)
    metrics['total_points'] = metrics['total_points'].round(3)
    
    # Round avg_finish_position
    if 'avg_finish_position' in metrics.columns:
        metrics['avg_finish_position'] = metrics['avg_finish_position'].round(3)
    
    logger.info(f"Calculated metrics for {len(metrics)} constructor-season combinations")
    
    return metrics