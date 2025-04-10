-- macros/calculate_affinity_scores.sql
/*
Purpose: Standardized approach to calculating user affinities
This macro:
- Implements a weighted scoring algorithm for user preferences
- Normalizes scores across different dimensions
- Handles frequency decay for historical items
- Provides consistent methodology across models
*/