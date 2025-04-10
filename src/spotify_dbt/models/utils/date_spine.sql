-- utils/date_spine.sql
/*
Purpose: Generate a complete date dimension table
This model:
- Creates a continuous series of dates for time-based analysis
- Adds calendar attributes (day of week, month, quarter, etc)
- Handles fiscal periods if needed
- Marks holidays and special events
- Provides consistent date handling across all models
*/