-- macros/generate_audio_feature_segments.sql
/*
Purpose: Macro to segment audio features into categorical ranges
This macro:
- Takes numerical audio features and converts to descriptive categories
- Creates standardized buckets for feature analysis
- Provides consistent labeling across all models
- Configurable thresholds for different segmentation strategies
*/