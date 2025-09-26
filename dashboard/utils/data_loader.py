import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from .database import DatabaseConnection

class DataLoader:
    """Handles data loading and caching for the Spotify dashboard"""
    
    def __init__(self):
        self.db = DatabaseConnection()
    
    @st.cache_data(ttl=300)  # Cache for 5 minutes
    def get_basic_stats(_self) -> Dict[str, int]:
        """Get basic statistics for the sidebar"""
        try:
            queries = {
                'artists': "SELECT COUNT(DISTINCT artist_id) FROM artists",
                'tracks': "SELECT COUNT(*) FROM tracks",
                'plays': "SELECT COUNT(*) FROM user_listening_history"
            }
            
            stats = {}
            for key, query in queries.items():
                result = _self.db.execute_query(query)
                stats[key] = int(result.iloc[0, 0]) if not result.empty else 0
            
            return stats
        except Exception as e:
            st.error(f"Error loading basic stats: {str(e)}")
            return {'artists': 0, 'tracks': 0, 'plays': 0}
    
    @st.cache_data(ttl=300)
    def get_overview_data(_self, days: Optional[int] = None) -> Dict[str, Any]:
        """Get overview data for the main dashboard"""
        try:
            # Build date filter
            date_filter = ""
            if days:
                date_filter = f"AND played_at >= CURRENT_DATE - INTERVAL '{days} days'"
            
            # Total listening hours - use track duration as fallback since ms_played is NULL
            hours_query = f"""
                SELECT COALESCE(SUM(t.duration_ms::float / 3600000), 0) as total_hours
                FROM user_listening_history h
                JOIN tracks t ON h.track_id = t.track_id
                WHERE 1=1 {date_filter}
            """
            
            # Unique tracks and artists
            unique_query = f"""
                SELECT
                    COUNT(DISTINCT h.track_id) as unique_tracks,
                    COUNT(DISTINCT ta.artist_id) as unique_artists
                FROM user_listening_history h
                LEFT JOIN track_artists ta ON h.track_id = ta.track_id AND ta.position = 0
                WHERE 1=1 {date_filter}
            """
            
            # Completion rate - calculate from actual data when available
            completion_query = f"""
                SELECT
                    CASE
                        WHEN COUNT(*) FILTER (WHERE h.ms_played IS NOT NULL AND t.duration_ms > 0) > 0
                        THEN AVG(
                            CASE
                                WHEN h.ms_played IS NOT NULL AND t.duration_ms > 0
                                THEN LEAST((h.ms_played::numeric / t.duration_ms::numeric) * 100, 100)
                                ELSE NULL
                            END
                        )
                        ELSE NULL
                    END as completion_rate
                FROM user_listening_history h
                JOIN tracks t ON h.track_id = t.track_id
                WHERE 1=1 {date_filter}
            """
            
            overview_data = {}

            # Execute queries
            hours_result = _self.db.execute_query(hours_query)
            overview_data['total_hours'] = float(hours_result.iloc[0, 0]) if not hours_result.empty and hours_result.iloc[0, 0] is not None else 0

            unique_result = _self.db.execute_query(unique_query)
            if not unique_result.empty:
                overview_data['unique_tracks'] = int(unique_result.iloc[0, 0]) if unique_result.iloc[0, 0] is not None else 0
                overview_data['unique_artists'] = int(unique_result.iloc[0, 1]) if unique_result.iloc[0, 1] is not None else 0
            else:
                overview_data['unique_tracks'] = 0
                overview_data['unique_artists'] = 0

            completion_result = _self.db.execute_query(completion_query)
            overview_data['completion_rate'] = float(completion_result.iloc[0, 0]) if not completion_result.empty and completion_result.iloc[0, 0] is not None else 0
            
            # Calculate period-over-period changes
            if days:
                # Get previous period data for comparison
                prev_date_filter = f"AND h.played_at >= CURRENT_DATE - INTERVAL '{days * 2} days' AND h.played_at < CURRENT_DATE - INTERVAL '{days} days'"

                # Previous period hours
                prev_hours_query = f"""
                    SELECT COALESCE(SUM(t.duration_ms::float / 3600000), 0) as total_hours
                    FROM user_listening_history h
                    JOIN tracks t ON h.track_id = t.track_id
                    WHERE 1=1 {prev_date_filter}
                """

                # Previous period unique tracks/artists
                prev_unique_query = f"""
                    SELECT
                        COUNT(DISTINCT h.track_id) as unique_tracks,
                        COUNT(DISTINCT ta.artist_id) as unique_artists
                    FROM user_listening_history h
                    LEFT JOIN track_artists ta ON h.track_id = ta.track_id AND ta.position = 0
                    WHERE 1=1 {prev_date_filter}
                """

                # Execute previous period queries
                prev_hours_result = _self.db.execute_query(prev_hours_query)
                prev_hours = float(prev_hours_result.iloc[0, 0]) if not prev_hours_result.empty and prev_hours_result.iloc[0, 0] is not None else 0

                prev_unique_result = _self.db.execute_query(prev_unique_query)
                if not prev_unique_result.empty:
                    prev_tracks = int(prev_unique_result.iloc[0, 0]) if prev_unique_result.iloc[0, 0] is not None else 0
                    prev_artists = int(prev_unique_result.iloc[0, 1]) if prev_unique_result.iloc[0, 1] is not None else 0
                else:
                    prev_tracks = 0
                    prev_artists = 0

                # Calculate changes
                overview_data.update({
                    'hours_change': overview_data['total_hours'] - prev_hours,
                    'tracks_change': overview_data['unique_tracks'] - prev_tracks,
                    'artists_change': overview_data['unique_artists'] - prev_artists,
                    'completion_change': 0  # Since completion rate is fixed at 80%
                })
            else:
                # For "All time", compare with previous equivalent period (if possible)
                overview_data.update({
                    'hours_change': 0,
                    'tracks_change': 0,
                    'artists_change': 0,
                    'completion_change': 0
                })
            
            return overview_data
            
        except Exception as e:
            st.error(f"Error loading overview data: {str(e)}")
            return {}
    
    @st.cache_data(ttl=300)
    def get_daily_activity(_self, days: Optional[int] = None) -> pd.DataFrame:
        """Get daily listening activity"""
        try:
            date_filter = ""
            if days:
                date_filter = f"AND played_at >= CURRENT_DATE - INTERVAL '{days} days'"

            # Use track duration as fallback since ms_played is NULL
            query = f"""
                SELECT
                    DATE(h.played_at) as listening_date,
                    SUM(t.duration_ms::float / 3600000) as total_hours_played,
                    COUNT(DISTINCT h.track_id) as unique_tracks,
                    COUNT(*) as total_plays
                FROM user_listening_history h
                JOIN tracks t ON h.track_id = t.track_id
                WHERE h.played_at IS NOT NULL
                {date_filter}
                GROUP BY DATE(h.played_at)
                ORDER BY listening_date DESC
                LIMIT 30
            """

            return _self.db.execute_query(query)

        except Exception as e:
            st.error(f"Error loading daily activity: {str(e)}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def get_top_genres(_self, limit: int = 10) -> pd.DataFrame:
        """Get top genres by affinity"""
        try:
            # Use JSONB functions to parse genres properly
            query = f"""
                SELECT
                    jsonb_array_elements_text(a.genres) as genre,
                    COUNT(*) as track_count,
                    AVG(a.popularity) as affinity_score,
                    ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as preference_rank
                FROM artists a
                JOIN track_artists ta ON a.artist_id = ta.artist_id
                JOIN user_listening_history h ON ta.track_id = h.track_id
                WHERE a.genres IS NOT NULL AND jsonb_array_length(a.genres) > 0
                GROUP BY genre
                ORDER BY track_count DESC
                LIMIT {limit}
            """

            return _self.db.execute_query(query)

        except Exception as e:
            st.error(f"Error loading top genres: {str(e)}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def get_recent_tracks(_self, limit: int = 10) -> pd.DataFrame:
        """Get recent tracks played"""
        try:
            query = f"""
                SELECT
                    h.played_at,
                    t.name as track_name,
                    a.name as artist_name,
                    CASE
                        WHEN h.ms_played IS NOT NULL AND t.duration_ms > 0
                        THEN LEAST(ROUND((h.ms_played::numeric / t.duration_ms::numeric) * 100, 1), 100)
                        ELSE NULL
                    END as completion_percentage
                FROM user_listening_history h
                JOIN tracks t ON h.track_id = t.track_id
                LEFT JOIN track_artists ta ON t.track_id = ta.track_id AND ta.position = 0
                LEFT JOIN artists a ON ta.artist_id = a.artist_id
                ORDER BY h.played_at DESC
                LIMIT {limit}
            """

            return _self.db.execute_query(query)

        except Exception as e:
            st.error(f"Error loading recent tracks: {str(e)}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def get_top_artists(_self, days: Optional[int] = None, limit: int = 15) -> pd.DataFrame:
        """Get top artists by affinity"""
        try:
            # Use actual listening history for top artists
            query = f"""
                SELECT
                    a.name as entity_name,
                    COUNT(h.id) as affinity_score,
                    a.popularity as spotify_popularity,
                    a.followers,
                    ROW_NUMBER() OVER (ORDER BY COUNT(h.id) DESC) as preference_rank
                FROM artists a
                JOIN track_artists ta ON a.artist_id = ta.artist_id
                JOIN user_listening_history h ON ta.track_id = h.track_id
                GROUP BY a.artist_id, a.name, a.popularity, a.followers
                ORDER BY COUNT(h.id) DESC
                LIMIT {limit}
            """

            return _self.db.execute_query(query)

        except Exception as e:
            st.error(f"Error loading top artists: {str(e)}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def get_genre_preferences(_self, limit: int = 10) -> pd.DataFrame:
        """Get genre preferences with affinity scores"""
        try:
            # Use JSONB and actual listening history
            query = f"""
                SELECT
                    jsonb_array_elements_text(a.genres) as genre,
                    COUNT(h.id) as affinity_score,
                    CASE
                        WHEN COUNT(h.id) >= 20 THEN 'High Affinity'
                        WHEN COUNT(h.id) >= 10 THEN 'Medium Affinity'
                        ELSE 'Low Affinity'
                    END as affinity_category
                FROM artists a
                JOIN track_artists ta ON a.artist_id = ta.artist_id
                JOIN user_listening_history h ON ta.track_id = h.track_id
                WHERE a.genres IS NOT NULL AND jsonb_array_length(a.genres) > 0
                GROUP BY genre
                ORDER BY affinity_score DESC
                LIMIT {limit}
            """

            return _self.db.execute_query(query)

        except Exception as e:
            st.error(f"Error loading genre preferences: {str(e)}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def get_artist_discovery_data(_self) -> pd.DataFrame:
        """Get artist discovery analysis data"""
        try:
            # Use actual listening history and play counts as affinity score
            query = """
                SELECT
                    a.name as artist_name,
                    COUNT(h.id) as affinity_score,
                    a.popularity as spotify_popularity,
                    a.followers,
                    CASE
                        WHEN a.popularity >= 80 THEN 'Mainstream'
                        WHEN a.popularity >= 60 THEN 'Popular'
                        WHEN a.popularity >= 40 THEN 'Emerging'
                        WHEN a.popularity >= 20 THEN 'Underground'
                        ELSE 'Niche'
                    END as popularity_category
                FROM artists a
                JOIN track_artists ta ON a.artist_id = ta.artist_id
                JOIN user_listening_history h ON ta.track_id = h.track_id
                WHERE a.popularity IS NOT NULL
                GROUP BY a.artist_id, a.name, a.popularity, a.followers
                ORDER BY COUNT(h.id) DESC
                LIMIT 50
            """

            return _self.db.execute_query(query)

        except Exception as e:
            st.error(f"Error loading artist discovery data: {str(e)}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def get_hourly_patterns(_self, days: Optional[int] = None) -> pd.DataFrame:
        """Get hourly listening patterns"""
        try:
            date_filter = ""
            if days:
                date_filter = f"AND h.played_at >= CURRENT_DATE - INTERVAL '{days} days'"

            query = f"""
                SELECT
                    CAST(EXTRACT(hour FROM h.played_at) AS INTEGER) as hour,
                    CAST(EXTRACT(dow FROM h.played_at) AS INTEGER) as day_of_week,
                    COUNT(*) as play_count
                FROM user_listening_history h
                WHERE h.played_at IS NOT NULL {date_filter}
                GROUP BY EXTRACT(hour FROM h.played_at), EXTRACT(dow FROM h.played_at)
                ORDER BY day_of_week, hour
            """

            result = _self.db.execute_query(query)
            return result

        except Exception as e:
            st.error(f"Error loading hourly patterns: {str(e)}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def get_weekly_patterns(_self, days: Optional[int] = None) -> pd.DataFrame:
        """Get weekly listening patterns"""
        try:
            date_filter = ""
            if days:
                date_filter = f"WHERE h.played_at >= CURRENT_DATE - INTERVAL '{days} days'"
            
            query = f"""
                SELECT
                    CASE EXTRACT(dow FROM h.played_at)
                        WHEN 0 THEN 'Sunday'
                        WHEN 1 THEN 'Monday'
                        WHEN 2 THEN 'Tuesday'
                        WHEN 3 THEN 'Wednesday'
                        WHEN 4 THEN 'Thursday'
                        WHEN 5 THEN 'Friday'
                        WHEN 6 THEN 'Saturday'
                    END as day_name,
                    EXTRACT(dow FROM h.played_at) as day_order,
                    COUNT(*) as play_count,
                    AVG(
                        CASE
                            WHEN h.ms_played IS NOT NULL THEN h.ms_played::float / 1000 / 60
                            ELSE t.duration_ms::float / 1000 / 60
                        END
                    ) as avg_minutes
                FROM user_listening_history h
                LEFT JOIN tracks t ON h.track_id = t.track_id
                {date_filter}
                GROUP BY EXTRACT(dow FROM h.played_at)
                ORDER BY day_order
            """
            
            return _self.db.execute_query(query)
            
        except Exception as e:
            st.error(f"Error loading weekly patterns: {str(e)}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def get_listening_behavior(_self, days: Optional[int] = None) -> Dict[str, float]:
        """Get listening behavior metrics"""
        try:
            date_filter = ""
            if days:
                date_filter = f"AND h.played_at >= CURRENT_DATE - INTERVAL '{days} days'"
            
            query = f"""
                SELECT
                    AVG(t.duration_ms::float / 60000) as avg_track_length_min,
                    CASE
                        WHEN COUNT(*) FILTER (WHERE h.ms_played IS NOT NULL) > 0
                        THEN (COUNT(*) FILTER (WHERE h.ms_played < t.duration_ms * 0.3)::float /
                              COUNT(*) FILTER (WHERE h.ms_played IS NOT NULL)::float) * 100
                        ELSE NULL
                    END as skip_rate,
                    COUNT(DISTINCT h.track_id)::float / COUNT(*)::float * 100 as discovery_rate
                FROM user_listening_history h
                JOIN tracks t ON h.track_id = t.track_id
                WHERE h.played_at IS NOT NULL {date_filter}
            """
            
            result = _self.db.execute_query(query)
            if not result.empty:
                return {
                    'avg_session_length': float(result.iloc[0, 0]) if result.iloc[0, 0] is not None else 0,
                    'skip_rate': float(result.iloc[0, 1]) if result.iloc[0, 1] is not None else 0,
                    'discovery_rate': float(result.iloc[0, 2]) if result.iloc[0, 2] is not None else 0
                }
            return {
                'avg_session_length': 0,
                'skip_rate': 0,
                'discovery_rate': 0
            }
            
        except Exception as e:
            st.error(f"Error loading listening behavior: {str(e)}")
            return {}
    
    @st.cache_data(ttl=300)
    def get_playlist_diversity(_self) -> pd.DataFrame:
        """Get playlist diversity metrics"""
        try:
            # Use direct query instead of broken reporting model
            query = """
                SELECT
                    p.name as playlist_name,
                    COUNT(DISTINCT pt.track_id) as track_count,
                    COUNT(DISTINCT ta.artist_id) as unique_artist_count,
                    ROUND((COUNT(DISTINCT ta.artist_id)::numeric / NULLIF(COUNT(DISTINCT pt.track_id), 0)) * 100, 2) as overall_diversity_score,
                    SUM(t.duration_ms::float / 60000) as total_duration_minutes,
                    AVG(t.popularity) as avg_track_popularity
                FROM playlists p
                LEFT JOIN playlist_tracks pt ON p.playlist_id = pt.playlist_id
                LEFT JOIN tracks t ON pt.track_id = t.track_id
                LEFT JOIN track_artists ta ON t.track_id = ta.track_id
                GROUP BY p.playlist_id, p.name
                HAVING COUNT(DISTINCT pt.track_id) > 0
                ORDER BY overall_diversity_score DESC
            """

            return _self.db.execute_query(query)

        except Exception as e:
            st.error(f"Error loading playlist diversity: {str(e)}")
            return pd.DataFrame()