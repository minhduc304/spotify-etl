import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
from utils.database import DatabaseConnection
from utils.data_loader import DataLoader
from utils.charts import ChartGenerator

# Page configuration
st.set_page_config(
    page_title="🎵 Spotify Analytics Dashboard",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p {
        font-size: 1.1rem;
        font-weight: 500;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'data_loader' not in st.session_state:
    st.session_state.data_loader = DataLoader()
    st.session_state.chart_gen = ChartGenerator()

def main():
    # Sidebar
    st.sidebar.title("🎵 Spotify Analytics")
    st.sidebar.markdown("---")
    
    # Time range selector
    time_ranges = {
        "Last 7 days": 7,
        "Last 30 days": 30,
        "Last 90 days": 90,
        "All time": None
    }
    selected_range = st.sidebar.selectbox("📅 Time Range", list(time_ranges.keys()), index=1)
    days = time_ranges[selected_range]
    
    # Refresh button
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Data Quality**")
    
    try:
        # Load basic stats for sidebar
        basic_stats = st.session_state.data_loader.get_basic_stats()
        st.sidebar.metric("Total Artists", f"{basic_stats['artists']:,}")
        st.sidebar.metric("Total Tracks", f"{basic_stats['tracks']:,}")
        st.sidebar.metric("Total Plays", f"{basic_stats['plays']:,}")
        
        # Main content
        st.title("🎵 Spotify Analytics Dashboard")
        st.markdown("### Your Personal Music Insights")
        
        # Create tabs
        tab1, tab2, tab3, tab4 = st.tabs(["🏠 Overview", "🎨 Artists & Genres", "📊 Listening Patterns", "🎵 Playlists"])
        
        with tab1:
            show_overview(days)
        
        with tab2:
            show_artists_genres(days)
        
        with tab3:
            show_listening_patterns(days)
            
        with tab4:
            show_playlists()
            
    except Exception as e:
        st.error(f"❌ Error loading data: {str(e)}")
        st.info("💡 Make sure your PostgreSQL database is running and accessible.")
        
        # Show connection troubleshooting
        with st.expander("🔧 Connection Troubleshooting"):
            st.markdown("""
            1. **Check Database Connection:**
               ```bash
               docker ps | grep postgres
               ```
            
            2. **Verify Environment Variables:**
               - SPOTIFY_DB_HOST
               - SPOTIFY_DB_PORT 
               - SPOTIFY_DB_USER
               - SPOTIFY_DB_PASSWORD
               - SPOTIFY_DB
            
            3. **Test Connection:**
               ```bash
               psql -h localhost -p 5432 -U analyst -d spotify
               ```
            """)

def show_overview(days):
    """Overview tab content"""
    st.header("📈 Overview")
    
    # Load overview data
    overview_data = st.session_state.data_loader.get_overview_data(days)
    
    # Key metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "🎵 Total Listening Hours", 
            f"{overview_data.get('total_hours', 0):.1f}",
            delta=f"{overview_data.get('hours_change', 0):+.1f} vs prev period"
        )
    
    with col2:
        st.metric(
            "🎶 Unique Tracks", 
            f"{overview_data.get('unique_tracks', 0):,}",
            delta=f"{overview_data.get('tracks_change', 0):+,} new"
        )
    
    with col3:
        st.metric(
            "🎤 Unique Artists", 
            f"{overview_data.get('unique_artists', 0):,}",
            delta=f"{overview_data.get('artists_change', 0):+,} new"
        )
    
    with col4:
        completion_rate = overview_data.get('completion_rate')
        if completion_rate is not None:
            st.metric(
                "✅ Completion Rate",
                f"{completion_rate:.1f}%",
                delta=f"{overview_data.get('completion_change', 0):+.1f}%"
            )
        else:
            st.metric(
                "✅ Completion Rate",
                "N/A",
                help="Completion data requires extended streaming history"
            )
    
    # Charts row
    col1, col2 = st.columns(2)
    
    with col1:
        # Daily listening activity
        activity_data = st.session_state.data_loader.get_daily_activity(days)
        if not activity_data.empty:
            fig = st.session_state.chart_gen.create_daily_activity_chart(activity_data)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Top genres pie chart
        genre_data = st.session_state.data_loader.get_top_genres(limit=8)
        if not genre_data.empty:
            fig = st.session_state.chart_gen.create_genre_pie_chart(genre_data)
            st.plotly_chart(fig, use_container_width=True)
    
    # Recent activity
    st.subheader("🕐 Recent Activity")
    recent_activity = st.session_state.data_loader.get_recent_tracks(limit=10)
    if not recent_activity.empty:
        # Check if we have completion data
        if recent_activity['completion_percentage'].notna().any():
            # Show with completion percentage if we have data
            st.dataframe(
                recent_activity[['played_at', 'track_name', 'artist_name', 'completion_percentage']],
                use_container_width=True
            )
        else:
            # Show without completion percentage if no data
            st.dataframe(
                recent_activity[['played_at', 'track_name', 'artist_name']],
                use_container_width=True
            )
            st.info("💡 Completion data not available. To get detailed listening metrics, you can request your extended streaming history from Spotify.")

def show_artists_genres(days):
    """Artists & Genres tab content"""
    st.header("🎨 Artists & Genres Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🌟 Top Artists")
        
        # Top artists by affinity
        top_artists = st.session_state.data_loader.get_top_artists(days, limit=15)
        if not top_artists.empty:
            fig = st.session_state.chart_gen.create_artist_affinity_chart(top_artists)
            st.plotly_chart(fig, use_container_width=True)
            
            # Artist details table
            st.dataframe(
                top_artists[['entity_name', 'affinity_score', 'spotify_popularity', 'followers']],
                use_container_width=True
            )
    
    with col2:
        st.subheader("🎵 Genre Preferences")
        
        # Genre affinity
        genre_data = st.session_state.data_loader.get_genre_preferences(limit=10)
        if not genre_data.empty:
            fig = st.session_state.chart_gen.create_genre_bar_chart(genre_data)
            st.plotly_chart(fig, use_container_width=True)
    
    # Artist popularity vs affinity scatter plot
    st.subheader("📈 Artist Discovery Analysis")
    artist_scatter_data = st.session_state.data_loader.get_artist_discovery_data()
    if not artist_scatter_data.empty:
        fig = st.session_state.chart_gen.create_artist_discovery_scatter(artist_scatter_data)
        st.plotly_chart(fig, use_container_width=True)

def show_listening_patterns(days):
    """Listening Patterns tab content"""
    st.header("📊 Listening Patterns")
    
    # Time-based analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("⏰ Listening by Hour")
        hourly_data = st.session_state.data_loader.get_hourly_patterns(days)
        if not hourly_data.empty:
            fig = st.session_state.chart_gen.create_hourly_heatmap(hourly_data)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No hourly listening data available for the selected time range.")
    
    with col2:
        st.subheader("📅 Weekly Patterns")
        weekly_data = st.session_state.data_loader.get_weekly_patterns(days)
        if not weekly_data.empty:
            fig = st.session_state.chart_gen.create_weekly_pattern_chart(weekly_data)
            st.plotly_chart(fig, use_container_width=True)
    
    # Listening behavior metrics
    st.subheader("🎧 Listening Behavior")
    behavior_data = st.session_state.data_loader.get_listening_behavior(days)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if 'avg_session_length' in behavior_data:
            st.metric("⏱️ Avg Session Length", f"{behavior_data['avg_session_length']:.1f} min")
    
    with col2:
        if 'skip_rate' in behavior_data and behavior_data['skip_rate'] is not None:
            st.metric("⏭️ Skip Rate", f"{behavior_data['skip_rate']:.1f}%")
        else:
            st.metric("⏭️ Skip Rate", "N/A", help="Requires extended streaming history")
    
    with col3:
        if 'discovery_rate' in behavior_data:
            st.metric("🔍 Discovery Rate", f"{behavior_data['discovery_rate']:.1f}%")

def show_playlists():
    """Playlists tab content"""
    st.header("🎵 Playlist Analysis")
    
    # Playlist diversity metrics
    playlist_data = st.session_state.data_loader.get_playlist_diversity()
    
    if not playlist_data.empty:
        # Playlist diversity chart
        fig = st.session_state.chart_gen.create_playlist_diversity_chart(playlist_data)
        st.plotly_chart(fig, use_container_width=True)
        
        # Detailed playlist table
        st.subheader("📋 Playlist Details")
        
        # Add selection for sorting
        sort_options = {
            "Most Diverse": "overall_diversity_score",
            "Longest": "total_duration_minutes", 
            "Most Artists": "unique_artist_count",
            "Most Popular": "avg_track_popularity"
        }
        
        sort_by = st.selectbox("Sort by:", list(sort_options.keys()))
        sort_column = sort_options[sort_by]
        
        sorted_playlists = playlist_data.sort_values(sort_column, ascending=False)
        
        st.dataframe(
            sorted_playlists[[
                'playlist_name', 'track_count', 'unique_artist_count', 
                'overall_diversity_score', 'total_duration_minutes'
            ]].head(20),
            use_container_width=True
        )
    else:
        st.info("No playlist data available. Make sure you've run the ETL process to collect playlist information.")

if __name__ == "__main__":
    main()