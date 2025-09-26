import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np

class ChartGenerator:
    """Generates charts for the Spotify dashboard"""
    
    def __init__(self):
        # Color palette for consistent styling
        self.colors = {
            'primary': '#1DB954',      # Spotify Green
            'secondary': '#191414',    # Spotify Black
            'accent': '#1ed760',       # Light Green
            'background': '#f0f2f6',   # Light Gray
            'text': '#000000',         # Pure black for better contrast
            'text_secondary': '#333333'
        }

        # Default layout settings
        self.default_layout = {
            'plot_bgcolor': 'white',
            'paper_bgcolor': 'white',
            'font': {'family': "Arial, sans-serif", 'size': 14, 'color': self.colors['text']},
            'title_font': {'size': 18, 'color': self.colors['text'], 'family': "Arial Black"},
            'margin': {'l': 60, 'r': 30, 't': 60, 'b': 60}
        }

        # Axis styling that can be applied individually
        self.axis_style = {
            'tickfont': {'size': 12, 'color': self.colors['text']},
            'title_font': {'size': 14, 'color': self.colors['text'], 'family': "Arial Black"}
        }
    
    def create_daily_activity_chart(self, data: pd.DataFrame) -> go.Figure:
        """Create daily listening activity line chart"""
        if data.empty:
            return self._create_empty_chart("No daily activity data available")
        
        fig = go.Figure()
        
        # Add listening hours line
        fig.add_trace(go.Scatter(
            x=data['listening_date'],
            y=data['total_hours_played'],
            mode='lines+markers',
            name='Listening Hours',
            line=dict(color=self.colors['primary'], width=3),
            marker=dict(size=6),
            hovertemplate='<b>%{x}</b><br>Hours: %{y:.1f}<extra></extra>'
        ))
        
        # Add unique tracks as secondary y-axis
        fig.add_trace(go.Scatter(
            x=data['listening_date'],
            y=data['unique_tracks'],
            mode='lines',
            name='Unique Tracks',
            line=dict(color=self.colors['accent'], width=2, dash='dash'),
            yaxis='y2',
            hovertemplate='<b>%{x}</b><br>Tracks: %{y}<extra></extra>'
        ))
        
        fig.update_layout(
            title='📈 Daily Listening Activity',
            xaxis=dict(title='Date', **self.axis_style),
            yaxis=dict(title='Hours', **self.axis_style),
            yaxis2=dict(
                title='Unique Tracks',
                overlaying='y',
                side='right',
                showgrid=False,
                **self.axis_style
            ),
            hovermode='x unified',
            **self.default_layout
        )
        
        return fig
    
    def create_genre_pie_chart(self, data: pd.DataFrame) -> go.Figure:
        """Create genre distribution pie chart"""
        if data.empty:
            return self._create_empty_chart("No genre data available")
        
        # Use a nice color palette
        colors = px.colors.qualitative.Set3
        
        fig = go.Figure(data=[go.Pie(
            labels=data['genre'],
            values=data['affinity_score'],
            hole=0.4,  # Donut chart
            marker_colors=colors[:len(data)],
            textinfo='label+percent',
            textposition='outside',
            textfont={'size': 12, 'color': self.colors['text'], 'family': 'Arial Black'},
            hovertemplate='<b>%{label}</b><br>Affinity: %{value:.1f}<br>%{percent}<extra></extra>'
        )])
        
        fig.update_layout(
            title='🎵 Top Genres by Affinity',
            showlegend=False,
            **self.default_layout
        )
        
        return fig
    
    def create_artist_affinity_chart(self, data: pd.DataFrame) -> go.Figure:
        """Create horizontal bar chart for top artists"""
        if data.empty:
            return self._create_empty_chart("No artist data available")
        
        # Sort by affinity score and take top 10
        data_sorted = data.head(10).sort_values('affinity_score')
        
        fig = go.Figure(go.Bar(
            x=data_sorted['affinity_score'],
            y=data_sorted['entity_name'],
            orientation='h',
            marker_color=self.colors['primary'],
            text=data_sorted['affinity_score'].round(1),
            textposition='outside',
            textfont={'size': 12, 'color': self.colors['text'], 'family': 'Arial Black'},
            hovertemplate='<b>%{y}</b><br>Affinity Score: %{x:.1f}<br>Popularity: %{customdata[0]}<extra></extra>',
            customdata=data_sorted[['spotify_popularity']].values
        ))
        
        fig.update_layout(
            title='🌟 Top Artists by Affinity Score',
            xaxis=dict(title='Affinity Score', **self.axis_style),
            yaxis=dict(title='Artist', categoryorder='total ascending', **self.axis_style),
            height=400,
            **self.default_layout
        )
        
        return fig
    
    def create_genre_bar_chart(self, data: pd.DataFrame) -> go.Figure:
        """Create vertical bar chart for genre preferences"""
        if data.empty:
            return self._create_empty_chart("No genre preference data available")
        
        # Color by affinity category
        color_map = {
            'favorite': self.colors['primary'],
            'highly_preferred': self.colors['accent'],
            'preferred': '#90EE90',
            'occasional': '#FFD700',
            'rare': '#FFA500'
        }
        
        colors = [color_map.get(cat, self.colors['primary']) for cat in data.get('affinity_category', [])]
        
        fig = go.Figure(go.Bar(
            x=data['genre'],
            y=data['affinity_score'],
            marker_color=colors,
            text=data['affinity_score'].round(1),
            textposition='outside',
            textfont={'size': 12, 'color': self.colors['text'], 'family': 'Arial Black'},
            hovertemplate='<b>%{x}</b><br>Affinity: %{y:.1f}<br>Category: %{customdata}<extra></extra>',
            customdata=data.get('affinity_category', [''] * len(data))
        ))
        
        fig.update_layout(
            title='🎵 Genre Preferences',
            xaxis=dict(title='Genre', tickangle=45, **self.axis_style),
            yaxis=dict(title='Affinity Score', **self.axis_style),
            height=400,
            **self.default_layout
        )
        
        return fig
    
    def create_artist_discovery_scatter(self, data: pd.DataFrame) -> go.Figure:
        """Create scatter plot for artist discovery analysis"""
        if data.empty:
            return self._create_empty_chart("No artist discovery data available")

        fig = px.scatter(
            data,
            x='spotify_popularity',
            y='affinity_score',
            size='followers',
            color='popularity_category',
            hover_name='artist_name',
            hover_data={'followers': ':,', 'spotify_popularity': True, 'affinity_score': ':.1f'},
            title='📈 Artist Discovery: Your Taste vs. Mainstream Popularity',
            labels={
                'spotify_popularity': 'Spotify Popularity (0-100)',
                'affinity_score': 'Your Affinity Score',
                'popularity_category': 'Category'
            },
            color_discrete_map={
                'Mainstream': '#E63946',     # Darker red for better contrast
                'Popular': '#2A9D8F',        # Darker teal
                'Emerging': '#264653',       # Dark blue-gray
                'Underground': '#E9C46A',    # Golden yellow
                'Niche': '#F4A261'          # Orange
            }
        )

        # Update axes with better contrast
        fig.update_xaxes(
            title_font={'size': 16, 'color': '#000000', 'family': 'Arial Black'},
            tickfont={'size': 14, 'color': '#000000'},
            showgrid=True,
            gridcolor='#E5E5E5',
            linecolor='#000000',
            linewidth=2
        )

        fig.update_yaxes(
            title_font={'size': 16, 'color': '#000000', 'family': 'Arial Black'},
            tickfont={'size': 14, 'color': '#000000'},
            showgrid=True,
            gridcolor='#E5E5E5',
            linecolor='#000000',
            linewidth=2
        )

        # Update legend with better contrast
        # Create a modified layout without conflicting title_font
        custom_layout = self.default_layout.copy()
        custom_layout['title_font'] = {'size': 20, 'color': '#000000', 'family': 'Arial Black'}

        fig.update_layout(
            height=500,
            legend=dict(
                title=dict(
                    text='<b>Category</b>',
                    font={'size': 16, 'color': '#000000', 'family': 'Arial Black'}
                ),
                font={'size': 14, 'color': '#000000', 'family': 'Arial'},
                bgcolor='rgba(255, 255, 255, 0.95)',
                bordercolor='#000000',
                borderwidth=2
            ),
            **custom_layout
        )

        # Update marker styling for better visibility
        fig.update_traces(
            marker=dict(
                line=dict(width=1.5, color='#000000'),
                opacity=0.85
            )
        )

        return fig
    
    def create_hourly_heatmap(self, data: pd.DataFrame) -> go.Figure:
        """Create heatmap for hourly listening patterns"""
        if data.empty:
            return self._create_empty_chart("No hourly pattern data available")

        try:

            # Ensure hour and day_of_week are integers
            data['hour'] = data['hour'].astype(int)
            data['day_of_week'] = data['day_of_week'].astype(int)

            # Create a complete grid of all hours and days
            all_hours = list(range(24))
            all_days = list(range(7))

            # Create pivot table with all combinations
            pivot_data = data.pivot_table(
                index='day_of_week',
                columns='hour',
                values='play_count',
                aggfunc='sum',  # In case of duplicates
                fill_value=0
            )

            # Reindex to ensure all hours and days are present
            pivot_data = pivot_data.reindex(index=all_days, columns=all_hours, fill_value=0)

            # Day names mapping
            day_names = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']

            # Create heatmap
            fig = go.Figure(data=go.Heatmap(
                z=pivot_data.values,
                x=[f"{h:02d}:00" for h in all_hours],  # Format hours nicely
                y=day_names,
                colorscale='Viridis',
                hovertemplate='<b>%{y}</b><br>Time: %{x}<br>Plays: %{z}<extra></extra>',
                colorbar=dict(
                    title="Play Count",
                    title_font={'size': 14, 'color': self.colors['text'], 'family': 'Arial Black'},
                    tickfont={'size': 12, 'color': self.colors['text']}
                )
            ))

            fig.update_layout(
                title='⏰ Listening Patterns by Hour & Day',
                xaxis=dict(
                    title='Hour of Day',
                    tickmode='linear',
                    dtick=2,  # Show every 2 hours
                    **self.axis_style
                ),
                yaxis=dict(title='Day of Week', **self.axis_style),
                height=400,  # Increased height for better visibility
                **self.default_layout
            )

            return fig

        except Exception as e:
            return self._create_empty_chart("Unable to create hourly heatmap")
    
    def create_weekly_pattern_chart(self, data: pd.DataFrame) -> go.Figure:
        """Create weekly pattern bar chart"""
        if data.empty:
            return self._create_empty_chart("No weekly pattern data available")
        
        fig = go.Figure()
        
        # Add play count bars
        fig.add_trace(go.Bar(
            x=data['day_name'],
            y=data['play_count'],
            name='Play Count',
            marker_color=self.colors['primary'],
            text=data['play_count'],
            textposition='outside',
            textfont={'size': 12, 'color': self.colors['text'], 'family': 'Arial Black'},
            hovertemplate='<b>%{x}</b><br>Plays: %{y}<br>Avg Minutes: %{customdata:.1f}<extra></extra>',
            customdata=data['avg_minutes']
        ))
        
        fig.update_layout(
            title='📅 Weekly Listening Patterns',
            xaxis=dict(title='Day of Week', **self.axis_style),
            yaxis=dict(title='Play Count', **self.axis_style),
            **self.default_layout
        )
        
        return fig
    
    def create_playlist_diversity_chart(self, data: pd.DataFrame) -> go.Figure:
        """Create scatter plot for playlist diversity analysis"""
        if data.empty:
            return self._create_empty_chart("No playlist data available")
        
        # Take top 20 playlists by track count for readability
        data_subset = data.nlargest(20, 'track_count')
        
        fig = px.scatter(
            data_subset,
            x='unique_artist_count',
            y='overall_diversity_score',
            size='track_count',
            hover_name='playlist_name',
            hover_data={
                'track_count': True,
                'unique_artist_count': True,
                'overall_diversity_score': ':.2f',
                'total_duration_minutes': ':.0f'
            },
            title='🎵 Playlist Diversity Analysis',
            labels={
                'unique_artist_count': 'Number of Unique Artists',
                'overall_diversity_score': 'Diversity Score',
                'track_count': 'Track Count'
            }
        )
        
        fig.update_traces(
            marker=dict(color=self.colors['primary'], opacity=0.7, line=dict(width=1, color='white'))
        )
        
        fig.update_layout(
            height=500,
            **self.default_layout
        )
        
        return fig
    
    def _create_empty_chart(self, message: str) -> go.Figure:
        """Create an empty chart with a message"""
        fig = go.Figure()
        
        fig.add_annotation(
            text=message,
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16, color="gray")
        )
        
        fig.update_layout(
            xaxis=dict(showgrid=False, showticklabels=False),
            yaxis=dict(showgrid=False, showticklabels=False),
            height=400,
            **self.default_layout
        )
        
        return fig