import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Statistical libraries
# from scipy import stats
# from scipy.stats import zscore, norm, ttest_ind
# import statsmodels.api as sm
# from statsmodels.tsa.seasonal import seasonal_decompose
# from statsmodels.tsa.stattools import adfuller
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.covariance import EllipticEnvelope

# Machine Learning
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error
# import xgboost as xgb

# Geographical
# import folium
# from folium.plugins import HeatMap

print("Libraries imported for Chicago analysis!")

def load_chicago_data():
    """Load và lọc dữ liệu Chicago từ cả 3 datasets"""
    print("Loading Chicago data...")
    
    # Load datasets
    accidents = pd.read_csv('data/accidents/US_Accidents_March23.csv')
    congestion = pd.read_csv('data/us_congestion_2016_2022_sample_2m.csv/us_congestion_2016_2022_sample_2m.csv')
    weather = pd.read_csv('data/WeatherEvents_Jan2016-Dec2022.csv/WeatherEvents_Jan2016-Dec2022.csv')
    
    # Lọc Chicago accidents - multiple possible city names
    chicago_accidents = accidents[
        (accidents['City'].str.contains('Chicago', case=False, na=False)) |
        (accidents['County'].str.contains('Cook', case=False, na=False)) |
        (accidents['State'].eq('IL')) & (accidents['City'].isin(['Chicago', 'CHICAGO']))
    ].copy()
    
    # Lọc Chicago congestion
    chicago_congestion = congestion[
        (congestion['City'].str.contains('Chicago', case=False, na=False)) |
        (congestion['County'].str.contains('Cook', case=False, na=False)) |
        (congestion['State'].eq('IL')) & (congestion['City'].isin(['Chicago', 'CHICAGO']))
    ].copy()
    
    # Lọc Chicago weather - sử dụng airport codes
    chicago_airports = ['ORD', 'MDW', 'CHI']  # O'Hare, Midway, Chicago
    chicago_weather = weather[
        weather['AirportCode'].isin(chicago_airports) |
        weather['City'].str.contains('Chicago', case=False, na=False)
    ].copy()
    
    print(f"Chicago Accidents: {len(chicago_accidents):,} records")
    print(f"Chicago Congestion: {len(chicago_congestion):,} records") 
    print(f"Chicago Weather: {len(chicago_weather):,} records")
    
    return chicago_accidents, chicago_congestion, chicago_weather

chicago_accidents, chicago_congestion, chicago_weather = load_chicago_data()

chicago_accidents.to_csv("data/chicago/chicago_accidents.csv")
chicago_congestion.to_csv("data/chicago/chicago_congestion.csv")
chicago_weather.to_csv("data/chicago/chicago_weather.csv")