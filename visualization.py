# -*- coding: utf-8 -*-
"""Untitled2.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1qByo8zO417vNZ6M4PPtlFjbee4KkO1Hc
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

"""
Created on Thu Mar 31 00:50:07 2022

@author: Antika Maudi Lanthasari
"""

def listColor():
    colorscale = ['aggrnyl', 'agsunset', 'algae', 'amp', 'armyrose', 'balance', 'blackbody',
                  'bluered', 'blues', 'blugrn', 'bluyl', 'brbg', 'brwnyl', 'bugn', 'bupu',
                  'burg', 'burgyl', 'cividis', 'curl', 'darkmint', 'deep', 'delta', 'dense',
                  'earth', 'edge', 'electric', 'emrld', 'fall', 'geyser', 'gnbu', 'gray',
                  'greens', 'greys', 'haline', 'hot', 'hsv', 'ice', 'icefire', 'inferno',
                  'jet', 'magenta', 'magma', 'matter', 'mint', 'mrybm', 'mygbm', 'oranges',
                  'orrd', 'oryel', 'peach', 'phase', 'picnic', 'pinkyl', 'piyg', 'plasma',
                  'plotly3', 'portland', 'prgn', 'pubu', 'pubugn', 'puor', 'purd', 'purp',
                  'purples', 'purpor', 'rainbow', 'rdbu', 'rdgy', 'rdpu', 'rdylbu', 'rdylgn', 
                  'redor', 'reds', 'solar', 'spectral', 'speed', 'sunset', 'sunsetdark',
                  'teal', 'tealgrn', 'tealrose', 'tempo', 'temps', 'thermal', 'tropic', 
                  'turbid', 'twilight', 'viridis', 'ylgn', 'ylgnbu', 'ylorbr', 'ylorrd']
    
def plotWorldMap(data : pd.DataFrame, **parameter) -> go.Figure:
    value    = parameter.get('value')
    typeshow = parameter.get('typeshow')
    
    if(typeshow == 'All'):
        data['annotate'] = '(TV Show : ' + data['TV Show'].astype('str') + ' - ' + \
                           'Movie : '   + data['Movie'].astype('str') + ')<br>' + data['country']
    elif(typeshow == 'TV Show'):
        data['annotate'] = '(TV Show : ' + data[value].astype('str') + ')<br>' + data['country']
    elif(typeshow == 'Movie'):
        data['annotate'] = '(Movie : '   + data[value].astype('str') + ')<br>' + data['country']
               
    fig = go.Figure(data = go.Choropleth(
                            locations = data['countrycode'],
                            z = data[value],
                            text = data['annotate'],
                            colorscale = 'viridis',
                            autocolorscale = False,
                            reversescale = True,
                            marker_line_color = 'darkgray',
                            marker_line_width = 0.5,
                            colorbar_title = 'Netflix Production'
                    ))

    fig.update_layout(
            autosize = False,
            width = 600,
            height = 600,
            geo = dict(
                    showframe = False,
                    showcoastlines = False,
                    projection_type = 'equirectangular'
                    )
            )
    return fig

def plotBarStack(data: pd.DataFrame, **parameter) -> go.Figure:
    X = parameter.get('x')
    Y = parameter.get('y')
        
    data['total'] = data.groupby(X)[Y].transform('sum')
    data = data.sort_values(by = ['total'], ascending = False).reset_index(drop = True)
    
    stacked   = data['video_type'].unique().tolist()  
    colour    = ['#fff000', '#02006c']
    types     = ['TV Show', 'Movie']
    colourMap = dict(zip(types, colour))
    
    fig = go.Figure()
    
    for i in range(len(stacked)):
        dataFilter = data[data['video_type'] == stacked[i]].reset_index(drop = True)
        fig.add_trace(go.Bar(
            x = dataFilter[Y],
            y = dataFilter[X],
            name = stacked[i],
            orientation = 'h',
            marker = dict(color = colourMap[stacked[i]])
            ))

    fig.update_layout(barmode = 'stack', yaxis = dict(autorange = "reversed"))
    return fig

def plotLine(data: pd.DataFrame, **parameter) -> go.Figure:
    X = parameter.get('x')
    Y = parameter.get('y')
    xlabel = parameter.get('xlabel', X)
    ylabel = parameter.get('ylabel', Y)
    colours = parameter.get('colorby')
    
    try:
        fig = px.line(data, x = X, y = Y, 
                      color = colours,  
                      labels = {X : xlabel, Y : ylabel, 'country - type' : 'country & type'}
                      )
    except:
        fig = px.line(data, x = X, y = Y, 
                      labels = {X : xlabel, Y : ylabel}
                      )
    fig.update_traces(mode = 'markers+lines')
    
    fig.update_layout(
            showlegend = True,
            width = 1000,
            height = 600
            )
    return fig

def plotHeatMap(data : pd.DataFrame, **parameter) -> go.Figure:
    colours = parameter.get('colorby')
    fig = px.imshow(data,
                labels=dict(color = ylorrd)
               )
    fig.update_xaxes(side="top")
    return fig

def plotPyramids(data : pd.DataFrame, **parameter) -> go.Figure:
    X = parameter.get('x')
    Y = parameter.get('y')
    
    data = data.sort_values(by = X, ascending = True, ignore_index = True)
    
    fig = make_subplots(rows = 1, cols = 2, horizontal_spacing = 0.15, shared_yaxes = True)

    fig.add_bar(
        x = data[X[0]],
        y = data[Y],
        orientation = "h",
        row = 1,
        col = 2,
        hovertemplate = "%{y}: %{x:.0f}<extra></extra>",
        name = X[0]
    )
    
    fig.add_bar(
        x = -data[X[1]],
        y = data[Y],
        orientation = "h",
        customdata = data[X[1]],
        row = 1,
        col = 1,
        hovertemplate="%{y}: %{customdata:.0f}<extra></extra>",
        name = X[1]
    )
    
    fig.update_layout(
        margin = dict(b = 20, t = 20, l = 20, r = 20),
        width = 800,
        height = 450,
        yaxis_visible = False,
        yaxis_fixedrange = True,
        # xaxis
        xaxis_title = X[1],
        xaxis_tick0 = 0,
        xaxis_dtick = 20,
        xaxis_tickvals = [-1000000, -600000, -400000, -200000, 0],
        xaxis_ticktext = ['1000000', '600000', '400000', '200000', '0'],
        xaxis_fixedrange = False,
        # xaxis2
        xaxis2_title = X[0],
        showlegend = False,
        xaxis2_tickvals = [0, 200000, 400000, 600000, 1000000],
        xaxis2_ticktext = ['0', '200000', '400000', '600000', '1000000'],
        xaxis2_fixedrange = False,
        
    )
    
    for i, label in enumerate(data[Y]):
        fig.add_annotation(
            text = label,
            x = 0.5, xanchor = "center", xref = "paper", ax = 0,
            y = i, yref = "y", ay = 0,
            showarrow = True,
        )
        
    return fig
