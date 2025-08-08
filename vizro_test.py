# Vizro is an open-source toolkit for creating modular data visualization applications.
# check out https://github.com/mckinsey/vizro for more info about Vizro
# and checkout https://vizro.readthedocs.io/en/stable/ for documentation.

import vizro.plotly.express as px
from vizro import Vizro
import vizro.models as vm
import geopandas as gpd
import pandas as pd
from vizro.managers import data_manager
from vizro.tables import dash_ag_grid
from flask_caching import Cache
import fsspec
from vizro.models.types import capture


Vizro._reset()

git_url = "https://raw.githubusercontent.com/JayCampanell/kenyaData/main/data/kenya_gpp_data.parquet"

with fsspec.open(git_url) as file:
    gdf = gpd.read_parquet(file)
    gdf['id'] = gdf['shapeName'] + '_' + gdf['formatted_date']

geojson = gdf[['geometry', 'id']].set_index('id').__geo_interface__


def load_nonspatial_data(date = "2021-01-01"):
    filtered = gdf[gdf['formatted_date'] == date]
    return filtered.drop('geometry', axis = 1)

def load_spatial():
    # filtered['id'] = filtered['shapeName'] + '_' + gdf['formatted_date']
    geojson = gdf[['geometry', 'id']].set_index('id').__geo_interface__

    return geojson

@capture("graph")
def choropleth(data_frame, geojson, location, color):
    fig = px.choropleth_map(
        data_frame,
        geojson = geojson,
        locations = location,
        color = color,
        center = {"lat": 0.0236, "lon": 37.9062},
        zoom = 6
    )
    return fig

data_manager.cache = Cache(config={"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 60*60*24})

data_manager['kenya_no_geo'] = load_nonspatial_data
data_manager['geo'] = load_spatial



page = vm.Page(
    title="Vizro Test",
    components=[
        vm.AgGrid(figure=dash_ag_grid(data_frame='kenya_no_geo', dashGridOptions={"pagination": True})),
        vm.Graph(figure = choropleth(
                           data_frame="kenya_no_geo",
                           geojson = geojson,
                           location = "id",
                           color = 'Gpp'
                           ),
                           id = 'graph')
    ],
    controls = [
       # vm.Filter(column = "formatted_date", selector = vm.Dropdown(value = str("2021-01-01")))
       vm.Parameter(targets = ['graph.data_frame.date'],
                    selector = vm.DatePicker(value = '2021-01-01'), range = False)
    ])

dashboard = vm.Dashboard(pages=[page])
Vizro().build(dashboard).run()
