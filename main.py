import os
import json
import geopandas as gpd
import pandas as pd
import ee
from datetime import datetime, timedelta





def authenticate_ee():
    """Authenticate Earth Engine using service account"""
    service_account = os.environ['SERVICE_ACCOUNT']
    credentials = ee.ServiceAccountCredentials(service_account, '/tmp/gee-service-account.json')
    ee.Initialize(credentials)

def get_last_update_date():
    """Get last update from local file"""
    if os.path.exists('data/metadata.json'):
        with open('data/metadata.json', 'r') as f:
            metadata = json.load(f)
            return datetime.fromisoformat(metadata['last_update'])
    return datetime.now() - timedelta(days=8)

def get_processed_image_ids():
    """Get list of already processed image IDs"""
    if os.path.exists('data/metadata.json'):
        with open('data/metadata.json', 'r') as f:
            metadata = json.load(f)
            return set(metadata.get('processed_ids', []))
    return set()

def save_metadata(date, processed_ids):
    """Save metadata to local file"""
    os.makedirs('data', exist_ok=True)
    metadata = {
        'last_update': date.isoformat(),
        'processed_ids': list(processed_ids)
    }
    with open('data/metadata.json', 'w') as f:
        json.dump(metadata, f)

def main():
    authenticate_ee()
    
    last_update = get_last_update_date()

    processed_ids = get_processed_image_ids()  # Get existing processed IDs
    
    print(f"Starting update. Last update: {last_update}")
    
    kenyaGeo = ee.FeatureCollection("WM/geoLab/geoBoundaries/600/ADM2").filter(ee.Filter.eq('shapeGroup', "KEN"))

    def getDF(image):
        doy = image.get('system:time_start')
        doyBand = ee.Image.constant(doy).rename('date')
        scaledImage = ee.Image(image.clip(kenyaGeo).multiply(0.0001))
        allBands = scaledImage.addBands(doyBand)
        reduced = allBands.reduceRegions(kenyaGeo, reducer = ee.Reducer.mean(), scale = 500, tileScale = 6)
        gppDF = ee.data.computeFeatures({'expression': reduced, 'fileFormat': "GEOPANDAS_GEODATAFRAME"})
        gppDF['formatted_date'] = pd.to_datetime(gppDF['date'], unit='ms').dt.strftime('%Y-%m-%d')
        print(gppDF['formatted_date'][0])
        return gppDF

    lastUpdateEE = ee.Date(last_update.strftime('%Y-%m-%d'))
    current = ee.Date(datetime.now().strftime('%Y-%m-%d'))

    kenyaImageCollection = ee.ImageCollection("MODIS/061/MOD17A2H").select("Gpp").filterBounds(kenyaGeo).filterDate(lastUpdateEE,current)
    
    gdfs = []
    new_processed_ids = processed_ids.copy()  # Start with existing processed IDs
    
    for img in kenyaImageCollection.getInfo()['features']:
        image_id = img['id']
        
        # Skip if already processed
        if image_id in processed_ids:
            print(f"Skipping already processed image: {image_id}")
            continue
            
        print(f"Processing new image: {image_id}")
        image = ee.Image(image_id)
        gdf = getDF(image)
        gdfs.append(gdf)
        new_processed_ids.add(image_id)  # Track this image as processed

    # Save results if there are new images
    if gdfs:
        combined_gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))
        
        # Pivot the Combined GDF
        combined_gdf['date'] = pd.to_datetime(combined_gdf['formatted_date'])
        combined_gdf['month-year'] = combined_gdf['date'].dt.to_period("M")

        grouped = combined_gdf.groupby(['geometry', 'shapeName','month-year'], as_index = False)['Gpp'].mean()
        pivot = grouped.pivot_table(index = ['shapeName', 'geometry'],
                                                columns = 'month-year',
                                                values = 'Gpp')

        pivot.columns = pivot.columns.strftime('%B %Y')

        pivot.reset_index(inplace = True)

        final = pivot.drop('geometry',axis = 1).rename(columns = {'shapeName': 'Sub-county'})
        final['Sub-county'] = final['Sub-county'] + ' Sub County'

        #dictionary = pd.read_csv('modis_df_with_county.csv')

        #final = dictionary[['County', 'Sub-county']].merge(final, on = 'Sub-county', how = 'left')

        # Load existing data
        if os.path.exists('data/kenya_gpp_data.parquet'):
            existing_df = pd.read_parquet('data/kenya_gpp_data.parquet')
            final_df = gpd.GeoDataFrame(existing_df.merge(final, on = ['County', 'Sub-county'], suffixes=('', '_new')))

            for col in list(existing_df.columns):
                if col in final.columns and col != 'Sub-county' and col != 'County':  # skip key
                    final_df[col] = final_df[[col, f'{col}_new']].mean(axis=1)
                    final_df.drop(columns=f'{col}_new', inplace=True)

            final_df = final_df.rename(columns = {'Sub-county': 'Subcoounty'})

        else:
            final_df = gpd.GeoDataFrame(final)
        
        # Save to data directory
        os.makedirs('data', exist_ok=True)
        final_df.to_parquet('data/kenya_gpp_data.parquet', index = False)

        # Get the maximum date from the combined GDF for metadata
        max_date_in_data = pd.to_datetime(combined_gdf['formatted_date']).max()
        update_date = max_date_in_data.to_pydatetime()
        
        print(f"Successfully processed {len(gdfs)} new images")
        print(f"Latest data date: {max_date_in_data.strftime('%Y-%m-%d')}")
    else:
        latest_update = get_last_update_date()
        print("No new images to process")
        # If no new images, use date of last image fallback
        update_date = latest_update
    
    # Always update metadata after checking for updates
    save_metadata(update_date, new_processed_ids)
    print("Metadata updated")
    

if __name__ == "__main__":
    main()