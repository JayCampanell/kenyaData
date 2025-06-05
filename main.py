import os
import json
import geopandas as gpd
import pandas as pd
import ee
from datetime import datetime, timedelta

def authenticate_ee():
    """Authenticate Earth Engine using service account"""
    key_file = os.environ["GEE_PROJ"]
    ee.Authenticate()
    ee.Initialize(project = key_file)

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
    
    if (datetime.now() - last_update).days >= 8:
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
            combined_gdf = pd.concat(gdfs, ignore_index=True)
            
            # Load existing data
            if os.path.exists('data/kenya_gpp_data.csv'):
                existing_df = pd.read_csv('data/kenya_gpp_data.csv')
                final_df = gpd.GeoDataFrame(pd.concat([existing_df, combined_gdf], ignore_index=True))
            else:
                final_df = gpd.GeoDataFrame(combined_gdf)
            
            # Save to data directory
            os.makedirs('data', exist_ok=True)
            final_df.to_csv('data/kenya_gpp_data.csv', index=False)
            
            # Get the maximum date from the combined GDF for metadata
            max_date_in_data = pd.to_datetime(combined_gdf['formatted_date']).max()
            update_date = max_date_in_data.to_pydatetime()
            
            print(f"Successfully processed {len(gdfs)} new images")
            print(f"Latest data date: {max_date_in_data.strftime('%Y-%m-%d')}")
        else:
            print("No new images to process")
            # If no new images, use current date as fallback
            update_date = datetime.now()
        
        # Always update metadata after checking for updates
        save_metadata(update_date, new_processed_ids)
        print("Metadata updated")
        
    else:
        print("No update needed")

if __name__ == "__main__":
    main()