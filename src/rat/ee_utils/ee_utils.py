import numpy as np
import ee
#### initialize the connection to the server ####
from ee_utils.ee_config import service_account,key_file

ee_credentials = ee.ServiceAccountCredentials(service_account, key_file)
ee.Initialize(ee_credentials)
#### Connection established ####

# Coverts a polygon geometry object to earth engine feature
def poly2feature(polygon,buffer_distance):
    '''
    Returns an earth engine feature with the same geometry as the polygon object with buffer_distance added. buffer_distance is in meters.
    '''
    if(polygon.type=='MultiPolygon'):
        all_cords=[]
        for poly in polygon.geoms:
            x,y = poly.exterior.coords.xy
            all_cords.append(np.dstack((x,y)).tolist())
            g=ee.Geometry.MultiPolygon(all_cords).buffer(buffer_distance)  #buffer for polygon_object in meters 
        
    else:  
        x,y = polygon.exterior.coords.xy
        cords = np.dstack((x,y)).tolist()
        
        g=ee.Geometry.Polygon(cords).buffer(buffer_distance)  #buffer for polygon_object in meters
        
    feature = ee.Feature(g)
    return feature