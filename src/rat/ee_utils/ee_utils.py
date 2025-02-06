import numpy as np
import ee

# Coverts a polygon geometry object to earth engine feature
def poly2feature(polygon,buffer_distance):
    '''
    Returns an earth engine feature with the same geometry as the polygon object with buffer_distance added. buffer_distance is in meters.
    '''
    if(polygon.geom_type=='MultiPolygon'):
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

def shape_index(polygon):
    """
    Calculate the shape index of a given polygon.

    The shape index is defined as the square of the perimeter divided by the area:
        SI = (perimeter^2) / area

    Parameters:
    polygon (shapely.geometry.Polygon): The input polygon.

    Returns:
    float: The shape index value.
    """
    return (polygon.length ** 2) / polygon.area

def compute_initial_tolerance(si):
    """
    Compute the initial simplification tolerance based on the shape index magnitude.

    The tolerance is set as 10^(-order_of_magnitude), where order_of_magnitude is 
    the exponent of the shape index rounded up.

    Parameters:
    si (float): The shape index value.

    Returns:
    float: The initial simplification tolerance.
    """
    order_of_magnitude = math.ceil(math.log10(si))  # Get the exponent of SI
    return 10 ** (-order_of_magnitude)  # Set tolerance as inverse of order

def simplify_geometry(polygon, threshold=800, initial_tolerance=None):
    """
    Simplify a polygon iteratively until its shape index falls below a given threshold.

    If no initial tolerance is provided, it is determined dynamically using the 
    compute_initial_tolerance function. The simplification process gradually 
    increases the tolerance by a factor of 3.5 in each iteration.

    Parameters:
    polygon (shapely.geometry.Polygon): The input polygon to simplify.
    threshold (float, optional): The shape index threshold for stopping simplification. Default is 800.
    initial_tolerance (float, optional): The starting tolerance for simplification. If None, 
                                         it is computed based on the shape index.

    Returns:
    shapely.geometry.Polygon: The simplified polygon.
    """
    simplification = 0
    si_original = shape_index(polygon)
    si = si_original
    if initial_tolerance is None:
        initial_tolerance = compute_initial_tolerance(si)
    else:
        initial_tolerance = initial_tolerance

    tolerance = initial_tolerance  # Start with an initial tolerance
    while si > threshold:  # Keep simplifying until shape index is below threshold
        simplification = 1
        simplified_polygon = polygon.simplify(tolerance, preserve_topology=True)
        
        # Stop if no significant simplification occurs
        if simplified_polygon == polygon:
            break
        
        polygon = simplified_polygon
        si = shape_index(polygon)
        tolerance *= 3.5  # Gradually increase tolerance for more simplification
    if simplification:
        print(f"Using simplified geometry to extract surface area because of complex shape.")
        print(f"Shape index of original geometry is {si_original} which is above threshold of {threshold}. Shape index of simplified geometry is {si}.")
    return polygon