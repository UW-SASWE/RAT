import numpy as np

def penman(RN, T, va, ea, P, A, rhowlambda=28.34):
    """
    (From Physical Hydrology slides - TODO: replace with actual reference later) for E in [mm/d] ->
        RN:     Net incoming radiation at surface [W/m2]
        va:     Wind speed [m/s]
        ea:     actual air vapor pressure at air temperature [kPa]
        rhowlambda:
                latent heat of vaporization = 28.34 [W•d/mm•m3]
    -----

        es:     saturation vap. pressure at air temperature [kPa]
    to calculate es ->
        T:      Air Temperautre [°C]
        es = 0.6108 * exp( (17.27*T)/(237.3 + T) )
    -----

        delta:  slope of the curve relating es to Ts [kPa/°C]
    to calculate delta ->
        delta = 4098 * es/(237.3 + T)^2
    -----

        gamma:  Psychrometric constant [kPa/°C]
    to Calculate gamma -> 
        P:      Atmospheric pressure [kPa]
        gamma = (ca * P) / (0.622 * 2.45 * 10^6 ) 
            ca is practically ≈ 1000 J/kg•°C
    -----

        KE:     Theoretical Mass transfer coefficient [mm•s/m•day 1/kPa] 
    to calculate KE ->
        A:      Area of lake in km^s
        KE = 1.26 * LakeArea^-0.05     from Dingman, which is derived from Harbeck, 1962.
    """
    es = 0.6108 * np.exp( (17.27*T)/(237.3 + T) )                    # kPa
    delta = 4098 * es/(237.3 + T)**2                                 # kPa

    gamma = (1000 * P)/(0.622 * 2.45 * 1e6)                          # kPa/°C

    # value used as-is from Dingman
    KE = 1.26 * np.power(A, -0.05)                                   # mm•S/m•day 1/kPa

    energybalance = (delta * RN)/(rhowlambda * (delta + gamma))
    massbalance = (KE * gamma * va * (es-ea))/(delta + gamma)

    E = energybalance + massbalance

    return E
