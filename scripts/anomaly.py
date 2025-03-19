import pandas as pd
import numpy as np
from typing import Union

def detect_anomalies(data: Union[pd.Series, np.ndarray], threshold: float = 3) -> pd.Series:
    """Detect anomalies using Z-score method.
    
    Args:
        data (Union[pd.Series, np.ndarray]): Input data
        threshold (float): Z-score threshold for anomaly detection
    Returns:
        pd.Series: Boolean mask indicating anomalies
    """
    mean = data.mean()
    std = data.std()
    z_scores = abs((data - mean) / std)
    return z_scores > threshold
