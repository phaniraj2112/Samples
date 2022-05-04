from utils.MathCalculations import BasicOperations as bo
def basiccal(col1:float,col2:float,funcname:str)->float:
    return getattr(bo,funcname)(col1,col2)
