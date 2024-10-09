import pandas as pd

def split_in_df(df, n_column, delimiter,drop_col = False):
    """
    Realiza un split en una columna especifica de un dataframe
    :param df: DataFrame de entrada
    :param n_column: Nombre de la columna a separar
    :param delimiter: Carácter utilizado como delimitador
    :param drop_col: bool para eliminar la columna orginal
    :return: DataFrame con la columna separada
    """
    if n_column in df.columns:
        separated_columns = df[n_column].str.split(delimiter, expand=True)
    
        separated_columns.columns = [f"{n_column}_{i+1}" for i in range(separated_columns.shape[1])]
    
        df = pd.concat([df, separated_columns], axis=1)
        
        if drop_col:
            df.drop(columns=[n_column], inplace=True)
        
        return df
    else:
        raise ValueError(f"La columna '{n_column}' no existe en el DataFrame.")