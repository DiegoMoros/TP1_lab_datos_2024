import pandas as pd
import os

def split_in_df(df, n_column, delimiter,drop_col = False,save_csv = False,file_name="Datos/df_split.csv"):
    """
    Realiza un split en una columna especifica de un dataframe
    :param df: DataFrame de entrada
    :param n_column: Nombre de la columna a separar
    :param delimiter: str utilizado como delimitador
    :param drop_col: bool para eliminar la columna orginal
    :param save_csv: bool para guardar el DataFrame como archivo CSV
    :param file_name: Nombre para guardar el dataframe
    :return: DataFrame con la columna separada
    """
    if not os.path.isfile(file_name):
        if n_column in df.columns:
            separated_columns = df[n_column].str.split(delimiter, expand=True)
        
            separated_columns.columns = [f"{n_column}_{i+1}" for i in range(separated_columns.shape[1])]
        
            df = pd.concat([df, separated_columns], axis=1)
            
            if drop_col:
                df.drop(columns=[n_column], inplace=True)
            if save_csv:
                df.to_csv(file_name, index=False)
            
            return pd.read_csv(file_name)
        else:
            raise ValueError(f"La columna '{n_column}' no existe en el DataFrame.")
    else: 
        return pd.read_csv(file_name)

        