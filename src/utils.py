from pathlib import Path
from pyspark.sql import DataFrame
from typing import Union
from src.config import OUTPUT_DIR


def save_result(result: Union[DataFrame, int, float, str], file_name: str, file_format: str = "csv") -> None:
    """
    Saves result from analysis to a specified output file

    args:
        result: Union[DataFrame, int, float, str]) -> result to save
        file_name: name of the output file
        output_dir: directory where the file will be stored
        file_format: format of the output file - defaults to csv

    returns:
        None
    """
    try:
            
        output_path = Path(OUTPUT_DIR)

        # initialize file path
        file_path = output_path / file_name

        # dataframe result handling
        if isinstance(result, DataFrame):
            if file_format == "csv":
                result.coalesce(1).write.csv(str(file_path), mode="overwrite", header=True)
            elif file_format == "json":
                result.coalesce(1).write.json(str(file_path), mode="overwrite")
            else:
                raise ValueError(f"file format unsupported: {file_format}")
            print(f"DataFrame result saved at -> {file_path}")

        # scaler result handling(int, float, str)
        elif isinstance(result, (int, float, str)):
            with open(f"{file_path}.txt", "w") as f:
                f.write(str(result))
            print(f"Scalar result saved at -> {file_path}.txt")

        # Handle unsupported result types
        else:
            raise TypeError(f"result unsupported: {type(result)}")
    
    except Exception as e:
        raise RuntimeError(f"failed to saveresult '{file_name}': {e}")

