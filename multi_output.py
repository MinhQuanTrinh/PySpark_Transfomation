from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, upper, length, when

def process_and_write_outputs(input_path: str, output_path_1: str, output_path_2: str, file_format: str = "parquet"):
    """
    Reads a file, transforms it, and writes two separate outputs to disk.
    
    Parameters:
    - input_path: Path to input file (CSV/Parquet/etc.)
    - output_path_1: Path to write first output
    - output_path_2: Path to write second output
    - file_format: File format to read/write (default: parquet)
    """
    
    spark = SparkSession.builder.getOrCreate()
    
    # Read input file
    if file_format == "csv":
        df = spark.read.option("header", True).csv(input_path, inferSchema=True)
    else:
        df = spark.read.format(file_format).load(input_path)

    # Transformations: Add uppercase and string length columns
    df_transformed = df \
        .withColumn("name_upper", upper(col("name"))) \
        .withColumn("name_length", length(col("name"))) \
        .withColumn("salary_category", when(col("salary") < 50000, "Low")
                                      .when(col("salary") < 100000, "Medium")
                                      .otherwise("High"))
    
    # Output 1: Save full transformed DataFrame
    df_transformed.write.mode("overwrite").format(file_format).save(output_path_1)
    
    # Output 2: Save only categorized salary report
    df_category = df_transformed.select("name", "salary", "salary_category")
    df_category.write.mode("overwrite").format(file_format).save(output_path_2)
