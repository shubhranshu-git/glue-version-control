from pyspark.context import SparkContext
from awsglue.context import GlueContext

def batch_update():
    """
    Sample function that demonstrates a data processing task
    """
    # Initialize Glue context
    glueContext = GlueContext(SparkContext.getOrCreate())
    
    # Sample processing logic
    print("Starting batch update process...")
    
    # Simulate some processing
    data = {"id": [1, 2, 3], "value": ["a", "b", "c"]}
    df = glueContext.spark_session.createDataFrame(data)
    
    # Some transformation
    df = df.withColumn("processed", df.value.upper())
    
    print(f"Processed {df.count()} records")
    return df

