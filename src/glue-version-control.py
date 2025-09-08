
import sys
import os
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# Add the project root to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

# Import the helper function
from helpers.sf_spark_helper import batch_update

def main():
    # Initialize job
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    # Call the helper function
    print("Starting main job execution...")
    result_df = batch_update()
    
    # Show results
    result_df.show()
    
    # Job completion
    job.commit()

if __name__ == "__main__":
    main()