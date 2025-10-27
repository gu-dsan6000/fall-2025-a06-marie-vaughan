from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, count, trim
import os
import subprocess
import sys
import time
import logging
import pandas as pd

# Configure logging with basicConfig
logging.basicConfig(
    level=logging.INFO,  # Set the log level to INFO
    # Define log message format
    format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
)

logger = logging.getLogger(__name__)
def create_spark_session(master_url):
    """Create a Spark session optimized for cluster execution."""

    spark = (
        SparkSession.builder
        .appName("Log File Analysis - Problem 1")

        # Cluster Configuration
        .master(master_url)  # Connect to Spark cluster

        # Memory Configuration
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")

        # Executor Configuration
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", "6")  # Use all available cores across cluster

        # S3 Configuration - Use S3A for AWS S3 access
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")

        # Performance settings for cluster execution
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

        # Serialization
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        # Arrow optimization for Pandas conversion
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")

        .getOrCreate()
    )

    logger.info("Spark session created successfully for cluster execution")
    return spark


def main():
    """Main function for Problem 1"""

    logger.info("Starting Problem 1")

    # Parse command line arguments
    master_url = None
    net_id = None
    
    i = 1
    while i < len(sys.argv):
        if sys.argv[i] == "--net-id" and i + 1 < len(sys.argv):
            net_id = sys.argv[i + 1]
            i += 2
        elif sys.argv[i].startswith("spark://"):
            master_url = sys.argv[i]
            i += 1
        else:
            i += 1
    
    # Get master URL
    if not master_url:
        master_private_ip = os.getenv("MASTER_PRIVATE_IP")
        if master_private_ip:
            master_url = f"spark://{master_private_ip}:7077"
        else:
            print("❌ Error: Master URL not provided")
            print("   Usage: python problem1.py spark://<master-ip>:7077 --net-id YOUR-NET-ID")
            return 1
    
    # Get net ID
    if not net_id:
        print("❌ Error: --net-id argument required")
        print("   Usage: python problem1.py spark://<master-ip>:7077 --net-id YOUR-NET-ID")
        return 1

    print(f"Connecting to Spark Master at: {master_url}")
    print(f"Using net ID: {net_id}")
    logger.info(f"Using Spark master URL: {master_url}")
    logger.info(f"Using net ID: {net_id}")

    # Create Spark session
    logger.info("Initializing Spark session for cluster execution")
    spark = create_spark_session(master_url)

    # Construct S3 bucket path from net ID
    spark_logs_bucket = f"s3a://{net_id}-assignment-spark-cluster-logs"
    
    print(f"S3 Bucket: {spark_logs_bucket}")
    
    # Define the path to log files in S3
    log_path = f"{spark_logs_bucket}/data/application_*/*"
    
    print(f"Reading log files from: {log_path}")
    logger.info(f"Log path: {log_path}")

    try:
        # LOG LEVEL COUNTS
        # Read log files as text
                # DEBUG: Print everything
        print("="*60)
        print("DEBUG INFORMATION:")
        print(f"net_id: {net_id}")
        print(f"spark_logs_bucket: {spark_logs_bucket}")
        print(f"log_path: {log_path}")
        print(f"Type of log_path: {type(log_path)}")
        print("="*60)
        
        # Read log files as text
        print("Reading log files from S3...")
        logs_df = spark.read.text(log_path)
        
        # Get total log lines processed
        total_lines = logs_df.count()
        print(f"Total log lines read: {total_lines:,}")
        
        # Extract log level using regex pattern
        logs_with_level = logs_df.withColumn(
            "log_level",
            regexp_extract(col("value"), r'\b(INFO|WARN|ERROR|DEBUG)\b', 1)
        )
        
        # Filter out rows where no log level was found
        logs_with_level = logs_with_level.filter(col("log_level") != "")
        
        # OUTPUT 1: COUNTS
        print("Starting output 1: Log Level Counts")
        # Count occurrences of each log level
        log_level_counts = logs_with_level.groupBy("log_level") \
            .agg(count("*").alias("count")) \
            .orderBy(col("count").desc())
        
        # Create output directory locally
        output_dir = "data/output"
        os.makedirs(output_dir, exist_ok=True)
        
        # Write to CSV with header
        output_path = f"{output_dir}/problem1_counts.csv"
        
        # Convert to Pandas for easier CSV writing with exact format
        pandas_df = log_level_counts.toPandas()
        pandas_df.to_csv(output_path, index=False)
        
        print(f"\nResults saved to: {output_path}")

        # OUTPUT 2: 10 RANDOM LOG ENTRIES
        print("Starting output 2: 10 Random Log Entries")
        
        # Rename 'value' column to 'log_entry' and select only needed columns
        logs_with_level_sample = logs_with_level.select(
            trim(col("value")).alias("log_entry"),
            col("log_level")
        )
        
        # Get 10 random samples
        sample_logs = logs_with_level_sample.sample(fraction=0.01, seed=42).limit(10)
        
        # Write to CSV with header
        output_path = f"{output_dir}/problem1_sample.csv"
        
        # Convert to Pandas for easier CSV writing with exact format
        pandas_df = sample_logs.toPandas()
        
        # Reorder columns to match expected output (log_entry, log_level)
        pandas_df = pandas_df[['log_entry', 'log_level']]
        
        # Save to CSV with proper quoting
        pandas_df.to_csv(output_path, index=False, quoting=1) 
        
        print(f"\nResults saved to: {output_path}")

        # OUTPUT 3: SUMMARY STATISTICS
        print("Starting output 3: Summary Statistics")

        # Get total lines with log levels
        total_with_levels = logs_with_level.count()
        
        # Collect results to driver
        level_counts = log_level_counts.collect()
        
        # Get unique log levels count
        unique_levels = len(level_counts)
        
        # Write summary to text file
        output_path = f"{output_dir}/problem1_summary.txt"
        
        with open(output_path, 'w') as f:
            # Write header statistics
            f.write(f"Total log lines processed: {total_lines:,}\n")
            f.write(f"Total lines with log levels: {total_with_levels:,}\n")
            f.write(f"Unique log levels found: {unique_levels}\n")
            f.write(f"\n")
            f.write(f"Log level distribution:\n")
            
            # Write each log level with count and percentage
            for row in level_counts:
                level = row['log_level']
                cnt = row['count']
                percentage = (cnt / total_with_levels) * 100
                
                # Format with proper spacing and alignment
                f.write(f"  {level:5s} : {cnt:9,} ({percentage:5.2f}%)\n")

        print(f"\nResults saved to: {output_path}")
        
        print("\n" + "="*60)
        print("✅ All outputs completed successfully!")
        print("="*60)

    except Exception as e:
        print(f"❌ Error processing log files: {str(e)}")
        logger.error(f"Error: {str(e)}", exc_info=True)
        return 1

    finally:
        # Stop Spark session
        spark.stop()
        logger.info("Spark session stopped")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())