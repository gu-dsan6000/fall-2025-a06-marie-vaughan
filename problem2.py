from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, min, max, count, first, last
from pyspark.sql.functions import to_timestamp, unix_timestamp, substring
import os
import sys
import logging
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import builtins

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
)

logger = logging.getLogger(__name__)

def create_spark_session(master_url):
    """Create a Spark session optimized for cluster execution."""
    
    spark = (
        SparkSession.builder
        .appName("Cluster Timeline Analysis - Problem 2")
        .master(master_url)
        
        # Memory Configuration
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")
        
        # Executor Configuration
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", "6")
        
        # S3 Configuration
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        
        # Performance settings
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        
        .getOrCreate()
    )
    
    logger.info("Spark session created successfully for cluster execution")
    return spark

def extract_timestamp(log_line):
    """Extract timestamp from log line in format: YY/MM/DD HH:MM:SS"""
    # Pattern: YY/MM/DD HH:MM:SS at the beginning of the line
    return regexp_extract(log_line, r'^(\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})', 1)

def main():
    """Main function for Problem 2"""
    
    logger.info("Starting Problem 2")
    
    # Parse command line arguments
    master_url = None
    net_id = None
    use_local = False
    
    i = 1
    while i < len(sys.argv):
        if sys.argv[i] == "--net-id" and i + 1 < len(sys.argv):
            net_id = sys.argv[i + 1]
            i += 2
        elif sys.argv[i] == "--local":
            use_local = True
            i += 1
        elif sys.argv[i].startswith("spark://"):
            master_url = sys.argv[i]
            i += 1
        else:
            i += 1
    
    # Handle local testing mode
    if use_local:
        print("Running in LOCAL TEST MODE")
        master_url = "local[*]"
        net_id = "local-test"
    else:
        # Get master URL
        if not master_url:
            master_private_ip = os.getenv("MASTER_PRIVATE_IP")
            if master_private_ip:
                master_url = f"spark://{master_private_ip}:7077"
            else:
                print("❌ Error: Master URL not provided")
                print("   Usage: python problem2.py spark://<master-ip>:7077 --net-id YOUR-NET-ID")
                print("   Or for local testing: python problem2.py --local")
                return 1
        
        # Get net ID
        if not net_id:
            print("❌ Error: --net-id argument required")
            print("   Usage: python problem2.py spark://<master-ip>:7077 --net-id YOUR-NET-ID")
            print("   Or for local testing: python problem2.py --local")
            return 1
    
    print(f"Connecting to Spark Master at: {master_url}")
    print(f"Using net ID: {net_id}")
    logger.info(f"Using Spark master URL: {master_url}")
    logger.info(f"Using net ID: {net_id}")
    
    # Create Spark session
    logger.info("Initializing Spark session for cluster execution")
    spark = create_spark_session(master_url)
    
    # Determine data path based on mode
    if use_local:
        log_path = "data/sample/application_*/*"
        print(f"Using local sample data: {log_path}")
    else:
        # Construct S3 bucket path from net ID
        spark_logs_bucket = f"s3a://{net_id}-assignment-spark-cluster-logs"
        log_path = f"{spark_logs_bucket}/data/application_*/*"
        print(f"S3 Bucket: {spark_logs_bucket}")
    
    print(f"Reading log files from: {log_path}")
    logger.info(f"Log path: {log_path}")
    
    try:
        # Read log files as text
        print("Reading log files from S3...")
        logs_df = spark.read.text(log_path)
        
        # Add input file name to identify which application each log belongs to
        from pyspark.sql.functions import input_file_name
        logs_df = logs_df.withColumn("file_path", input_file_name())
        
        # Extract application_id from file path
        # Path format: .../application_CLUSTERID_APPNUM/container_...
        logs_df = logs_df.withColumn(
            "application_id",
            regexp_extract(col("file_path"), r'application_(\d+_\d+)', 0)
        )
        
        # Extract cluster_id and app_number from application_id
        logs_df = logs_df.withColumn(
            "cluster_id",
            regexp_extract(col("application_id"), r'application_(\d+)_\d+', 1)
        )
        logs_df = logs_df.withColumn(
            "app_number",
            regexp_extract(col("application_id"), r'application_\d+_(\d+)', 1)
        )
        
        # Extract timestamp from log line
        logs_df = logs_df.withColumn(
            "timestamp_str",
            regexp_extract(col("value"), r'^(\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})', 1)
        )
        
        # Filter out rows without timestamps
        logs_with_time = logs_df.filter(col("timestamp_str") != "")
        
        # Convert timestamp string to timestamp type (format: YY/MM/DD HH:MM:SS)
        logs_with_time = logs_with_time.withColumn(
            "timestamp",
            to_timestamp(col("timestamp_str"), "yy/MM/dd HH:mm:ss")
        )
        
        # Filter out invalid applications (no application_id)
        logs_with_time = logs_with_time.filter(col("application_id") != "")
        
        print(f"Total log entries with timestamps: {logs_with_time.count():,}")
        
        # OUTPUT 1: TIMELINE DATA
        print("\nGenerating timeline data...")
        
        # Group by application and get start/end times
        timeline_df = logs_with_time.groupBy("cluster_id", "application_id", "app_number").agg(
            min("timestamp").alias("start_time"),
            max("timestamp").alias("end_time")
        ).orderBy("cluster_id", "app_number")
        
        # Create output directory
        output_dir = "data/output"
        os.makedirs(output_dir, exist_ok=True)
        
        # Convert to Pandas and save
        timeline_pandas = timeline_df.toPandas()
        
        # Format timestamps
        timeline_pandas['start_time'] = pd.to_datetime(timeline_pandas['start_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
        timeline_pandas['end_time'] = pd.to_datetime(timeline_pandas['end_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        output_path = f"{output_dir}/problem2_timeline.csv"
        timeline_pandas.to_csv(output_path, index=False)
        print(f"Timeline data saved to: {output_path}")
        
        # OUTPUT 2: CLUSTER SUMMARY
        print("\nGenerating cluster summary...")
        
        cluster_summary = timeline_df.groupBy("cluster_id").agg(
            count("application_id").alias("num_applications"),
            min("start_time").alias("cluster_first_app"),
            max("end_time").alias("cluster_last_app")
        ).orderBy(col("num_applications").desc())
        
        cluster_summary_pandas = cluster_summary.toPandas()
        
        # Format timestamps
        cluster_summary_pandas['cluster_first_app'] = pd.to_datetime(cluster_summary_pandas['cluster_first_app']).dt.strftime('%Y-%m-%d %H:%M:%S')
        cluster_summary_pandas['cluster_last_app'] = pd.to_datetime(cluster_summary_pandas['cluster_last_app']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        output_path = f"{output_dir}/problem2_cluster_summary.csv"
        cluster_summary_pandas.to_csv(output_path, index=False)
        print(f"Cluster summary saved to: {output_path}")
        
        # OUTPUT 3: STATISTICS
        print("\nGenerating statistics...")
        
        total_clusters = cluster_summary_pandas.shape[0]
        total_applications = cluster_summary_pandas['num_applications'].sum()
        avg_apps_per_cluster = total_applications / total_clusters
        
        output_path = f"{output_dir}/problem2_stats.txt"
        with open(output_path, 'w') as f:
            f.write(f"Total unique clusters: {total_clusters}\n")
            f.write(f"Total applications: {total_applications}\n")
            f.write(f"Average applications per cluster: {avg_apps_per_cluster:.2f}\n")
            f.write(f"\n")
            f.write(f"Most heavily used clusters:\n")
            
            for idx, row in cluster_summary_pandas.head(10).iterrows():
                f.write(f"  Cluster {row['cluster_id']}: {row['num_applications']} applications\n")
        
        print(f"Statistics saved to: {output_path}")
        
        # OUTPUT 4: BAR CHART
        print("\nGenerating bar chart...")
        
        plt.figure(figsize=(12, 6))
        bars = plt.bar(range(len(cluster_summary_pandas)), 
                       cluster_summary_pandas['num_applications'],
                       color=plt.cm.Set3(range(len(cluster_summary_pandas))))
        
        # Add value labels on top of bars
        for i, (bar, value) in enumerate(zip(bars, cluster_summary_pandas['num_applications'])):
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height(), 
                    str(value), ha='center', va='bottom', fontsize=10)
        
        plt.xlabel('Cluster ID', fontsize=12)
        plt.ylabel('Number of Applications', fontsize=12)
        plt.title('Number of Applications per Cluster', fontsize=14, fontweight='bold')
        plt.xticks(range(len(cluster_summary_pandas)), 
                  cluster_summary_pandas['cluster_id'], 
                  rotation=45, ha='right')
        plt.tight_layout()
        
        output_path = f"{output_dir}/problem2_bar_chart.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Bar chart saved to: {output_path}")
        
        # OUTPUT 5: DENSITY PLOT
        print("\nGenerating density plot...")
        
        # Find the largest cluster
        largest_cluster_id = cluster_summary_pandas.iloc[0]['cluster_id']
        
        # Calculate duration for the largest cluster
        timeline_largest = timeline_pandas[timeline_pandas['cluster_id'] == largest_cluster_id].copy()
        
        # Convert to datetime and calculate duration in minutes
        timeline_largest['start_time_dt'] = pd.to_datetime(timeline_largest['start_time'])
        timeline_largest['end_time_dt'] = pd.to_datetime(timeline_largest['end_time'])
        timeline_largest['duration_minutes'] = (timeline_largest['end_time_dt'] - timeline_largest['start_time_dt']).dt.total_seconds() / 60
        
        # Remove any zero or negative durations
        timeline_largest = timeline_largest[timeline_largest['duration_minutes'] > 0]
        
        n_samples = len(timeline_largest)
        
        if n_samples < 2:
            print(f"Warning: Only {n_samples} sample(s) in largest cluster. Skipping density plot (need at least 2 samples).")
        else:
            plt.figure(figsize=(10, 6))
            
            # Determine number of bins (max 30, but adjust for small datasets)
            n_bins = builtins.min(30, builtins.max(5, n_samples // 2))
            plt.hist(timeline_largest['duration_minutes'], bins=n_bins, alpha=0.7, color='skyblue', edgecolor='black')
            
            # Add KDE overlay only if we have enough samples
            if n_samples >= 3:
                try:
                    from scipy import stats
                    kde = stats.gaussian_kde(timeline_largest['duration_minutes'])
                    x_range = np.linspace(timeline_largest['duration_minutes'].min(), 
                                         timeline_largest['duration_minutes'].max(), 200)
                    
                    # Scale KDE to match histogram
                    bin_width = (timeline_largest['duration_minutes'].max() - timeline_largest['duration_minutes'].min()) / n_bins
                    kde_scaled = kde(x_range) * len(timeline_largest) * bin_width
                    
                    plt.plot(x_range, kde_scaled, 'r-', linewidth=2, label='KDE')
                    plt.legend()
                except Exception as e:
                    print(f"Warning: Could not generate KDE overlay: {e}")
            
            # Use log scale only if range is large enough
            duration_range = timeline_largest['duration_minutes'].max() / timeline_largest['duration_minutes'].min()
            if duration_range > 10:
                plt.xscale('log')
                plt.xlabel('Job Duration (minutes, log scale)', fontsize=12)
            else:
                plt.xlabel('Job Duration (minutes)', fontsize=12)
            
            plt.ylabel('Frequency', fontsize=12)
            plt.title(f'Job Duration Distribution - Cluster {largest_cluster_id} (n={n_samples})', 
                     fontsize=14, fontweight='bold')
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            
            output_path = f"{output_dir}/problem2_density_plot.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            print(f"Density plot saved to: {output_path}")
        
        print("\n" + "="*60)
        print("All outputs completed successfully!")
        print("="*60)
        
    except Exception as e:
        print(f"Error processing log files: {str(e)}")
        logger.error(f"Error: {str(e)}", exc_info=True)
        return 1
    
    finally:
        # Stop Spark session
        spark.stop()
        logger.info("Spark session stopped")
    
    return 0

if __name__ == "__main__":
    import numpy as np
    sys.exit(main())