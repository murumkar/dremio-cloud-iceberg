import pyarrow.flight as flight
import pandas as pd
import boto3
import argparse
import sys
from datetime import datetime
from urllib.parse import urlparse
from dremio.arguments.parse import get_config
from dremio.flight.endpoint import DremioFlightEndpoint


class DremioS3Cleanup:
    """Dremio S3 cleanup tool for Dremio Cloud. This tool is used to query 
    Dremio for deleted tables and optionally delete the corresponding S3 objects.
    It uses the Dremio Python SDK to connect to Dremio and the boto3 library to 
    interact with S3.
    """
    
    def __init__(self):
        self.flight_client = None
        self.s3_client = None
    
    def parse_arguments(self):
        """Parse command line arguments"""
        parser = argparse.ArgumentParser(
            description='Query Dremio and optionally delete S3 objects',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
  python cleanup.py --bucket my-bucket                                     # Query all deleted tables in bucket
  python cleanup.py -b my-bucket --filter-prefix s3source                  # Filter paths starting with 's3source'
  python cleanup.py --bucket my-bucket --filter-prefix mys3src --dry-run   # Preview deletions for paths starting with 'mys3src'
  python cleanup.py -b my-bucket --filter-prefix s3src/ --delete           # Actually delete objects under 's3src/' path
            """
        )
        
        parser.add_argument(
            '--bucket', '-b',
            required=True,
            help='S3 bucket name (required)'
        )
        
        parser.add_argument(
            '--filter-prefix', '-f',
            default='',
            help='Filter paths by prefix (case-insensitive). This is used to filter the results of the query.'
        )
        
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Preview operations without executing them'
        )
        
        parser.add_argument(
            '--delete',
            action='store_true',
            help='Delete S3 objects (requires AWS credentials)'
        )
        
        return parser.parse_args()
    
    def check_config_file(self):
        """Check if config.yaml exists and provide guidance if not"""
        import os
        
        config_files = ['config.yaml', './config.yaml']
        config_exists = any(os.path.exists(f) for f in config_files)
        
        if not config_exists:
            print("✗ config.yaml not found!")
            print("\nPlease create a config.yaml file with your Dremio Cloud connection details:")
            print("""
# config.yaml example:
hostname: data.dremio.cloud
port: 443
token: your-personal-access-token
tls: true
query: |
  SELECT
    action,
    "timestamp",
    REGEXP_REPLACE(details, '.*"id":"([^"]+)".*', '$1') AS extracted_id,
    REGEXP_REPLACE(details, '.*"name":"([^"]+)".*', '$1') AS extracted_name,
    REPLACE(REPLACE(REGEXP_REPLACE(details, '.*"path":"(.*?)"(?:,|}).*', '$1'), '\\"', '"'), '"', '') AS extracted_path
  FROM SYS.PROJECT.HISTORY.EVENTS
  WHERE event_type = 'TABLE' 
  AND action = 'DELETE'
            """)
            print("\nFor more details, see: https://docs.dremio.com/cloud/sonar/client-apps/python/")
            return False
        return True

    def connect_to_dremio(self):
        """Connect to Dremio Cloud"""
        try:
            # Check if config file exists first
            if not self.check_config_file():
                return None, None
                
            print("Connecting to Dremio Cloud...")
            
            # Save original sys.argv
            original_argv = sys.argv.copy()
            
            # Filter out our custom arguments before calling get_config()
            filtered_argv = [original_argv[0]]  # Keep script name
            
            # Only keep Dremio-compatible arguments
            i = 1
            while i < len(original_argv):
                arg = original_argv[i]
                if arg in ['-config', '--config']:
                    # Keep config argument and its value
                    filtered_argv.append(arg)
                    if i + 1 < len(original_argv):
                        filtered_argv.append(original_argv[i + 1])
                        i += 2
                    else:
                        i += 1
                elif arg.startswith('-config=') or arg.startswith('--config='):
                    # Keep config argument with value
                    filtered_argv.append(arg)
                    i += 1
                elif arg in ['--filter-prefix', '-f', '--dry-run', '--delete', '--bucket', '-b']:
                    # Skip our custom arguments
                    if arg in ['--filter-prefix', '-f', '--bucket', '-b'] and i + 1 < len(original_argv):
                        i += 2  # Skip argument and its value
                    else:
                        i += 1  # Skip flag-only arguments
                else:
                    # Keep unknown arguments (might be Dremio-specific)
                    filtered_argv.append(arg)
                    i += 1
            
            # Temporarily replace sys.argv
            sys.argv = filtered_argv
            
            try:
                args = get_config()
                dremio_endpoint = DremioFlightEndpoint(args)
                self.flight_client = dremio_endpoint.connect()
                print("✓ Connected to Dremio Cloud")
                return dremio_endpoint, args  # Return both endpoint and args
            finally:
                # Restore original sys.argv
                sys.argv = original_argv
                
        except FileNotFoundError as e:
            print(f"✗ Configuration file not found: {e}")
            return None, None
        except Exception as e:
            print(f"✗ Failed to connect to Dremio: {e}")
            print("\nTroubleshooting tips:")
            print("1. Check your config.yaml file has correct credentials")
            print("2. Verify your Dremio Cloud hostname and port")
            print("3. Ensure your token is valid")
            print("4. Check network connectivity to Dremio Cloud")
            return None, None
    
    def setup_s3_client(self):
        """Setup S3 client with default credentials"""
        try:
            print("Setting up S3 client...")
            # Uses default credential chain (env vars, ~/.aws/credentials, IAM roles, etc.)
            self.s3_client = boto3.client('s3')
            
            # Test connection by listing buckets
            self.s3_client.list_buckets()
            print("✓ S3 client configured successfully")
            return True
        except Exception as e:
            print(f"✗ Failed to setup S3 client: {e}")
            print("Ensure AWS credentials are configured via:")
            print("  - Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)")
            print("  - ~/.aws/credentials file")
            print("  - IAM roles (for EC2/Lambda)")
            return False
    
    def query_deleted_tables(self, dremio_endpoint, dremio_args, bucket_name, filter_prefix=''):
        """Query Dremio for deleted tables"""
        try:
            print("Querying deleted tables...")
            
            # Get the query from config and print it
            if hasattr(dremio_args, 'query') and dremio_args.query:
                print(f"\n📋 Executing query:")
                print("-" * 60)
                print(dremio_args.query)
                print("-" * 60)
            else:
                print("\n📋 Using default query from config.yaml")
            
            # Use the reader from config.yaml query
            reader = dremio_endpoint.get_reader(self.flight_client)
            df = reader.read_pandas()
            
            print(f"Retrieved {len(df)} records from Dremio")
            
            # Filter by table name
            if 'extracted_path' in df.columns:
                df['extracted_path'] = df['extracted_path'].astype(str)
                
                # Apply filter to get the tables created in a source
                if filter_prefix:
                    print(f"Filtering paths that start with: '{filter_prefix}'")
                    # Filter table names that start with the specified prefix
                    mask = df['extracted_path'].str.startswith(filter_prefix, na=False)
                    
                    # Print the matching extracted_path values
                    matching_paths = df[mask]['extracted_path'].tolist()
                    print(f"Found {len(matching_paths)} matching paths:")
                    for path in matching_paths:
                        print(f"  - {path}")
                    
                    # Filter the dataframe to only include matching records
                    df = df[mask]
            else:
                print("Warning: 'extracted_path' column not found")
                print("Available columns:", list(df.columns) if not df.empty else "None")
            
            return df
            
        except Exception as e:
            print(f"✗ Query failed: {e}")
            return None
    
    def parse_s3_path(self, s3_path):
        """Parse S3 path into bucket and key"""
        if not s3_path or not s3_path.startswith('s3://'):
            return None, None
        
        parsed = urlparse(s3_path)
        bucket = parsed.netloc
        key = parsed.path.lstrip('/')
        
        return bucket, key
    
    def delete_s3_objects(self, df, bucket_name, dry_run=True):
        """Delete S3 objects recursively based on extracted paths"""
        if df is None or df.empty:
            print("No data to process")
            return
        
        if 'extracted_path' not in df.columns:
            print("No 'extracted_path' column found")
            return
        
        # Get unique extracted paths and append them to the S3 bucket
        extracted_paths = df['extracted_path'].dropna().unique()
        
        if not extracted_paths:
            print("No extracted paths found in the data")
            return
        
        print(f"\nProcessing {len(extracted_paths)} unique extracted paths for bucket '{bucket_name}'")
        
        deleted_count = 0
        error_count = 0
        
        for extracted_path in extracted_paths:
            # Remove everything from start until first "." and append to bucket
            if '.' in extracted_path:
                # Find the first "." and get everything after it
                first_dot_index = extracted_path.find('.')
                s3_prefix = extracted_path[first_dot_index + 1:]
            else:
                # If no "." found, use the full path
                s3_prefix = extracted_path
            
            # Replace remaining "." with "/" to create directory paths
            s3_prefix = s3_prefix.replace('.', '/')
            
            # Construct full S3 path
            s3_path = f"s3://{bucket_name}/{s3_prefix}"
            print(f"\nProcessing: {s3_path}")
            print(f"  Original extracted_path: {extracted_path}")
            print(f"  S3 prefix used: {s3_prefix}")
            
            try:
                # List all objects in the folder recursively
                paginator = self.s3_client.get_paginator('list_objects_v2')
                page_iterator = paginator.paginate(
                    Bucket=bucket_name,
                    Prefix=s3_prefix
                )
                
                objects_to_delete = []
                for page in page_iterator:
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            objects_to_delete.append(obj['Key'])
                
                if not objects_to_delete:
                    print(f"  No objects found in folder: {extracted_path}")
                    continue
                
                print(f"  Found {len(objects_to_delete)} objects to delete")
                
                if dry_run:
                    # Show what would be deleted
                    for obj_key in objects_to_delete:
                        print(f"    [DRY RUN] Would delete: s3://{bucket_name}/{obj_key}")
                    deleted_count += len(objects_to_delete)
                else:
                    # Actually delete the objects
                    for obj_key in objects_to_delete:
                        try:
                            self.s3_client.delete_object(Bucket=bucket_name, Key=obj_key)
                            print(f"    ✓ Deleted: s3://{bucket_name}/{obj_key}")
                            deleted_count += 1
                        except Exception as e:
                            print(f"    ✗ Error deleting s3://{bucket_name}/{obj_key}: {e}")
                            error_count += 1
                    
            except Exception as e:
                print(f"✗ Error processing folder {extracted_path}: {e}")
                error_count += 1
        
        mode = "DRY RUN" if dry_run else "ACTUAL"
        print(f"\n{mode} Summary for bucket '{bucket_name}':")
        print(f"  Total objects processed: {deleted_count}")
        print(f"  Errors: {error_count}")
        
        if dry_run and deleted_count > 0:
            print(f"\nTo actually delete these objects, run with --delete flag")
    
    def save_results(self, df, filter_prefix=''):
        """Save results to CSV"""
        if df is None or df.empty:
            print("No results to save")
            return
        
        filename = "results.csv"
        df.to_csv(filename, index=False)
        print(f"Results saved to: {filename}")
    
    def run(self):
        """Main execution flow"""
        args = self.parse_arguments()
        
        try:
            # Connect to Dremio
            dremio_endpoint, dremio_args = self.connect_to_dremio()
            if not dremio_endpoint:
                return 1
            
            # Setup S3 client if deletion is requested
            if args.delete or args.dry_run:
                if not self.setup_s3_client():
                    return 1
            
            # Query Dremio
            results = self.query_deleted_tables(dremio_endpoint, dremio_args, args.bucket, args.filter_prefix)
            if results is None:
                return 1
            
            # Display results
            print(f"\nQuery Results for bucket '{args.bucket}' ({len(results)} rows):")
            print("=" * 80)
            if not results.empty:
                print(results.to_string(max_rows=10))
                if len(results) > 10:
                    print(f"... and {len(results) - 10} more rows")
            else:
                print("No results found")
            
            # Save results
            self.save_results(results, args.filter_prefix)
            
            # Handle S3 operations
            if args.delete or args.dry_run:
                print("\n" + "=" * 80)
                self.delete_s3_objects(results, args.bucket, dry_run=not args.delete)
            
            return 0
            
        except KeyboardInterrupt:
            print("\n\nOperation cancelled by user")
            return 1
        except Exception as e:
            print(f"Unexpected error: {e}")
            return 1
        finally:
            if self.flight_client:
                self.flight_client.close()
                print("\nDremio connection closed")


def main():
    """Entry point"""
    cleanup_tool = DremioS3Cleanup()
    exit_code = cleanup_tool.run()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()