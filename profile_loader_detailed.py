"""Detailed profiling with line-by-line analysis using line_profiler."""
import time
import logging
from pathlib import Path
from csv_processor import CSVProcessor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def profile_with_timing(data_dir: str = 'data', state_file: str = 'profile_state.json', 
                       max_rows: int = 100000, file_name: str = None):
    """Profile with manual timing at different stages."""
    
    timings = {
        'total': 0,
        'file_reading': 0,
        'row_parsing': 0,
        'message_conversion': 0,
        'kafka_sending': 0,
        'state_saving': 0,
        'lz4_decompression': 0,
        'json_parsing': 0,
        'data_extraction': 0
    }
    
    row_count = 0
    message_count = 0
    file_count = 0
    skipped_rows = 0
    
    start_total = time.time()
    
    try:
        processor = CSVProcessor(data_dir=data_dir, state_file=state_file)
        
        data_dir_path = Path(data_dir)
        if file_name:
            # Use specified file
            file_path = data_dir_path / file_name
            if not file_path.exists():
                logger.error(f"File not found: {file_path}")
                return timings
            files = [file_path]
        else:
            # Find first available file
            files = list(data_dir_path.glob('*.lz4')) + list(data_dir_path.glob('*.csv'))
            files.sort()
        
        if not files:
            logger.error("No files found to profile")
            return timings
        
        for file_path in files[:1]:  # Profile first file only
            file_count += 1
            filename = file_path.name
            logger.info(f"Profiling file: {filename} (max {max_rows} rows)")
            
            # Time file reading
            start_read = time.time()
            
            # Decompress if needed
            csv_path = file_path
            temp_csv = None
            if file_path.suffix == '.lz4':
                decompress_start = time.time()
                csv_path_str = processor.decompress_lz4(file_path)
                timings['lz4_decompression'] += time.time() - decompress_start
                if not csv_path_str:
                    continue
                csv_path = Path(csv_path_str)
                temp_csv = csv_path
            
            # Read batches to measure reading time
            import pandas as pd
            import json as json_lib
            chunk_iterator = pd.read_csv(
                csv_path,
                header=None,
                chunksize=1000,
                iterator=True,
                dtype=str,
                keep_default_na=False,
                na_filter=False
            )
            
            # Process batches
            batch_count = 0
            for chunk in chunk_iterator:
                if row_count >= max_rows:
                    logger.info(f"Reached max rows limit: {max_rows}")
                    break
                
                batch_count += 1
                batch_start = time.time()
                
                # Time row parsing
                parse_start = time.time()
                batch_messages = []
                batch_row_indices = []
                
                for idx_in_chunk, row in chunk.iterrows():
                    if row_count >= max_rows:
                        break
                    
                    row_count += 1
                    
                    # Time message conversion
                    convert_start = time.time()
                    
                    # Detailed timing for conversion steps
                    if processor.config and 'csv_columns' in processor.config:
                        csv_cols = processor.config['csv_columns']
                        event_data_col_idx = csv_cols['event_data']
                        
                        # Time JSON parsing
                        json_start = time.time()
                        event_data_str = str(row.iloc[event_data_col_idx]) if pd.notna(row.iloc[event_data_col_idx]) else '{}'
                        try:
                            event_data = json_lib.loads(event_data_str)
                        except json_lib.JSONDecodeError:
                            skipped_rows += 1
                            continue
                        timings['json_parsing'] += time.time() - json_start
                        
                        # Time data extraction
                        extract_start = time.time()
                        event_data_fields = processor.config['event_data_fields']
                        chat_id = str(event_data.get(event_data_fields['chat_id'], ''))
                        message_id = str(event_data.get(event_data_fields['message_id'], ''))
                        text = str(event_data.get(event_data_fields['text'], ''))
                        timings['data_extraction'] += time.time() - extract_start
                    
                    message = processor.csv_row_to_message(row, row_count - 1)
                    timings['message_conversion'] += time.time() - convert_start
                    
                    if message:
                        batch_messages.append(message)
                        batch_row_indices.append(row_count - 1)
                    else:
                        skipped_rows += 1
                
                timings['row_parsing'] += time.time() - parse_start
                
                # Time Kafka sending
                send_start = time.time()
                for message in batch_messages:
                    if processor.producer.send_message(message):
                        message_count += 1
                    else:
                        logger.error("Failed to send message")
                        break
                timings['kafka_sending'] += time.time() - send_start
                
                # Time state saving
                save_start = time.time()
                if batch_row_indices:
                    processor.state_manager.save_state(filename, batch_row_indices[-1] + 1)
                timings['state_saving'] += time.time() - save_start
                
                batch_time = time.time() - batch_start
                logger.info(f"Batch {batch_count}: {len(batch_messages)} messages, {batch_time:.3f}s "
                          f"(rows: {row_count}/{max_rows}, parse: {time.time() - parse_start:.3f}s, "
                          f"send: {time.time() - send_start:.3f}s)")
                
                if row_count >= max_rows:
                    break
            
            timings['file_reading'] += time.time() - start_read
            
            # Cleanup - close file handles first
            if temp_csv and Path(temp_csv).exists():
                import os
                try:
                    # Wait a bit for file handles to close
                    time.sleep(0.1)
                    os.remove(temp_csv)
                except PermissionError:
                    logger.warning(f"Could not remove temp file {temp_csv}, will be cleaned up later")
        
        processor.close()
        timings['total'] = time.time() - start_total
        
        # Print results
        print("\n" + "="*80)
        print("DETAILED TIMING ANALYSIS")
        print("="*80)
        print(f"Files processed: {file_count}")
        print(f"Rows processed: {row_count}")
        print(f"Messages sent: {message_count}")
        print(f"\nTime breakdown:")
        print(f"  Total time:              {timings['total']:.3f}s ({100:.1f}%)")
        print(f"  LZ4 decompression:       {timings['lz4_decompression']:.3f}s ({timings['lz4_decompression']/timings['total']*100:.1f}%)")
        print(f"  File reading:            {timings['file_reading']:.3f}s ({timings['file_reading']/timings['total']*100:.1f}%)")
        print(f"  Row parsing:             {timings['row_parsing']:.3f}s ({timings['row_parsing']/timings['total']*100:.1f}%)")
        print(f"  Message conversion:      {timings['message_conversion']:.3f}s ({timings['message_conversion']/timings['total']*100:.1f}%)")
        print(f"  Kafka sending:           {timings['kafka_sending']:.3f}s ({timings['kafka_sending']/timings['total']*100:.1f}%)")
        print(f"  State saving:            {timings['state_saving']:.3f}s ({timings['state_saving']/timings['total']*100:.1f}%)")
        
        print(f"\nAverage times per operation:")
        if row_count > 0:
            print(f"  Per row parsing:         {timings['row_parsing']/row_count*1000:.3f}ms")
            print(f"  Per message conversion:  {timings['message_conversion']/row_count*1000:.3f}ms")
            if timings['json_parsing'] > 0:
                print(f"  Per JSON parsing:        {timings['json_parsing']/row_count*1000:.3f}ms")
            if timings['data_extraction'] > 0:
                print(f"  Per data extraction:     {timings['data_extraction']/row_count*1000:.3f}ms")
        if message_count > 0:
            print(f"  Per Kafka send:           {timings['kafka_sending']/message_count*1000:.3f}ms")
        
        print(f"\nAdditional statistics:")
        print(f"  Skipped rows:            {skipped_rows}")
        print(f"  Success rate:            {message_count/row_count*100:.1f}%" if row_count > 0 else "  Success rate:            N/A")
        print(f"  Throughput:              {message_count/timings['total']:.1f} messages/sec" if timings['total'] > 0 else "  Throughput:              N/A")
        
        return timings
        
    except Exception as e:
        logger.error(f"Error during profiling: {e}", exc_info=True)
        timings['total'] = time.time() - start_total
        return timings


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Detailed timing profile of CSV processor')
    parser.add_argument('--data-dir', default='data', help='Directory containing CSV files')
    parser.add_argument('--state-file', default='profile_state.json', 
                       help='State file for profiling')
    parser.add_argument('--max-rows', type=int, default=100000,
                       help='Maximum number of rows to process (default: 100000)')
    parser.add_argument('--file', help='Specific file to profile (filename only, not full path)')
    
    args = parser.parse_args()
    profile_with_timing(args.data_dir, args.state_file, args.max_rows, args.file)
