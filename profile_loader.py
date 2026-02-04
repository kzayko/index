"""Profile CSV processor to identify performance bottlenecks."""
import cProfile
import pstats
import io
import logging
from pathlib import Path
from csv_processor import CSVProcessor
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def profile_processor(data_dir: str = 'data', state_file: str = 'profile_state.json'):
    """Profile CSV processor execution."""
    logger.info("Starting profiling...")
    
    # Create profiler
    profiler = cProfile.Profile()
    
    # Start profiling
    profiler.enable()
    start_time = time.time()
    
    try:
        processor = CSVProcessor(data_dir=data_dir, state_file=state_file)
        
        # Process all files
        messages_sent = processor.process_all_files()
        
        # Flush and close
        processor.close()
        
        logger.info(f"Processing complete. Sent {messages_sent} messages")
        
    except Exception as e:
        logger.error(f"Error during profiling: {e}", exc_info=True)
    finally:
        # Stop profiling
        profiler.disable()
        end_time = time.time()
        
        logger.info(f"Total execution time: {end_time - start_time:.2f} seconds")
        
        # Generate statistics
        stats_stream = io.StringIO()
        stats = pstats.Stats(profiler, stream=stats_stream)
        stats.sort_stats('cumulative')
        
        # Print top 50 functions by cumulative time
        logger.info("\n" + "="*80)
        logger.info("TOP 50 FUNCTIONS BY CUMULATIVE TIME")
        logger.info("="*80)
        stats.print_stats(50)
        
        # Print top 50 functions by total time
        logger.info("\n" + "="*80)
        logger.info("TOP 50 FUNCTIONS BY TOTAL TIME")
        logger.info("="*80)
        stats.sort_stats('tottime')
        stats.print_stats(50)
        
        # Save detailed stats to file
        stats_file = 'profile_stats.txt'
        with open(stats_file, 'w', encoding='utf-8') as f:
            stats = pstats.Stats(profiler, stream=f)
            stats.sort_stats('cumulative')
            stats.print_stats()
        
        logger.info(f"\nDetailed profile saved to: {stats_file}")
        
        # Print summary statistics
        print("\n" + "="*80)
        print("PROFILING SUMMARY")
        print("="*80)
        print(f"Total execution time: {end_time - start_time:.2f} seconds")
        print(f"Total function calls: {stats.total_calls}")
        print(f"Primitive calls: {stats.prim_calls}")
        print(f"Total time in functions: {stats.total_tt:.4f} seconds")
        print(f"Total cumulative time: {stats.total_cumtime:.4f} seconds")
        
        # Get top time consumers
        print("\n" + "="*80)
        print("TOP 10 TIME CONSUMERS (by cumulative time)")
        print("="*80)
        stats.sort_stats('cumulative')
        stats.print_stats(10)
        
        return stats_stream.getvalue()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Profile CSV processor')
    parser.add_argument('--data-dir', default='data', help='Directory containing CSV files')
    parser.add_argument('--state-file', default='profile_state.json', 
                       help='State file for profiling (separate from production)')
    parser.add_argument('--file', help='Profile single file instead of all files')
    
    args = parser.parse_args()
    
    if args.file:
        # Profile single file
        file_path = Path(args.file)
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            exit(1)
        
        logger.info(f"Profiling single file: {file_path}")
        profiler = cProfile.Profile()
        profiler.enable()
        start_time = time.time()
        
        try:
            processor = CSVProcessor(data_dir=str(file_path.parent), state_file=args.state_file)
            messages_sent = processor.process_file(file_path)
            processor.close()
            logger.info(f"Sent {messages_sent} messages")
        except Exception as e:
            logger.error(f"Error: {e}", exc_info=True)
        finally:
            profiler.disable()
            end_time = time.time()
            
            stats = pstats.Stats(profiler)
            stats.sort_stats('cumulative')
            
            print("\n" + "="*80)
            print(f"PROFILING RESULTS for {file_path.name}")
            print("="*80)
            print(f"Total time: {end_time - start_time:.2f} seconds")
            print(f"Messages sent: {messages_sent}")
            print("\nTop 20 functions by cumulative time:")
            stats.print_stats(20)
    else:
        # Profile all files
        profile_processor(args.data_dir, args.state_file)
