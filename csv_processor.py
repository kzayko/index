"""CSV file processor that converts CSV to Kafka messages with state tracking."""
import os
import json
import csv
import logging
import lz4.frame
import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
import uuid
from kafka_producer import KafkaMessageProducer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StateManager:
    """Manages processing state to resume from last position."""
    
    def __init__(self, state_file: str = 'processing_state.json'):
        """Initialize state manager."""
        self.state_file = state_file
        self.state = self._load_state()
    
    def _load_state(self) -> Dict[str, Any]:
        """Load state from file."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                    logger.info(f"Loaded state: {state}")
                    return state
            except Exception as e:
                logger.warning(f"Failed to load state: {e}, starting fresh")
        return {
            'current_file': None,
            'current_row': 0,
            'processed_files': []
        }
    
    def save_state(self, filename: str, row_index: int):
        """Save current processing state."""
        self.state['current_file'] = filename
        self.state['current_row'] = row_index
        try:
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, indent=2)
            logger.debug(f"State saved: file={filename}, row={row_index}")
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
    
    def mark_file_complete(self, filename: str):
        """Mark a file as completely processed."""
        if filename not in self.state['processed_files']:
            self.state['processed_files'].append(filename)
        self.state['current_file'] = None
        self.state['current_row'] = 0
        self.save_state('', 0)
    
    def is_file_processed(self, filename: str) -> bool:
        """Check if file was already processed completely."""
        return filename in self.state['processed_files']
    
    def get_resume_position(self, filename: str) -> Optional[int]:
        """Get row index to resume from for a file."""
        if self.state['current_file'] == filename:
            return self.state['current_row']
        return None


class CSVProcessor:
    """Processes CSV files and sends messages to Kafka."""
    
    def __init__(self, data_dir: str = 'data', state_file: str = 'processing_state.json'):
        """Initialize CSV processor."""
        self.data_dir = Path(data_dir)
        self.state_manager = StateManager(state_file)
        self.producer = KafkaMessageProducer()
    
    def decompress_lz4(self, file_path: Path) -> Optional[str]:
        """Decompress lz4 file and return temporary CSV file path."""
        try:
            with open(file_path, 'rb') as f:
                compressed_data = f.read()
            
            decompressed_data = lz4.frame.decompress(compressed_data)
            
            # Create temporary file for CSV
            temp_csv = file_path.with_suffix('.csv.tmp')
            with open(temp_csv, 'wb') as f:
                f.write(decompressed_data)
            
            logger.info(f"Decompressed {file_path.name} to {temp_csv.name}")
            return str(temp_csv)
        except Exception as e:
            logger.error(f"Failed to decompress {file_path}: {e}")
            return None
    
    def csv_row_to_message(self, row: Dict[str, Any], row_index: int) -> Optional[Dict[str, Any]]:
        """
        Convert CSV row to message format (as in message.json).
        
        Expected CSV columns (adjust based on your actual CSV structure):
        - user_id
        - chat_id
        - message_id (or generate if missing)
        - text
        - time (optional, or use current timestamp)
        
        Additional fields can be mapped from CSV or set to defaults.
        """
        try:
            # Extract required fields from CSV row
            user_id = str(row.get('user_id', ''))
            chat_id = str(row.get('chat_id', ''))
            message_id = str(row.get('message_id', ''))
            text = str(row.get('text', ''))
            
            # Validate required fields
            if not all([user_id, chat_id, message_id, text]):
                logger.warning(f"Row {row_index} missing required fields: user_id={user_id}, "
                             f"chat_id={chat_id}, message_id={message_id}, text={'present' if text else 'missing'}")
                return None
            
            # Generate message_id if missing
            if not message_id or message_id == 'nan':
                message_id = str(uuid.uuid4())
            
            # Get timestamp from CSV or use current time
            timestamp = row.get('time')
            if not timestamp or timestamp == 'nan':
                timestamp = int(datetime.now().timestamp())
            else:
                try:
                    timestamp = int(float(timestamp))
                except (ValueError, TypeError):
                    timestamp = int(datetime.now().timestamp())
            
            # Build message in format from message.json
            message = {
                "kafkaClientId": "csv-processor",
                "kafkaClientVersion": "1.0.0",
                "time": timestamp,
                "event_type": "gigaback_request_generated",
                "user_id": user_id,
                "app_name": row.get('app_name', 'csv'),
                "event_properties": {
                    "text": text,
                    "model_type": row.get('model_type', 'CSV-Import'),
                    "message_id": message_id,
                    "linked_message_id": row.get('linked_message_id', ''),
                    "chat_id": chat_id,
                    "finish_reason": row.get('finish_reason', 'stop'),
                    "response_types": ["TEXT"] if row.get('response_types') else ["TEXT"],
                    "conversation_id": row.get('conversation_id', ''),
                    "external_session_id": row.get('external_session_id', ''),
                    "bot_id": row.get('bot_id', '1'),
                    "request_id": row.get('request_id', str(uuid.uuid4())),
                    "chat_type": row.get('chat_type', 'DEFAULT')
                }
            }
            
            return message
        except Exception as e:
            logger.error(f"Error converting row {row_index} to message: {e}")
            return None
    
    def process_file(self, file_path: Path) -> int:
        """
        Process a single CSV file and send messages to Kafka.
        
        Returns:
            Number of messages successfully sent
        """
        filename = file_path.name
        logger.info(f"Processing file: {filename}")
        
        # Check if file is already completely processed
        if self.state_manager.is_file_processed(filename):
            logger.info(f"File {filename} already processed, skipping")
            return 0
        
        # Decompress if lz4
        csv_path = file_path
        temp_csv = None
        if file_path.suffix == '.lz4':
            csv_path_str = self.decompress_lz4(file_path)
            if not csv_path_str:
                return 0
            csv_path = Path(csv_path_str)
            temp_csv = csv_path
        
        try:
            # Get resume position
            start_row = self.state_manager.get_resume_position(filename) or 0
            if start_row > 0:
                logger.info(f"Resuming from row {start_row} in {filename}")
            
            # Read CSV file
            messages_sent = 0
            total_rows = 0
            
            # Use pandas for better CSV handling
            df = pd.read_csv(csv_path)
            total_rows = len(df)
            
            logger.info(f"File {filename} has {total_rows} rows, starting from row {start_row}")
            
            # Process rows
            for idx, row in df.iterrows():
                if idx < start_row:
                    continue
                
                # Convert row to message
                message = self.csv_row_to_message(row.to_dict(), idx)
                
                if message:
                    # Send to Kafka
                    if self.producer.send_message(message):
                        messages_sent += 1
                        # Save state after each successful message
                        self.state_manager.save_state(filename, idx + 1)
                        
                        if messages_sent % 100 == 0:
                            logger.info(f"Sent {messages_sent} messages from {filename}")
                    else:
                        logger.error(f"Failed to send message from row {idx}")
                        # Stop on error to preserve state
                        break
                else:
                    logger.debug(f"Skipping row {idx} due to missing required fields")
            
            # Mark file as complete if all rows processed
            if messages_sent > 0 and idx >= total_rows - 1:
                self.state_manager.mark_file_complete(filename)
                logger.info(f"Completed processing {filename}: {messages_sent} messages sent")
            
            return messages_sent
            
        except Exception as e:
            logger.error(f"Error processing file {filename}: {e}")
            return messages_sent
        finally:
            # Clean up temporary file
            if temp_csv and os.path.exists(temp_csv):
                try:
                    os.remove(temp_csv)
                    logger.debug(f"Removed temporary file {temp_csv}")
                except Exception as e:
                    logger.warning(f"Failed to remove temporary file {temp_csv}: {e}")
    
    def process_all_files(self) -> int:
        """Process all CSV files in data directory."""
        if not self.data_dir.exists():
            logger.error(f"Data directory {self.data_dir} does not exist")
            return 0
        
        # Find all lz4 and csv files
        files = list(self.data_dir.glob('*.lz4')) + list(self.data_dir.glob('*.csv'))
        files.sort()  # Process in alphabetical order
        
        if not files:
            logger.warning(f"No CSV or lz4 files found in {self.data_dir}")
            return 0
        
        logger.info(f"Found {len(files)} files to process")
        
        total_messages = 0
        for file_path in files:
            try:
                messages_sent = self.process_file(file_path)
                total_messages += messages_sent
            except Exception as e:
                logger.error(f"Error processing {file_path.name}: {e}")
                continue
        
        # Flush producer to ensure all messages are sent
        self.producer.flush()
        
        logger.info(f"Total messages sent: {total_messages}")
        return total_messages
    
    def close(self):
        """Close producer and cleanup."""
        self.producer.close()


def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Process CSV files and send to Kafka')
    parser.add_argument('--data-dir', default='data', help='Directory containing CSV files')
    parser.add_argument('--state-file', default='processing_state.json', 
                       help='File to store processing state')
    parser.add_argument('--file', help='Process single file instead of all files')
    
    args = parser.parse_args()
    
    processor = CSVProcessor(data_dir=args.data_dir, state_file=args.state_file)
    
    try:
        if args.file:
            # Process single file
            file_path = Path(args.file)
            if not file_path.exists():
                logger.error(f"File not found: {file_path}")
                return 1
            messages_sent = processor.process_file(file_path)
        else:
            # Process all files
            messages_sent = processor.process_all_files()
        
        logger.info(f"Processing complete. Sent {messages_sent} messages to Kafka")
        return 0 if messages_sent > 0 else 1
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 1
    finally:
        processor.close()


if __name__ == "__main__":
    import sys
    sys.exit(main())
