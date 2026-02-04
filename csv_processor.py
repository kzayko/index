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
    
    def __init__(self, data_dir: str = 'data', state_file: str = 'processing_state.json', 
                 config_file: str = 'parser_config.json', async_kafka: bool = True):
        """
        Initialize CSV processor.
        
        Args:
            data_dir: Directory containing CSV files
            state_file: File to store processing state
            config_file: Configuration file path
            async_kafka: If True, use async Kafka sending for better performance (default: True)
        """
        self.data_dir = Path(data_dir)
        self.state_manager = StateManager(state_file)
        self.producer = KafkaMessageProducer(async_mode=async_kafka)
        self.config = self._load_config(config_file)
    
    def _load_config(self, config_file: str) -> Dict[str, Any]:
        """Load parser configuration from JSON file."""
        config_path = Path(config_file)
        if not config_path.exists():
            logger.warning(f"Configuration file not found: {config_path}, using defaults")
            return {}
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}, using defaults")
            return {}
    
    def decompress_lz4(self, file_path: Path) -> Optional[str]:
        """Decompress lz4 file and return temporary CSV file path."""
        try:
            with open(file_path, 'rb') as f:
                compressed_data = f.read()
            
            decompressed_data = lz4.frame.decompress(compressed_data)
            
            # Create temporary file for CSV in /tmp to avoid read-only filesystem issues
            import tempfile
            temp_dir = tempfile.gettempdir()
            temp_csv = os.path.join(temp_dir, f"{file_path.stem}.csv.tmp")
            with open(temp_csv, 'wb') as f:
                f.write(decompressed_data)
            
            logger.info(f"Decompressed {file_path.name} to {temp_csv}")
            return temp_csv
        except Exception as e:
            logger.error(f"Failed to decompress {file_path}: {e}")
            return None
    
    def csv_row_to_message(self, row: pd.Series, row_index: int) -> Optional[Dict[str, Any]]:
        """
        Convert CSV row to message format (as in message.json).
        
        Uses configuration from parser_config.json to map columns and extract data.
        Row is a pandas Series with integer indices (0, 1, 2, ...) since CSV has no headers.
        """
        try:
            # If config is available, use it
            if self.config and 'csv_columns' in self.config:
                return self._parse_row_with_config(row, row_index)
            else:
                # Fallback to old method
                return self._parse_row_legacy(row, row_index)
        except Exception as e:
            logger.error(f"Error converting row {row_index} to message: {e}", exc_info=True)
            return None
    
    def _parse_row_with_config(self, row: pd.Series, row_index: int) -> Optional[Dict[str, Any]]:
        """Parse row using configuration file."""
        csv_cols = self.config['csv_columns']
        # Column indices are now integers, not column names
        event_type_col_idx = csv_cols['event_type']
        timestamp_col_idx = csv_cols['timestamp']
        user_id_col_idx = csv_cols['user_id']
        event_data_col_idx = csv_cols['event_data']
        
        # Check event type filter
        event_type = str(row.iloc[event_type_col_idx]) if pd.notna(row.iloc[event_type_col_idx]) else ''
        if event_type not in self.config.get('event_type_filter', []):
            return None  # Skip rows that don't match event type filter
        
        # Extract user_id
        user_id = str(row.iloc[user_id_col_idx]) if pd.notna(row.iloc[user_id_col_idx]) else ''
        
        # Extract timestamp
        timestamp = row.iloc[timestamp_col_idx]
        if pd.notna(timestamp):
            try:
                timestamp = int(float(timestamp))
            except (ValueError, TypeError):
                timestamp = int(datetime.now().timestamp())
        else:
            timestamp = int(datetime.now().timestamp())
        
        # Parse event_data JSON
        event_data_str = str(row.iloc[event_data_col_idx]) if pd.notna(row.iloc[event_data_col_idx]) else '{}'
        try:
            event_data = json.loads(event_data_str)
        except json.JSONDecodeError as e:
            logger.warning(f"Row {row_index} - Failed to parse event_data JSON: {e}")
            return None
        
        # Extract required fields from event_data
        event_data_fields = self.config['event_data_fields']
        chat_id = str(event_data.get(event_data_fields['chat_id'], ''))
        message_id = str(event_data.get(event_data_fields['message_id'], ''))
        text = str(event_data.get(event_data_fields['text'], ''))
        
        # Validate required fields
        required = self.config.get('required_fields', ['user_id', 'chat_id', 'message_id', 'text'])
        if not all([user_id, chat_id, message_id, text]):
            logger.warning(f"Row {row_index} missing required fields: user_id={bool(user_id)}, "
                         f"chat_id={bool(chat_id)}, message_id={bool(message_id)}, text={bool(text)}")
            return None
        
        # Build message in format from message.json
        output_format = self.config.get('output_format', {})
        message = {
            "kafkaClientId": output_format.get("kafkaClientId", "csv-processor"),
            "kafkaClientVersion": output_format.get("kafkaClientVersion", "1.0.0"),
            "time": timestamp,
            "event_type": event_type,
            "user_id": user_id,
            "app_name": output_format.get("app_name", "csv"),
            "event_properties": {
                "text": text,
                "message_id": message_id,
                "chat_id": chat_id,
                "model_type": event_data.get('model_type', 'CSV-Import'),
                "linked_message_id": event_data.get('linked_message_id', ''),
                "finish_reason": event_data.get('finish_reason', 'stop'),
                "response_types": event_data.get('response_types', ['TEXT']),
                "conversation_id": event_data.get('conversation_id', ''),
                "external_session_id": event_data.get('external_session_id', ''),
                "bot_id": event_data.get('bot_id', '1'),
                "request_id": event_data.get('request_id', ''),
                "chat_type": event_data.get('chat_type', 'DEFAULT')
            }
        }
        
        return message
    
    def _parse_row_legacy(self, row: pd.Series, row_index: int) -> Optional[Dict[str, Any]]:
        """Legacy parsing method (fallback)."""
        row_dict = row.to_dict()
        user_id = str(row_dict.get('user_id', ''))
        chat_id = str(row_dict.get('chat_id', ''))
        message_id = str(row_dict.get('message_id', ''))
        text = str(row_dict.get('text', ''))
        
        if not all([user_id, chat_id, message_id, text]):
            return None
        
        if not message_id or message_id == 'nan':
            message_id = str(uuid.uuid4())
        
        timestamp = row_dict.get('time')
        if not timestamp or timestamp == 'nan':
            timestamp = int(datetime.now().timestamp())
        else:
            try:
                timestamp = int(float(timestamp))
            except (ValueError, TypeError):
                timestamp = int(datetime.now().timestamp())
        
        message = {
            "kafkaClientId": "csv-processor",
            "kafkaClientVersion": "1.0.0",
            "time": timestamp,
            "event_type": "gigaback_request_generated",
            "user_id": user_id,
            "app_name": row_dict.get('app_name', 'csv'),
            "event_properties": {
                "text": text,
                "model_type": row_dict.get('model_type', 'CSV-Import'),
                "message_id": message_id,
                "linked_message_id": row_dict.get('linked_message_id', ''),
                "chat_id": chat_id,
                "finish_reason": row_dict.get('finish_reason', 'stop'),
                "response_types": ["TEXT"],
                "conversation_id": row_dict.get('conversation_id', ''),
                "external_session_id": row_dict.get('external_session_id', ''),
                "bot_id": row_dict.get('bot_id', '1'),
                "request_id": row_dict.get('request_id', str(uuid.uuid4())),
                "chat_type": row_dict.get('chat_type', 'DEFAULT')
            }
        }
        
        return message
    
    def process_file(self, file_path: Path) -> int:
        """
        Process a single CSV file and send messages to Kafka.
        Processes file in batches of 1000 rows to balance memory usage and performance.
        
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
            
            messages_sent = 0
            row_index = 0
            batch_size = 1000
            
            # Process file in batches of 1000 rows
            try:
                # Use pandas read_csv with chunksize=1000 for batch processing
                # header=None because CSV doesn't have column names
                chunk_iterator = pd.read_csv(
                    csv_path, 
                    header=None, 
                    chunksize=batch_size,
                    iterator=True,
                    dtype=str,  # Read all as strings to avoid type conversion issues
                    keep_default_na=False,  # Don't convert empty strings to NaN
                    na_filter=False  # Don't filter NaN values
                )
                
                for chunk in chunk_iterator:
                    # Skip rows until we reach the resume position
                    if row_index < start_row:
                        # Skip entire chunk if it's before resume position
                        chunk_start = row_index
                        chunk_end = row_index + len(chunk)
                        if chunk_end <= start_row:
                            row_index = chunk_end
                            continue
                        # Otherwise, skip rows within this chunk
                        skip_count = start_row - row_index
                        chunk = chunk.iloc[skip_count:]
                        row_index = start_row
                    
                    # Process batch of rows
                    if len(chunk) == 0:
                        break
                    
                    # Collect messages from this batch
                    batch_messages = []
                    batch_row_indices = []
                    
                    for idx_in_chunk, row in chunk.iterrows():
                        # Convert row to message
                        message = self.csv_row_to_message(row, row_index)
                        
                        if message:
                            batch_messages.append(message)
                            batch_row_indices.append(row_index)
                        else:
                            logger.debug(f"Skipping row {row_index} due to missing required fields")
                        
                        row_index += 1
                    
                    # Send batch to Kafka - use batch send for better performance
                    if batch_messages:
                        batch_sent = self.producer.send_batch(batch_messages)
                        messages_sent += batch_sent
                        
                        # Only check for errors if sync mode (async mode errors are checked in flush)
                        if not self.producer.async_mode and batch_sent < len(batch_messages):
                            logger.error(f"Failed to send {len(batch_messages) - batch_sent} messages in batch")
                            # Save state at the last successfully sent row
                            if batch_sent > 0:
                                self.state_manager.save_state(filename, batch_row_indices[batch_sent - 1] + 1)
                            else:
                                # If all messages failed, save state before this batch
                                self.state_manager.save_state(filename, batch_row_indices[0])
                            return messages_sent
                        
                        # Save state after successful batch
                        # Save at the last processed row in this batch
                        last_row_in_batch = batch_row_indices[-1] + 1
                        self.state_manager.save_state(filename, last_row_in_batch)
                        
                        logger.info(f"Sent batch: {batch_sent} messages from {filename} (rows {batch_row_indices[0]}-{batch_row_indices[-1]}, total: {messages_sent})")
                
                # Mark file as complete if we processed all rows
                if messages_sent > 0:
                    self.state_manager.mark_file_complete(filename)
                    logger.info(f"Completed processing {filename}: {messages_sent} messages sent (total rows processed: {row_index})")
                
            except StopIteration:
                # End of file reached
                if messages_sent > 0:
                    self.state_manager.mark_file_complete(filename)
                    logger.info(f"Completed processing {filename}: {messages_sent} messages sent (total rows processed: {row_index})")
            
            return messages_sent
            
        except Exception as e:
            logger.error(f"Error processing file {filename}: {e}", exc_info=True)
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
