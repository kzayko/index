#!/usr/bin/env python3
"""Test script to parse LZ4 CSV file and extract JSON messages."""
import lz4.frame
import pandas as pd
import json
import sys
from pathlib import Path
from typing import Dict, Any, Optional, List

# Load configuration
def load_config(config_path: Path = Path('parser_config.json')) -> Dict[str, Any]:
    """Load parser configuration from JSON file."""
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def parse_row_to_message(row: pd.Series, config: Dict[str, Any], row_index: int) -> Optional[Dict[str, Any]]:
    """
    Parse a CSV row into Kafka message format.
    
    Args:
        row: pandas Series representing a CSV row
        config: Configuration dictionary
        row_index: Row index for logging
    
    Returns:
        Message dictionary or None if row doesn't match requirements
    """
    try:
        # Get column mappings from config
        csv_cols = config['csv_columns']
        event_type_col = csv_cols['event_type']
        timestamp_col = csv_cols['timestamp']
        user_id_col = csv_cols['user_id']
        event_data_col = csv_cols['event_data']
        
        # Check event type filter
        event_type = str(row[event_type_col]) if pd.notna(row[event_type_col]) else ''
        if event_type not in config['event_type_filter']:
            return None  # Skip rows that don't match event type filter
        
        # Extract user_id
        user_id = str(row[user_id_col]) if pd.notna(row[user_id_col]) else ''
        
        # Extract timestamp
        timestamp = row[timestamp_col]
        if pd.notna(timestamp):
            try:
                timestamp = int(float(timestamp))
            except (ValueError, TypeError):
                timestamp = None
        else:
            timestamp = None
        
        # Parse event_data JSON
        event_data_str = str(row[event_data_col]) if pd.notna(row[event_data_col]) else '{}'
        try:
            event_data = json.loads(event_data_str)
        except json.JSONDecodeError as e:
            print(f"Warning: Row {row_index} - Failed to parse event_data JSON: {e}")
            return None
        
        # Extract required fields from event_data
        event_data_fields = config['event_data_fields']
        chat_id = str(event_data.get(event_data_fields['chat_id'], ''))
        message_id = str(event_data.get(event_data_fields['message_id'], ''))
        text = str(event_data.get(event_data_fields['text'], ''))
        
        # Validate required fields
        required = config['required_fields']
        if not all([user_id, chat_id, message_id, text]):
            missing = [f for f in required if not locals().get(f)]
            print(f"Warning: Row {row_index} - Missing required fields: {missing}")
            return None
        
        # Build message in format from message.json
        output_format = config['output_format']
        message = {
            "kafkaClientId": output_format.get("kafkaClientId", "csv-processor"),
            "kafkaClientVersion": output_format.get("kafkaClientVersion", "1.0.0"),
            "time": timestamp or 0,
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
        
    except Exception as e:
        print(f"Error parsing row {row_index}: {e}")
        import traceback
        traceback.print_exc()
        return None


def parse_lz4_file(input_file: Path, config_file: Path, output_file: Path = None) -> List[Dict[str, Any]]:
    """
    Parse LZ4 compressed CSV file and extract messages.
    
    Args:
        input_file: Path to LZ4 compressed CSV file
        config_file: Path to configuration JSON file
        output_file: Optional path to write output JSON file
    
    Returns:
        List of parsed messages
    """
    print(f"Loading configuration from {config_file}...")
    config = load_config(config_file)
    
    print(f"Decompressing {input_file.name}...")
    with open(input_file, 'rb') as f:
        compressed_data = f.read()
    
    decompressed_data = lz4.frame.decompress(compressed_data)
    
    # Write temporary CSV
    import tempfile
    temp_csv = Path(tempfile.gettempdir()) / f"{input_file.stem}_temp.csv"
    with open(temp_csv, 'wb') as f:
        f.write(decompressed_data)
    
    print(f"Reading CSV file...")
    df = pd.read_csv(temp_csv)
    
    print(f"Found {len(df)} rows and {len(df.columns)} columns")
    print(f"Filtering for event types: {config['event_type_filter']}")
    
    # Parse rows
    messages = []
    csv_cols = config['csv_columns']
    event_type_col = csv_cols['event_type']
    
    # Count rows by event type
    if event_type_col in df.columns:
        event_counts = df[event_type_col].value_counts()
        print(f"\nEvent type distribution:")
        for event, count in event_counts.items():
            print(f"  {event}: {count} rows")
    
    print(f"\nParsing rows...")
    for idx, row in df.iterrows():
        message = parse_row_to_message(row, config, idx)
        if message:
            messages.append(message)
        
        if (idx + 1) % 1000 == 0:
            print(f"  Processed {idx + 1} rows, found {len(messages)} messages")
    
    print(f"\n✓ Parsed {len(messages)} messages from {len(df)} rows")
    
    # Write output JSON
    if output_file:
        print(f"Writing output to {output_file}...")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(messages, f, indent=2, ensure_ascii=False)
        print(f"✓ Written {len(messages)} messages to {output_file}")
        print(f"  File size: {output_file.stat().st_size / 1024 / 1024:.2f} MB")
    
    # Cleanup
    if temp_csv.exists():
        temp_csv.unlink()
    
    return messages


def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Parse LZ4 CSV file and extract JSON messages')
    parser.add_argument('input_file', help='Input LZ4 compressed CSV file')
    parser.add_argument('-c', '--config', default='parser_config.json', help='Configuration JSON file')
    parser.add_argument('-o', '--output', help='Output JSON file (default: input_file with .json extension)')
    
    args = parser.parse_args()
    
    input_file = Path(args.input_file)
    config_file = Path(args.config)
    output_file = Path(args.output) if args.output else input_file.with_suffix('.json')
    
    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        sys.exit(1)
    
    try:
        messages = parse_lz4_file(input_file, config_file, output_file)
        
        # Print summary
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"Total messages parsed: {len(messages)}")
        if messages:
            print(f"\nSample message:")
            print(json.dumps(messages[0], indent=2, ensure_ascii=False))
        
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
