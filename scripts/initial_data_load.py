#!/usr/bin/env python3
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import INDICATORS, update_indicator_data, init_db
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_all_indicators():
    logger.info("Initializing database...")
    init_db()
    
    total_indicators = sum(len(section.keys()) for section in INDICATORS.values())
    current = 0
    
    logger.info(f"Loading data for {total_indicators} indicators...")
    
    for section_name, section_indicators in INDICATORS.items():
        logger.info(f"Loading {section_name} indicators...")
        
        for series_id in section_indicators.keys():
            current += 1
            logger.info(f"[{current}/{total_indicators}] Loading {series_id}...")
            
            success = update_indicator_data(series_id, force_update=True)
            if success:
                logger.info(f"✓ Successfully loaded {series_id}")
            else:
                logger.error(f"✗ Failed to load {series_id}")
    
    logger.info("Initial data load complete!")

if __name__ == "__main__":
    load_all_indicators()