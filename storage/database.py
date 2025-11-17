"""Database module for Meilisearch storage."""

import meilisearch
import logging
from typing import List, Dict, Any, Optional, Set
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


class TorrentDatabase:
    """Meilisearch database manager for torrent metadata."""
    
    def __init__(self, url: str, api_key: Optional[str], index_name: str):
        self.url = url
        self.api_key = api_key
        self.index_name = index_name
        self.client = None
        self.index = None
        self.init_database()
    
    def init_database(self):
        """Initialize Meilisearch client and index."""
        try:
            self.client = meilisearch.Client(self.url, self.api_key)
            
            # Create or get index
            try:
                self.index = self.client.get_index(self.index_name)
                logger.info(f"Connected to existing index: {self.index_name}")
            except meilisearch.errors.MeilisearchApiError:
                # Index doesn't exist, create it
                task = self.client.create_index(self.index_name, {'primaryKey': 'info_hash'})
                self.client.wait_for_task(task.task_uid)
                self.index = self.client.get_index(self.index_name)
                logger.info(f"Created new index: {self.index_name}")
            
            self.configure_index()
            logger.info(f"Meilisearch initialized at {self.url}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Meilisearch: {e}", exc_info=True)
            raise
    
    def configure_index(self):
        """Configure index settings for optimal search."""
        try:
            # Configure searchable attributes
            self.index.update_searchable_attributes([
                'name',
                'info_hash',
                'files.path',
                'directories',
                'comment'
            ])
            
            # Configure filterable attributes
            self.index.update_filterable_attributes([
                'total_size',
                'num_files',
                'num_directories',
                'directories',
                'collected_at',
                'inserted_at'
            ])
            
            # Configure sortable attributes
            self.index.update_sortable_attributes([
                'total_size',
                'num_files',
                'collected_at',
                'inserted_at'
            ])
            
            # Configure displayed attributes
            self.index.update_displayed_attributes([
                'info_hash',
                'name',
                'total_size',
                'num_files',
                'num_directories',
                'files',
                'directories',
                'piece_length',
                'num_pieces',
                'comment',
                'created_date',
                'collected_at',
                'inserted_at'
            ])
            
            logger.info("Index configuration updated")
            
        except Exception as e:
            logger.error(f"Failed to configure index: {e}", exc_info=True)
            # Non-fatal error, continue
    
    def extract_directories(self, files: List[Dict[str, Any]]) -> List[str]:
        """Extract unique directory paths from file list.
        
        Args:
            files: List of file dicts with 'path' field
            
        Returns:
            List of unique directory paths
        """
        directories: Set[str] = set()
        
        for file_info in files:
            file_path = file_info.get('path', '')
            if file_path:
                # Extract directory using pathlib
                path_obj = Path(file_path)
                
                # Add all parent directories
                parts = path_obj.parts
                if len(parts) > 1:  # Has directory
                    # Add all directory levels
                    for i in range(len(parts) - 1):  # Exclude filename
                        dir_path = str(Path(*parts[:i+1]))
                        directories.add(dir_path)
        
        return sorted(list(directories))
    
    def insert_torrent(
        self,
        info_hash: str,
        name: str,
        total_size: int,
        files: List[Dict[str, Any]],
        piece_length: Optional[int] = None,
        num_pieces: Optional[int] = None,
        comment: Optional[str] = None,
        created_date: Optional[str] = None,
        collected_at: Optional[str] = None
    ) -> bool:
        """
        Insert torrent and its files into Meilisearch.
        
        Args:
            info_hash: Torrent info hash (primary key)
            name: Torrent name
            total_size: Total size in bytes
            files: List of file dicts with 'path' and 'size'
            piece_length: Piece length in bytes
            num_pieces: Number of pieces
            comment: Torrent comment
            created_date: Creation date ISO string
            collected_at: Collection timestamp ISO string
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Extract directory information
            directories = self.extract_directories(files)
            
            # Prepare document for Meilisearch
            document = {
                'info_hash': info_hash,
                'name': name,
                'total_size': total_size,
                'num_files': len(files),
                'num_directories': len(directories),
                'files': files,  # Store as array of objects
                'directories': directories,  # Store extracted directories
                'inserted_at': datetime.utcnow().isoformat(),
                'collected_at': collected_at or datetime.utcnow().isoformat()
            }
            
            # Add optional fields
            if piece_length is not None:
                document['piece_length'] = piece_length
            if num_pieces is not None:
                document['num_pieces'] = num_pieces
            if comment:
                document['comment'] = comment
            if created_date:
                document['created_date'] = created_date
            
            # Add document to Meilisearch
            task = self.index.add_documents([document])
            
            # Wait for task to complete (optional, for immediate feedback)
            result = self.client.wait_for_task(task.task_uid, timeout_in_ms=5000)
            
            if result.status == 'succeeded':
                logger.info(f"Inserted torrent {info_hash}: {name} ({len(files)} files)")
                return True
            else:
                logger.error(f"Failed to insert torrent {info_hash}: {result.error}")
                return False
            
        except Exception as e:
            logger.error(f"Failed to insert torrent {info_hash}: {e}", exc_info=True)
            return False
    
    def torrent_exists(self, info_hash: str) -> bool:
        """
        Check if torrent already exists in Meilisearch.
        
        Args:
            info_hash: Torrent info hash
            
        Returns:
            True if exists, False otherwise
        """
        try:
            document = self.index.get_document(info_hash)
            return document is not None
        except meilisearch.errors.MeilisearchApiError as e:
            if e.code == 'document_not_found':
                return False
            logger.error(f"Error checking torrent existence: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Error checking torrent existence: {e}", exc_info=True)
            return False
    
    def get_torrent_count(self) -> int:
        """Get total number of torrents in Meilisearch index."""
        try:
            stats = self.index.get_stats()
            return stats.number_of_documents
        except Exception as e:
            logger.error(f"Error getting torrent count: {e}", exc_info=True)
            return 0
    
    def close(self):
        """Close Meilisearch connection (not needed, but kept for compatibility)."""
        logger.info("Meilisearch connection closed (no-op)")
