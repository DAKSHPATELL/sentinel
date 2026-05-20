"""
SENTINEL LanceDB client.
Vector database for content embeddings, entity embeddings, and page snapshots.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import structlog

from sentinel.config import get_config

logger = structlog.get_logger(__name__)


class LanceDBClient:
    """LanceDB wrapper for vector storage and similarity search."""

    def __init__(self) -> None:
        """Initialize LanceDB client (call connect() to open database)."""
        self._db: Any = None
        self._tables: dict[str, Any] = {}

    async def connect(self) -> None:
        """Open LanceDB database and initialize tables."""
        import lancedb

        config = get_config()
        db_path = Path(config.lancedb.path)
        db_path.mkdir(parents=True, exist_ok=True)

        self._db = lancedb.connect(str(db_path))
        logger.info("lancedb_connected", path=str(db_path))

    @property
    def db(self) -> Any:
        """Get the database connection."""
        if self._db is None:
            raise RuntimeError("LanceDB not connected. Call connect() first.")
        return self._db

    async def close(self) -> None:
        """Close the database (LanceDB doesn't require explicit close)."""
        self._db = None
        self._tables.clear()
        logger.info("lancedb_disconnected")

    def _get_or_create_table(self, name: str, schema: list[dict[str, Any]]) -> Any:
        """Get existing table or create with schema."""
        if name in self._tables:
            return self._tables[name]

        try:
            table = self.db.open_table(name)
            self._tables[name] = table
            return table
        except Exception:
            # Table doesn't exist, create it
            import pyarrow as pa

            fields = []
            for col in schema:
                col_name = col["name"]
                col_type = col["type"]
                if col_type == "string":
                    fields.append(pa.field(col_name, pa.string()))
                elif col_type == "float32":
                    fields.append(pa.field(col_name, pa.float32()))
                elif col_type == "float64":
                    fields.append(pa.field(col_name, pa.float64()))
                elif col_type == "int32":
                    fields.append(pa.field(col_name, pa.int32()))
                elif col_type == "int64":
                    fields.append(pa.field(col_name, pa.int64()))
                elif col_type.startswith("vector"):
                    dim = int(col_type.split("[")[1].rstrip("]"))
                    fields.append(pa.field(col_name, pa.list_(pa.float32(), dim)))
                elif col_type == "timestamp":
                    fields.append(pa.field(col_name, pa.string()))  # Store as ISO string
                else:
                    fields.append(pa.field(col_name, pa.string()))

            pa_schema = pa.schema(fields)
            table = self.db.create_table(name, schema=pa_schema)
            self._tables[name] = table
            logger.info("lancedb_table_created", table=name)
            return table

    async def initialize_tables(self) -> None:
        """Initialize all required tables per PRD Section 4.3.2."""
        config = get_config()
        dim = config.lancedb.embedding_dim

        self._get_or_create_table("content_embeddings", [
            {"name": "id", "type": "string"},
            {"name": "url", "type": "string"},
            {"name": "title", "type": "string"},
            {"name": "source_type", "type": "string"},
            {"name": "vector", "type": f"vector[{dim}]"},
            {"name": "timestamp", "type": "string"},
        ])

        self._get_or_create_table("entity_embeddings", [
            {"name": "id", "type": "string"},
            {"name": "name", "type": "string"},
            {"name": "entity_type", "type": "string"},
            {"name": "vector", "type": f"vector[{dim}]"},
        ])

        self._get_or_create_table("page_snapshots", [
            {"name": "url", "type": "string"},
            {"name": "vector", "type": f"vector[{dim}]"},
            {"name": "content_hash", "type": "string"},
            {"name": "snapshot_at", "type": "string"},
        ])

        self._get_or_create_table("paragraph_embeddings", [
            {"name": "id", "type": "string"},
            {"name": "content_id", "type": "string"},
            {"name": "url", "type": "string"},
            {"name": "paragraph_index", "type": "int32"},
            {"name": "text", "type": "string"},
            {"name": "vector", "type": f"vector[{dim}]"},
            {"name": "extracted_at", "type": "string"},
        ])

        self._get_or_create_table("relationship_embeddings", [
            {"name": "source_id", "type": "string"},
            {"name": "target_id", "type": "string"},
            {"name": "relationship_type", "type": "string"},
            {"name": "text", "type": "string"},
            {"name": "vector", "type": f"vector[{dim}]"},
            {"name": "first_seen", "type": "string"},
        ])

        logger.info("lancedb_tables_initialized")

    async def insert(self, table_name: str, rows: list[dict[str, Any]]) -> None:
        """Alias for add_rows — used by GraphBuilder."""
        await self.add_rows(table_name, rows)

    async def add_rows(self, table_name: str, rows: list[dict[str, Any]]) -> None:
        """
        Add rows to a table.

        Args:
            table_name: Name of the table.
            rows: List of row dictionaries.
        """
        try:
            table = self.db.open_table(table_name)
            table.add(rows)
            logger.debug("lancedb_rows_added", table=table_name, count=len(rows))
        except Exception as e:
            logger.error("lancedb_add_failed", table=table_name, error=str(e))
            raise

    async def search(
        self,
        table_name: str,
        query_vector: list[float],
        limit: int = 10,
        filter_expr: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """
        Search for similar vectors.

        Args:
            table_name: Table to search.
            query_vector: Query embedding vector.
            limit: Number of results.
            filter_expr: Optional filter expression.

        Returns:
            List of matching rows with distance scores.
        """
        try:
            table = self.db.open_table(table_name)
            query = table.search(query_vector).limit(limit)
            if filter_expr:
                query = query.where(filter_expr)
            results = query.to_list()
            return results
        except Exception as e:
            logger.error("lancedb_search_failed", table=table_name, error=str(e))
            return []

    async def delete_rows(self, table_name: str, filter_expr: str) -> None:
        """
        Delete rows matching a filter expression.

        Args:
            table_name: Table to delete from.
            filter_expr: Filter expression for rows to delete.
        """
        try:
            table = self.db.open_table(table_name)
            table.delete(filter_expr)
            logger.debug("lancedb_rows_deleted", table=table_name, filter=filter_expr)
        except Exception as e:
            logger.error("lancedb_delete_failed", table=table_name, error=str(e))
            raise

    async def get_table_stats(self, table_name: str) -> dict[str, Any]:
        """Get statistics for a table."""
        try:
            table = self.db.open_table(table_name)
            return {
                "name": table_name,
                "row_count": table.count_rows(),
            }
        except Exception as e:
            return {"name": table_name, "row_count": 0, "error": str(e)}
