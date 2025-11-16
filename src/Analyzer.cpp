// src/Analyzer.cpp
#include "Analyzer.h"
#include "ValueVisitor.h"
#include <sqlite3.h>
#include <iostream>
#include <shared_mutex>
#include <mutex>
#include <atomic>

namespace sqlite_flux
{

	// Pimpl idiom implementation with thread-safety
	struct Analyzer::Impl
	{
		sqlite3* db = nullptr;
		std::string lastError;

		// Thread-safety primitives
		mutable std::mutex dbMutex_;              // Protects database operations
		mutable std::shared_mutex schemaMutex_;   // Protects schema cache (read-write)

		// Schema cache (protected by schemaMutex_)
		std::unordered_map<std::string, TableSchema> schemaCache;
		std::atomic<bool> isCacheInitialized{ false };  // Lock-free flag
		std::atomic<bool> walModeEnabled{ false };      // WAL mode status

		~Impl()
		{
			if (db)
			{
				sqlite3_close(db);
			} // end of if
		} // end of destructor

		ColumnValue getColumnValue(sqlite3_stmt* stmt, int col)
		{
			int type = sqlite3_column_type(stmt, col);

			switch (type)
			{
			case SQLITE_INTEGER:
				return sqlite3_column_int64(stmt, col);
			case SQLITE_FLOAT:
				return sqlite3_column_double(stmt, col);
			case SQLITE_TEXT:
				return std::string(reinterpret_cast<const char*>(sqlite3_column_text(stmt, col)));
			case SQLITE_BLOB:
			{
				const void* blob = sqlite3_column_blob(stmt, col);
				int size = sqlite3_column_bytes(stmt, col);
				const uint8_t* data = static_cast<const uint8_t*>(blob);
				return std::vector<uint8_t>(data, data + size);
			} // end of case SQLITE_BLOB
			case SQLITE_NULL:
			default:
				return std::monostate{};
			} // end of switch
		} // end of getColumnValue
	}; // end of struct Impl

	Analyzer::Analyzer(const std::string& dbPath)
		: pImpl_(std::make_unique<Impl>())
	{
		open(dbPath);
	} // end of Analyzer constructor

	Analyzer::~Analyzer() = default;

	Analyzer::Analyzer(Analyzer&&) noexcept = default;
	Analyzer& Analyzer::operator=(Analyzer&&) noexcept = default;

	bool Analyzer::open(const std::string& dbPath)
	{
		close();

		std::lock_guard lock(pImpl_->dbMutex_);  // Thread-safe

		int rc = sqlite3_open(dbPath.c_str(), &pImpl_->db);
		if (rc != SQLITE_OK)
		{
			pImpl_->lastError = sqlite3_errmsg(pImpl_->db);
			sqlite3_close(pImpl_->db);
			pImpl_->db = nullptr;
			return false;
		} // end of if

		// Enable WAL mode directly (we already hold the mutex)
		if (pImpl_->db)
		{
			char* errMsg = nullptr;
			int rc = sqlite3_exec(pImpl_->db, "PRAGMA journal_mode=WAL", nullptr, nullptr, &errMsg);

			if (rc == SQLITE_OK)
			{
				sqlite3_exec(pImpl_->db, "PRAGMA synchronous=NORMAL", nullptr, nullptr, nullptr);
				sqlite3_exec(pImpl_->db, "PRAGMA busy_timeout=5000", nullptr, nullptr, nullptr);
				pImpl_->walModeEnabled.store(true, std::memory_order_release);
			} // end of if
			else
			{
				sqlite3_free(errMsg);
			} // end of else
		} // end of if

		return true;
	} // end of open

	void Analyzer::close()
	{
		std::lock_guard lock(pImpl_->dbMutex_);  // Thread-safe

		if (pImpl_->db)
		{
			sqlite3_close(pImpl_->db);
			pImpl_->db = nullptr;
		} // end of if
	} // end of close

	bool Analyzer::isOpen() const
	{
		std::lock_guard lock(pImpl_->dbMutex_);  // Thread-safe
		return pImpl_->db != nullptr;
	} // end of isOpen

	bool Analyzer::enableWALMode()
	{
		std::lock_guard lock(pImpl_->dbMutex_);  // Thread-safe

		if (!pImpl_->db) return false;

		// Enable WAL mode for better concurrency
		char* errMsg = nullptr;
		int rc = sqlite3_exec(pImpl_->db, "PRAGMA journal_mode=WAL", nullptr, nullptr, &errMsg);

		if (rc != SQLITE_OK)
		{
			pImpl_->lastError = errMsg ? errMsg : "Failed to enable WAL mode";
			sqlite3_free(errMsg);
			return false;
		} // end of if

		// Set synchronous to NORMAL for better performance
		sqlite3_exec(pImpl_->db, "PRAGMA synchronous=NORMAL", nullptr, nullptr, nullptr);

		// Set busy timeout to 5 seconds
		sqlite3_exec(pImpl_->db, "PRAGMA busy_timeout=5000", nullptr, nullptr, nullptr);

		pImpl_->walModeEnabled.store(true, std::memory_order_release);
		return true;
	} // end of enableWALMode

	bool Analyzer::isWALMode() const
	{
		return pImpl_->walModeEnabled.load(std::memory_order_acquire);
	} // end of isWALMode

	std::vector<std::string> Analyzer::getTableNames() const
	{
		std::lock_guard lock(pImpl_->dbMutex_);  // Thread-safe

		std::vector<std::string> tables;
		if (!pImpl_->db) return tables;

		const char* query = "SELECT name FROM sqlite_master "
			"WHERE type='table' AND name NOT LIKE 'sqlite_%'";

		sqlite3_stmt* stmt;
		if (sqlite3_prepare_v2(pImpl_->db, query, -1, &stmt, nullptr) == SQLITE_OK)
		{
			while (sqlite3_step(stmt) == SQLITE_ROW)
			{
				tables.emplace_back(reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0)));
			} // end of while
			sqlite3_finalize(stmt);
		} // end of if

		return tables;
	} // end of getTableNames

	std::vector<std::string> Analyzer::getColumnNames(const std::string& tableName) const
	{
		std::lock_guard lock(pImpl_->dbMutex_);  // Thread-safe

		std::vector<std::string> columns;
		if (!pImpl_->db) return columns;

		std::string query = "PRAGMA table_info(" + tableName + ")";

		sqlite3_stmt* stmt;
		if (sqlite3_prepare_v2(pImpl_->db, query.c_str(), -1, &stmt, nullptr) == SQLITE_OK)
		{
			while (sqlite3_step(stmt) == SQLITE_ROW)
			{
				columns.emplace_back(reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1)));
			} // end of while
			sqlite3_finalize(stmt);
		} // end of if

		return columns;
	} // end of getColumnNames

	TableSchema Analyzer::getTableSchema(const std::string& tableName) const
	{
		// Quick atomic check (no lock needed)
		if (pImpl_->isCacheInitialized.load(std::memory_order_acquire))
		{
			// Try to read from cache with shared lock (concurrent reads allowed)
			std::shared_lock lock(pImpl_->schemaMutex_);
			auto it = pImpl_->schemaCache.find(tableName);
			if (it != pImpl_->schemaCache.end())
			{
				return it->second;
			} // end of if
		} // end of if

		// Not in cache, fetch from database with exclusive lock
		std::unique_lock cacheLock(pImpl_->schemaMutex_);  // Exclusive for write
		std::lock_guard dbLock(pImpl_->dbMutex_);          // Serialize DB access

		TableSchema schema;
		if (!pImpl_->db) return schema;

		std::string query = "PRAGMA table_info(" + tableName + ")";

		sqlite3_stmt* stmt;
		if (sqlite3_prepare_v2(pImpl_->db, query.c_str(), -1, &stmt, nullptr) == SQLITE_OK)
		{
			while (sqlite3_step(stmt) == SQLITE_ROW)
			{
				ColumnInfo info;
				info.name = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
				info.type = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));
				info.notNull = sqlite3_column_int(stmt, 3) != 0;
				info.primaryKey = sqlite3_column_int(stmt, 5) != 0;
				schema.push_back(info);
			} // end of while
			sqlite3_finalize(stmt);
		} // end of if

		// Cache it for next time
		pImpl_->schemaCache[tableName] = schema;

		return schema;
	} // end of getTableSchema

	ResultSet Analyzer::query(const std::string& sql) const
	{
		std::lock_guard lock(pImpl_->dbMutex_);  // Thread-safe: serialize all DB operations

		ResultSet results;
		if (!pImpl_->db) return results;

		sqlite3_stmt* stmt;

		if (sqlite3_prepare_v2(pImpl_->db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK)
		{
			const_cast<Impl*>(pImpl_.get())->lastError = sqlite3_errmsg(pImpl_->db);
			return results;
		} // end of if

		int columnCount = sqlite3_column_count(stmt);

		while (sqlite3_step(stmt) == SQLITE_ROW)
		{
			Row row;
			for (int i = 0; i < columnCount; ++i)
			{
				std::string columnName = sqlite3_column_name(stmt, i);
				row[columnName] = pImpl_->getColumnValue(stmt, i);
			} // end of for
			results.push_back(std::move(row));
		} // end of while

		sqlite3_finalize(stmt);
		return results;
	} // end of query

	ResultSet Analyzer::selectAll(const std::string& tableName) const
	{
		return query("SELECT * FROM " + tableName);
	} // end of selectAll

	ResultSet Analyzer::selectWhere(const std::string& tableName, const std::string& whereClause) const
	{
		return query("SELECT * FROM " + tableName + " WHERE " + whereClause);
	} // end of selectWhere

	bool Analyzer::execute(const std::string& sql)
	{
		std::lock_guard lock(pImpl_->dbMutex_);  // Thread-safe

		if (!pImpl_->db) return false;

		char* errMsg = nullptr;
		int rc = sqlite3_exec(pImpl_->db, sql.c_str(), nullptr, nullptr, &errMsg);

		if (rc != SQLITE_OK)
		{
			pImpl_->lastError = errMsg ? errMsg : "Unknown error";
			sqlite3_free(errMsg);
			return false;
		} // end of if

		return true;
	} // end of execute

	bool Analyzer::beginTransaction()
	{
		return execute("BEGIN TRANSACTION");
	} // end of beginTransaction

	bool Analyzer::commit()
	{
		return execute("COMMIT");
	} // end of commit

	bool Analyzer::rollback()
	{
		return execute("ROLLBACK");
	} // end of rollback

	std::string Analyzer::getLastError() const
	{
		std::lock_guard lock(pImpl_->dbMutex_);  // Thread-safe
		return pImpl_->lastError;
	} // end of getLastError

	std::optional<int64_t> Analyzer::getRowCount(const std::string& tableName) const
	{
		auto result = query("SELECT COUNT(*) as count FROM " + tableName);
		if (result.empty()) return std::nullopt;

		return getValue<int64_t>(result[0], "count");
	} // end of getRowCount

	void Analyzer::cacheAllSchemas()
	{
		std::unique_lock lock(pImpl_->schemaMutex_);  // Exclusive lock for write
		std::lock_guard dbLock(pImpl_->dbMutex_);     // Serialize DB access

		if (!pImpl_->db) return;

		// Get all table names
		std::vector<std::string> tables;
		const char* query = "SELECT name FROM sqlite_master "
			"WHERE type='table' AND name NOT LIKE 'sqlite_%'";

		sqlite3_stmt* stmt;
		if (sqlite3_prepare_v2(pImpl_->db, query, -1, &stmt, nullptr) == SQLITE_OK)
		{
			while (sqlite3_step(stmt) == SQLITE_ROW)
			{
				tables.emplace_back(reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0)));
			} // end of while
			sqlite3_finalize(stmt);
		} // end of if

		// Cache each table's schema
		for (const auto& tableName : tables)
		{
			TableSchema schema;
			std::string schemaQuery = "PRAGMA table_info(" + tableName + ")";

			if (sqlite3_prepare_v2(pImpl_->db, schemaQuery.c_str(), -1, &stmt, nullptr) == SQLITE_OK)
			{
				while (sqlite3_step(stmt) == SQLITE_ROW)
				{
					ColumnInfo info;
					info.name = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
					info.type = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));
					info.notNull = sqlite3_column_int(stmt, 3) != 0;
					info.primaryKey = sqlite3_column_int(stmt, 5) != 0;
					schema.push_back(info);
				} // end of while
				sqlite3_finalize(stmt);
			} // end of if

			pImpl_->schemaCache[tableName] = schema;
		} // end of for

		pImpl_->isCacheInitialized.store(true, std::memory_order_release);
	} // end of cacheAllSchemas

	bool Analyzer::isSchemaCached() const
	{
		return pImpl_->isCacheInitialized.load(std::memory_order_acquire);
	} // end of isSchemaCached

	void Analyzer::clearSchemaCache()
	{
		std::unique_lock lock(pImpl_->schemaMutex_);  // Exclusive lock
		pImpl_->schemaCache.clear();
		pImpl_->isCacheInitialized.store(false, std::memory_order_release);
	} // end of clearSchemaCache

	std::optional<TableSchema> Analyzer::getCachedSchema(const std::string& tableName) const
	{
		std::shared_lock lock(pImpl_->schemaMutex_);  // Shared lock (concurrent reads)

		auto it = pImpl_->schemaCache.find(tableName);
		if (it != pImpl_->schemaCache.end())
		{
			return it->second;
		} // end of if

		return std::nullopt;
	} // end of getCachedSchema

} // namespace sqlite_flux