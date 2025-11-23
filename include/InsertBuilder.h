// include/InsertBuilder.h
#pragma once

#include "Analyzer.h"
#include "TableTypes.h"
#include "ColumnValue.h"
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>

// Forward declaration
struct sqlite3_stmt;

namespace sqlite_flux
{

	// ============================================================================
	// ConflictResolution - SQLite conflict handling strategies
	// ============================================================================

	enum class ConflictResolution
	{
		None,       // No conflict handling (default)
		Ignore,     // INSERT OR IGNORE
		Replace     // INSERT OR REPLACE
	}; // end of enum ConflictResolution

	// ============================================================================
	// PreparedInsert - High-performance batch insert operations
	// ============================================================================

	class PreparedInsert
	{
	public:
		PreparedInsert(Analyzer& analyzer, const std::string& tableName,
			const std::vector<std::string>& columns,
			ConflictResolution conflict);

		~PreparedInsert();

		// Disable copy, enable move constructor only (no move assignment due to reference member)
		PreparedInsert(const PreparedInsert&) = delete;
		PreparedInsert& operator=(const PreparedInsert&) = delete;
		PreparedInsert(PreparedInsert&& other) noexcept;
		PreparedInsert& operator=(PreparedInsert&&) = delete;

		// Set values for current batch item
		PreparedInsert& Values(const std::unordered_map<std::string, ColumnValue>& values);

		// Execute current batch item (adds to transaction)
		void ExecuteBatch();

		// Finalize batch and commit transaction
		// Returns total number of rows inserted
		int64_t Finalize();

		// Get number of successful inserts in current batch
		int64_t getInsertCount() const { return insertCount_; }

	private:
		Analyzer& analyzer_;
		std::string tableName_;
		std::vector<std::string> columns_;
		std::string currentSql_;  // Current SQL statement being built
		sqlite3_stmt* stmt_ = nullptr;
		bool inTransaction_ = false;
		int64_t insertCount_ = 0;
		int64_t batchSize_ = 1000;  // Auto-commit every N operations

		void prepareStatement(ConflictResolution conflict);
		void bindValue(int index, const ColumnValue& value_);
		void cleanup();
	}; // end of class PreparedInsert

	// ============================================================================
	// InsertBuilder - Fluent API for INSERT operations
	// ============================================================================

	class InsertBuilder
	{
	public:
		InsertBuilder(Analyzer& analyzer, const std::string& tableName);

		// Set values to insert (map-based - self-documenting)
		InsertBuilder& Values(const std::unordered_map<std::string, ColumnValue>& values);

		// Conflict resolution strategies
		InsertBuilder& OrIgnore();   // INSERT OR IGNORE
		InsertBuilder& OrReplace();  // INSERT OR REPLACE

		// Execute insert and return last insert rowid
		// Returns 0 if OR IGNORE skipped the insert
		int64_t Execute();

		// Prepare for batch operations
		// Returns PreparedInsert object for high-performance batching
		PreparedInsert Prepare();

		// Generate SQL statement (for debugging/logging)
		std::string buildSql() const;

	private:
		Analyzer& analyzer_;
		std::string tableName_;
		TableSchema schema_;

		std::unordered_map<std::string, ColumnValue> values_;
		ConflictResolution conflictResolution_ = ConflictResolution::None;

		// Validation helpers
		void validateColumn(const std::string& column_) const;
		void validateColumnType(const std::string& column_, const ColumnValue& value_) const;
		void validateAllColumns() const;
		std::string getSqliteType(const std::string& column_) const;
		bool isTypeCompatible(const std::string& sqliteType, const ColumnValue& value_) const;

		// SQL generation helper
		std::string getConflictClause() const;
	}; // end of class InsertBuilder

} // namespace sqlite_flux