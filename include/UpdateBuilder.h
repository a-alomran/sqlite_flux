// include/UpdateBuilder.h
#pragma once

#include "Analyzer.h"
#include "TableTypes.h"
#include "ColumnValue.h"
#include "QueryBuilder.h"  // For FilterCondition and CompareOp
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>

// Forward declaration
struct sqlite3_stmt;

namespace sqlite_flux
{

	// ============================================================================
	// PreparedUpdate - High-performance batch update operations
	// ============================================================================

	class PreparedUpdate
	{
	public:
		PreparedUpdate(Analyzer& analyzer, const std::string& tableName,
			const std::unordered_map<std::string, ColumnValue>& updates);

		~PreparedUpdate();

		// Disable copy, enable move constructor only (no move assignment due to reference member)
		PreparedUpdate(const PreparedUpdate&) = delete;
		PreparedUpdate& operator=(const PreparedUpdate&) = delete;
		PreparedUpdate(PreparedUpdate&& other) noexcept;
		PreparedUpdate& operator=(PreparedUpdate&&) = delete;

		// Set WHERE conditions for current batch item
		PreparedUpdate& Where(const std::string& column_, const ColumnValue& value_,
			CompareOp op_ = CompareOp::Equal);

		// Execute current batch item
		void ExecuteBatch();

		// Finalize batch and commit transaction
		// Returns total number of rows updated
		int64_t Finalize();

		// Get number of rows updated in current batch
		int64_t getUpdateCount() const { return updateCount_; }

	private:
		Analyzer& analyzer_;
		std::string tableName_;
		std::unordered_map<std::string, ColumnValue> updates_;
		std::vector<FilterCondition> currentFilters_;
		bool inTransaction_ = false;
		int64_t updateCount_ = 0;
		int64_t batchSize_ = 1000;  // Auto-commit every N operations

		void cleanup();
	}; // end of class PreparedUpdate

	// ============================================================================
	// UpdateBuilder - Fluent API for UPDATE operations
	// ============================================================================

	class UpdateBuilder
	{
	public:
		UpdateBuilder(Analyzer& analyzer, const std::string& tableName);

		// Set column values to update
		UpdateBuilder& Set(const std::string& column_, const ColumnValue& value_);

		// WHERE conditions (reuse FilterCondition from QueryBuilder)
		UpdateBuilder& Where(const std::string& column_, const ColumnValue& value_,
			CompareOp op_ = CompareOp::Equal);

		// Safety mechanism: Allow UPDATE without WHERE clause
		// By default, UPDATE without WHERE will throw exception
		UpdateBuilder& Unsafe();

		// Execute update and return number of rows affected
		int64_t Execute();

		// Prepare for batch operations
		// Returns PreparedUpdate object for high-performance batching
		PreparedUpdate Prepare();

		// Generate SQL statement (for debugging/logging)
		std::string buildSql() const;

	private:
		Analyzer& analyzer_;
		std::string tableName_;
		TableSchema schema_;

		std::unordered_map<std::string, ColumnValue> updates_;
		std::vector<FilterCondition> filters_;
		bool allowUnsafe_ = false;

		// Validation helpers
		void validateColumn(const std::string& column_) const;
		void validateColumnType(const std::string& column_, const ColumnValue& value_) const;
		std::string getSqliteType(const std::string& column_) const;
		bool isTypeCompatible(const std::string& sqliteType, const ColumnValue& value_) const;
		void validateSafeExecution() const;
	}; // end of class UpdateBuilder

} // namespace sqlite_flux