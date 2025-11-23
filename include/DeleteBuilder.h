// include/DeleteBuilder.h
#pragma once

#include "Analyzer.h"
#include "TableTypes.h"
#include "ColumnValue.h"
#include "QueryBuilder.h"  // For FilterCondition and CompareOp
#include <string>
#include <vector>

namespace sqlite_flux
{

	// ============================================================================
	// DeleteBuilder - Fluent API for DELETE operations
	// ============================================================================

	class DeleteBuilder
	{
	public:
		DeleteBuilder(Analyzer& analyzer, const std::string& tableName);

		// WHERE conditions (reuse FilterCondition from QueryBuilder)
		DeleteBuilder& Where(const std::string& column_, const ColumnValue& value_,	CompareOp op_ = CompareOp::Equal);

		// ORDER BY (useful with LIMIT for targeted deletion)
		DeleteBuilder& OrderBy(const std::string& column_, bool ascending = true);

		// LIMIT (safety mechanism to prevent mass deletion)
		DeleteBuilder& Limit(int limit);

		// Safety mechanism: Allow DELETE without WHERE clause
		// By default, DELETE without WHERE will throw exception
		DeleteBuilder& Unsafe();

		// Execute delete and return number of rows affected
		int64_t Execute();

		// Generate SQL statement (for debugging/logging)
		std::string buildSql() const;

	private:
		Analyzer& analyzer_;
		std::string tableName_;
		TableSchema schema_;

		std::vector<FilterCondition> filters_;
		std::string orderByColumn_;
		bool orderAscending_ = true;
		int limitValue_ = -1;
		bool allowUnsafe_ = false;

		// Validation helpers
		void validateColumn(const std::string& column_) const;
		void validateSafeExecution() const;
	}; // end of class DeleteBuilder

} // namespace sqlite_flux